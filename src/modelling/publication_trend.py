import pandas as pd
import mlflow
import time
import sys
from bertopic import BERTopic
from sentence_transformers import SentenceTransformer
from pathlib import Path
from logging_config import setup_logging
from gensim.corpora import Dictionary
from gensim.models.coherencemodel import CoherenceModel

BASE_DIR = Path(__file__).resolve().parent.parent.parent
DATA_DIR = BASE_DIR / "data" / "cleaned"
MODEL_DIR = BASE_DIR / "model"
MODEL_FILE = MODEL_DIR / "bertopic_model.pkl"
LOGS_DIR = BASE_DIR / "logs"
APP_DIR = BASE_DIR / "app"
MLFLOW_DIR = BASE_DIR / "mlruns"
OUTPUT_DIR = DATA_DIR / "output"

INPUT_PATH = DATA_DIR / "titles_cleaned.csv"
TOPIC_ASSIGNMENT_PATH = DATA_DIR / "topic_assignments.csv"
TOPIC_TREND_PATH = DATA_DIR / "topic_trends.csv"

MODEL_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(exist_ok=True)
APP_DIR.mkdir(exist_ok=True)
MLFLOW_DIR.mkdir(exist_ok=True)

sys.path.insert(0, str(APP_DIR))

mlflow.set_tracking_uri(f"file:///{MLFLOW_DIR.resolve().as_posix()}")
mlflow.set_experiment("bertopic_experiment")

log = setup_logging(__name__, log_dir=LOGS_DIR)

def compute_topic_coherence(titles, topic_model, top_n_words=10):
    topic_words = topic_model.get_topics()
    topics_tokens = [
        [word for word, _ in topic_words[topic]] 
        for topic in topic_words if topic != -1
    ]
    words = [doc.split() for doc in titles]
    dictionary = Dictionary(words)

    coherence_model = CoherenceModel(
        topics=topics_tokens,
        texts=words,
        dictionary=dictionary,
        coherence='u_mass'
    )
    return coherence_model.get_coherence()

def compute_topic_diversity(topic_model, top_k=10):
    topic_words = topic_model.get_topics()
    all_words = set()
    total = 0
    for topic in topic_words.values():
        if not topic: continue
        words = [word for word, _ in topic[:top_k]]
        total += len(words)
        all_words.update(words)
    return len(all_words) / total if total > 0 else 0

log.info("Loading data...")
df = pd.read_csv(INPUT_PATH).dropna(subset=["judul", "tahun"])
titles = df["judul"].tolist()
years = df["tahun"].astype(str).tolist()

with mlflow.start_run(run_name="bertopic_training"):
    start_time = time.time()

    mlflow.log_param("model", "BERTopic")
    mlflow.log_param("embedding_model", "paraphrase-multilingual-MiniLM-L12-v2")
    mlflow.log_param("num_titles", len(titles))

    log.info("Loading embedding model...")
    embedding_model = SentenceTransformer("paraphrase-multilingual-MiniLM-L12-v2")

    log.info("Training BERTopic model...")
    topic_model = BERTopic(embedding_model=embedding_model, language="multilingual", verbose=True)
    topics, probs = topic_model.fit_transform(titles)

    log.info("Assigning topics...")
    topic_info = topic_model.get_topic_info()
    topic_names = {row["Topic"]: row["Name"] for _, row in topic_info.iterrows()}
    df["topic"] = topics
    df["probability"] = probs
    df["topic_name"] = df["topic"].map(topic_names)
    df[["judul", "tahun", "topic", "probability", "topic_name"]].to_csv(TOPIC_ASSIGNMENT_PATH, index=False)
    mlflow.log_artifact(str(TOPIC_ASSIGNMENT_PATH))

    df["tahun"] = df["tahun"].astype(int)
    df = df[df["topic"] != -1]

    valid_years = df["tahun"].value_counts()
    valid_years = valid_years[valid_years > 2].index
    df = df[df["tahun"].isin(valid_years)]

    valid_topics = topic_model.get_topic_freq()["Topic"].tolist()
    df = df[df["topic"].isin(valid_topics)]

    titles = df["judul"].tolist()
    topics = df["topic"].tolist()
    years = df["tahun"].astype(str).tolist()

    log.info("Calculating topic trends...")

    topics_over_time = topic_model.topics_over_time(
        docs=titles,
        topics=topics,
        timestamps=years,
        nr_bins=30
    )

    log.info("Saving topic trends visualization...")
    fig = topic_model.visualize_topics_over_time(topics_over_time)
    fig.write_html(str(OUTPUT_DIR / "topics_over_time.html"))
    mlflow.log_artifact(str(OUTPUT_DIR / "topics_over_time.html"))

    trends_df = topics_over_time[["Topic", "Words", "Timestamp", "Frequency"]]
    trends_df.columns = ["topic", "topic_words", "tahun", "count"]
    trends_df.to_csv(TOPIC_TREND_PATH, index=False)
    mlflow.log_artifact(str(TOPIC_TREND_PATH))

    mlflow.log_metric("num_topics", len(topic_info))

    log.info("Evaluating topic quality...")
    coherence = compute_topic_coherence(titles, topic_model)
    diversity = compute_topic_diversity(topic_model)
    mlflow.log_metric("topic_coherence", coherence)
    mlflow.log_metric("topic_diversity", diversity)
    log.info(f"Coherence (c_umass): {coherence:.4f}")
    log.info(f"Diversity: {diversity:.4f}")

    log.info("Saving model...")
    topic_model.save(str(MODEL_FILE))
    mlflow.log_artifact(str(MODEL_FILE), artifact_path="model")

    duration = time.time() - start_time
    mlflow.log_metric("training_duration_seconds", duration)
    log.info(f"Training completed in {duration:.2f} seconds")