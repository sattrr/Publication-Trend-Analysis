import os
import sys
import time
import re
import mlflow
import numpy as np
import pandas as pd
from pathlib import Path
from bertopic import BERTopic
from gensim.corpora import Dictionary
from gensim.models.coherencemodel import CoherenceModel
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
from logging_config import setup_logging

BASE_DIR = Path(__file__).resolve().parent.parent.parent
DATA_DIR = BASE_DIR / "data" / "cleaned"
MODEL_DIR = BASE_DIR / "model"
LOGS_DIR = BASE_DIR / "logs"
APP_DIR = BASE_DIR / "app"
MLFLOW_DIR = BASE_DIR / "mlruns"
OUTPUT_DIR = DATA_DIR / "output"

INPUT_PATH = DATA_DIR / "titles_cleaned.xlsx"
TOPIC_ASSIGNMENT_PATH = OUTPUT_DIR / "topic_assignments.xlsx"
TOPIC_TREND_PATH = OUTPUT_DIR / "topic_trends.xlsx"
TOPIC_DOMAIN_MAP_PATH = OUTPUT_DIR / "topic_domain_mapping.xlsx"
MODEL_FILE = MODEL_DIR / "bertopic_model.pkl"

for p in [MODEL_DIR, LOGS_DIR, APP_DIR, MLFLOW_DIR, OUTPUT_DIR]:
    p.mkdir(parents=True, exist_ok=True)

sys.path.insert(0, str(APP_DIR))

mlflow.set_tracking_uri(f"file:///{MLFLOW_DIR.resolve().as_posix()}")
mlflow.set_experiment("bertopic_experiment")

log = setup_logging(__name__, log_dir=LOGS_DIR)

EMBED_MODEL_NAME = os.getenv("EMBED_MODEL_NAME", "paraphrase-multilingual-MiniLM-L12-v2")

DOMAIN_LABELS = {
    "Sains & Teknologi": [
        "sains", "ilmu", "fisika", "kimia", "biologi", "matematika",
        "komputer", "informatika", "rekayasa", "engineering", "teknologi",
        "otomasi", "elektronik", "ai", "machine learning", "data"
    ],
    "Kesehatan & Kedokteran": [
        "kesehatan", "kedokteran", "klinis", "epidemiologi", "farmasi", "obat",
        "medis", "rumah sakit", "keperawatan", "gizi", "kesehatan masyarakat"
    ],
    "Ilmu Sosial & Humaniora": [
        "sosial", "budaya", "politik", "ekonomi", "manajemen", "bisnis",
        "pendidikan", "hukum", "komunikasi", "psikologi", "antropologi",
        "sosiologi", "humaniora"
    ],
    "Lingkungan & Keberlanjutan": [
        "lingkungan", "iklim", "energi", "keberlanjutan", "konservasi",
        "polusi", "ekologi", "perubahan iklim", "kehutanan", "biodiversitas"
    ],
}

DOMAIN_SENTENCES = {
    k: f"Topik bidang {k.lower()} tentang " + ", ".join(v) for k, v in DOMAIN_LABELS.items()
}

def safe_metric_name(name: str) -> str:
    """Sanitize metric name agar sesuai aturan MLflow."""
    return re.sub(r"[^a-zA-Z0-9_\- ./]", "_", name)

def compute_topic_coherence(titles, topic_model, top_n_words=10):
    """Hitung u_mass coherence (cepat) untuk kualitas topik."""
    topic_words = topic_model.get_topics()
    topics_tokens = [
        [word for word, _ in topic_words[t][:top_n_words]]
        for t in topic_words if t != -1 and topic_words[t]
    ]
    words = [doc.split() for doc in titles]
    dictionary = Dictionary(words)
    cm = CoherenceModel(
        topics=topics_tokens,
        texts=words,
        dictionary=dictionary,
        coherence='u_mass'
    )
    return cm.get_coherence()

def compute_topic_diversity(topic_model, top_k=10):
    """Proporsi kata unik di seluruh topik (semakin tinggi semakin beragam)."""
    topic_words = topic_model.get_topics()
    all_words = set()
    total = 0
    for topic in topic_words.values():
        if not topic:
            continue
        words = [w for w, _ in topic[:top_k]]
        total += len(words)
        all_words.update(words)
    return len(all_words) / total if total > 0 else 0

def topic_label_text(topic_model, topic_id, top_k=8):
    """Gabungkan top-k kata dari satu topik menjadi frasa representatif."""
    words = topic_model.get_topic(topic_id)
    if not words:
        return ""
    return ", ".join([w for w, _ in words[:top_k]])

def map_topics_to_domains(topic_model, embedder, threshold=0.30, top_k_words=8):
    """
    Pemetaan topic -> domain dengan cosine similarity antara:
    - embedding frasa kata topik
    - embedding kalimat label domain
    Fallback: keyword overlap jika similarity < threshold.
    """
    domain_keys = list(DOMAIN_SENTENCES.keys())
    domain_texts = [DOMAIN_SENTENCES[k] for k in domain_keys]

    domain_emb = embedder.encode(
        domain_texts,
        convert_to_numpy=True,
        normalize_embeddings=True
    )

    mapping_rows = []
    topic_info = topic_model.get_topic_info()

    for _, row in topic_info.iterrows():
        t_id = row["Topic"]
        if t_id == -1:
            continue

        rep_text = topic_label_text(topic_model, t_id, top_k=top_k_words)
        if not rep_text:
            mapping_rows.append({
                "topic": t_id, "topic_words": "", "best_domain": "Unassigned",
                "similarity": 0.0
            })
            continue

        t_emb = embedder.encode([rep_text], convert_to_numpy=True, normalize_embeddings=True)
        sims = cosine_similarity(t_emb, domain_emb)[0]
        best_idx = int(np.argmax(sims))
        best_domain = domain_keys[best_idx]
        best_score = float(sims[best_idx])

        if best_score < threshold:
            rep_tokens = set([w.strip().lower() for w in rep_text.split(",")])
            overlap_scores = []
            for dk, kw in DOMAIN_LABELS.items():
                overlap = len(rep_tokens.intersection(set(kw)))
                overlap_scores.append((dk, overlap))
            overlap_scores.sort(key=lambda x: x[1], reverse=True)
            if overlap_scores and overlap_scores[0][1] > 0:
                best_domain = overlap_scores[0][0]
                best_score = max(best_score, 0.25)

        mapping_rows.append({
            "topic": t_id,
            "topic_words": rep_text,
            "best_domain": best_domain,
            "similarity": round(best_score, 4)
        })

    mapping_df = pd.DataFrame(mapping_rows).sort_values(["best_domain", "topic"]).reset_index(drop=True)
    return mapping_df

def main():
    np.random.seed(42)

    log.info("Loading cleaned data...")
    df = pd.read_excel(INPUT_PATH).dropna(subset=["judul", "tahun"])
    df["tahun"] = df["tahun"].astype(str).str.extract(r"(\d{4})")
    df = df.dropna(subset=["tahun"])
    df["tahun"] = df["tahun"].astype(int)

    titles_all = df["judul"].astype(str).tolist()
    years_all = df["tahun"].astype(str).tolist()

    with mlflow.start_run(run_name="bertopic_training_with_domains"):
        start_time = time.time()

        mlflow.log_param("model", "BERTopic")
        mlflow.log_param("embedding_model", EMBED_MODEL_NAME)
        mlflow.log_param("num_titles", len(titles_all))

        log.info(f"Loading embedding model: {EMBED_MODEL_NAME}")
        embedder = SentenceTransformer(EMBED_MODEL_NAME)

        log.info("Training BERTopic...")
        topic_model = BERTopic(
            embedding_model=embedder,
            language="multilingual",
            verbose=True
        )
        topics, probs = topic_model.fit_transform(titles_all)

        log.info("Assigning topics to documents...")
        topic_info = topic_model.get_topic_info()
        topic_names = {row["Topic"]: row["Name"] for _, row in topic_info.iterrows()}

        df["topic"] = topics
        df["probability"] = probs
        df["topic_name"] = df["topic"].map(topic_names)

        log.info("Mapping topics to domains...")
        domain_map_df = map_topics_to_domains(topic_model, embedder, threshold=0.30, top_k_words=8)
        domain_map_df.to_excel(TOPIC_DOMAIN_MAP_PATH, index=False)
        mlflow.log_artifact(str(TOPIC_DOMAIN_MAP_PATH))

        topic_to_domain = dict(zip(domain_map_df["topic"], domain_map_df["best_domain"]))
        df["domain"] = df["topic"].map(topic_to_domain).fillna("Unassigned")

        assign_cols = ["judul", "tahun", "topic", "probability", "topic_name", "domain"]
        df[assign_cols].to_excel(TOPIC_ASSIGNMENT_PATH, index=False)
        mlflow.log_artifact(str(TOPIC_ASSIGNMENT_PATH))

        df_valid = df[df["topic"] != -1].copy()
        valid_years = df_valid["tahun"].value_counts()
        valid_years = valid_years[valid_years > 2].index
        df_valid = df_valid[df_valid["tahun"].isin(valid_years)]

        valid_topics = topic_model.get_topic_freq()["Topic"].tolist()
        df_valid = df_valid[df_valid["topic"].isin(valid_topics)]

        titles = df_valid["judul"].tolist()
        topics_valid = df_valid["topic"].tolist()
        years = df_valid["tahun"].astype(str).tolist()

        log.info("Calculating topics over time...")
        unique_years = sorted(df_valid["tahun"].unique())
        nr_bins = min(30, max(5, len(unique_years)))
        topics_over_time = topic_model.topics_over_time(
            docs=titles,
            topics=topics_valid,
            timestamps=years,
            nr_bins=nr_bins
        )

        trends_df = topics_over_time[["Topic", "Words", "Timestamp", "Frequency"]].copy()
        trends_df.columns = ["topic", "topic_words", "tahun", "count"]
        trends_df.to_excel(TOPIC_TREND_PATH, index=False)
        mlflow.log_artifact(str(TOPIC_TREND_PATH))

        counts = domain_map_df["best_domain"].value_counts().to_dict()
        for dom, cnt in counts.items():
            metric_name = f"topics_in_{safe_metric_name(dom)}"
            mlflow.log_metric(metric_name, int(cnt))

        mlflow.log_metric("num_topics", int((topic_info["Topic"] != -1).sum()))

        log.info("Evaluating topic quality...")
        coherence = compute_topic_coherence(titles, topic_model)
        diversity = compute_topic_diversity(topic_model)
        mlflow.log_metric("topic_coherence_umass", float(coherence))
        mlflow.log_metric("topic_diversity", float(diversity))

        log.info(f"Coherence (u_mass): {coherence:.4f}")
        log.info(f"Diversity: {diversity:.4f}")

        log.info("Saving BERTopic model...")
        topic_model.save(str(MODEL_FILE), serialization="pkl")
        if MODEL_FILE.exists():
            mlflow.log_artifact(str(MODEL_FILE), artifact_path="model")
        else:
            log.warning("Model file not found, skipping artifact log.")

        duration = time.time() - start_time
        mlflow.log_metric("training_duration_seconds", float(duration))
        log.info(f"Training completed in {duration:.2f} seconds")

if __name__ == "__main__":
    main()