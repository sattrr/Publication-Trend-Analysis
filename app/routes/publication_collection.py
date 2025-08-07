import subprocess
import logging
from fastapi import APIRouter, BackgroundTasks
from starlette.responses import StreamingResponse
from pathlib import Path

router = APIRouter()
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parents[2]
COLLECTION_DIR = BASE_DIR / "src" / "data-cleaning"
FINAL_EXCEL_PATH = BASE_DIR / "data" / "cleaned" / "final_publication.xlsx"

def run_scripts(script_names: list):
    output_log = ""
    for script_name in script_names:
        script_path = COLLECTION_DIR / script_name
        logger.info(f"[SCRIPT] Running script: {script_name}")
        try:
            process = subprocess.Popen(
                ["python", str(script_path)],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True
            )
            for line in iter(process.stdout.readline, ''):
                logger.info(f"[SCRIPT:{script_name}] {line.strip()}")
                output_log += line
            process.stdout.close()
            process.wait()

            logger.info(f"[SCRIPT] Finished script: {script_name} (exit code: {process.returncode})")
        except Exception as e:
            logger.error(f"[SCRIPT] Error running {script_name}: {str(e)}")
    return output_log

@router.post("/run-collection/")
async def run_publication_collection(background_tasks: BackgroundTasks):
    def task():
        logger.info("[COLLECTION] Starting publication collection task...")

        logger.info("[COLLECTION] Running preprocessing scripts...")
        run_scripts([
            "preprocessing_sister.py",
            "preprocessing_scopus.py"
        ])
        
        logger.info("[COLLECTION] Running combining script...")
        run_scripts(["combine_publication.py"])

        logger.info("[COLLECTION] Running sorting script...")
        run_scripts(["sort_publication.py"])

        logger.info(f"[COLLECTION] Final publication saved to: {FINAL_EXCEL_PATH}")
        logger.info("[COLLECTION] Publication collection task completed.")

    background_tasks.add_task(task)
    return {"message": "Publication collection running in background. Please wait."}