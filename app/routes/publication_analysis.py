from fastapi import APIRouter
from starlette.responses import StreamingResponse
from pathlib import Path
import subprocess
import logging

router = APIRouter()
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parents[2]

SCRIPTS = [
    ("preprocessing_title.py", BASE_DIR / "src" / "data-cleaning"),
    ("publication_trend.py", BASE_DIR / "src" / "modelling")
]

def stream_script(scripts_with_path: list):
    def generate():
        for script_name, script_dir in scripts_with_path:
            script_path = script_dir / script_name
            if not script_path.exists():
                logger.error(f"Script not found: {script_path}")
                yield f"Script not found: {script_path}\n"
                continue

            logger.info(f"Running script: {script_path}")
            process = subprocess.Popen(
                ["python", str(script_path)],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True
            )
            for line in iter(process.stdout.readline, ''):
                logger.info(line.strip())
                yield line
            process.stdout.close()
            process.wait()
            logger.info(f"Finished: {script_name}\n")
            yield f"\nFinished: {script_name}\n\n"
    return generate

@router.post("/run-analysis/")
async def run_analysis():
    return StreamingResponse(
        stream_script(SCRIPTS),
        media_type="text/plain"
    )