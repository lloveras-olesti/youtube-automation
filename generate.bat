@echo off
cd /d *ADD RUTE*
python app/pipeline/run_pipeline.py --pipeline workflows/video-pipeline.json --fila 1
pause
