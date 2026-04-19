@echo off
cd /d C:\docker\projects\canal-reli
python app/pipeline/run_pipeline.py --pipeline workflows/video-pipeline.json --fila 1
pause
