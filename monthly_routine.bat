@echo off
cd /d *ADD RUTE*
python app/pipeline/run_pipeline.py --pipeline workflows/mensual-pipeline.json
pause
