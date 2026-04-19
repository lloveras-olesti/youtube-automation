@echo off
cd /d C:\docker\projects\canal-reli

:: Contar líneas del CSV excluyendo la cabecera
set COUNT=0
for /f "skip=1 tokens=*" %%A in (data\input\calendario.csv) do set /a COUNT+=1

echo Encontradas %COUNT% entradas en calendario.csv
echo Iniciando pipeline para cada una...
echo.

:: Ejecutar la pipeline tantas veces como entradas haya
for /l %%i in (1,1,%COUNT%) do (
    echo === Ejecutando entrada %%i de %COUNT% ===
    python app/pipeline/run_pipeline.py --pipeline workflows/video-pipeline.json --fila 1
    if errorlevel 1 (
        echo ERROR en la entrada %%i. Abortando.
        pause
        exit /b 1
    )
    echo.
)

echo Todas las entradas procesadas correctamente.
pause