@echo off
REM ============================================================
REM  inicializar_estado_portadas.bat
REM  Ubicación: C:\docker\projects\canal-reli\data\input\recursos\portadas\
REM
REM  Doble clic para regenerar estado_portadas.json escaneando
REM  todas las carpetas de imágenes de portada.
REM
REM  Úsalo siempre que:
REM    - Añadas imágenes nuevas a cualquier carpeta de temática
REM    - Elimines imágenes de cualquier carpeta de temática
REM    - Quieras resetear los contadores a cero (borra estado_portadas.json
REM      manualmente antes de ejecutar)
REM ============================================================

REM Calcular ruta raíz del proyecto (4 niveles arriba desde esta carpeta)
SET "SCRIPT_DIR=%~dp0"
SET "PROJECT_ROOT=%SCRIPT_DIR%..\..\..\..\"

REM Ruta al script Python
SET "PY_SCRIPT=%PROJECT_ROOT%app\utils\inicializar_estado_portadas.py"

echo.
echo Ejecutando inicializar_estado_portadas.py...
echo.

python "%PY_SCRIPT%"

echo.
pause
