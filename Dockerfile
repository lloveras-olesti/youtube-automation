# ============================================
# DOCKERFILE PARA PROYECTO CANAL RELI
# Imagen con Python, FFMPEG, Whisper y dependencias
# Ubicación: C:\docker\projects\canal-reli\Dockerfile
# ============================================

FROM python:3.11-slim

# Metadata
LABEL maintainer="YouTube Automation"
LABEL description="Automated video pipeline"

# Variables de entorno
ENV PYTHONUNBUFFERED=1 \
    DEBIAN_FRONTEND=noninteractive \
    DATA_PATH=/app/data

# Directorio de trabajo
WORKDIR /app

# ============================================
# INSTALACIÓN DE DEPENDENCIAS DEL SISTEMA
# ============================================

RUN apt-get update && apt-get install -y \
    # FFMPEG y herramientas de video/audio
    ffmpeg \
    # Fuentes para subtítulos
    fonts-liberation \
    fontconfig \
    # Herramientas de red (para debugging)
    curl \
    wget \
    # Herramientas de desarrollo
    git \
    # Limpieza
    && rm -rf /var/lib/apt/lists/* \
    # Actualizar caché de fuentes
    && fc-cache -f -v

# ============================================
# INSTALACIÓN DE DEPENDENCIAS DE PYTHON
# ============================================

# Copiar requirements.txt primero (mejor cache de Docker)
COPY requirements.txt .

# Instalar dependencias Python
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir --break-system-packages -r requirements.txt

# Precargar modelo Whisper small (opcional, ahorra tiempo en primera ejecución)
# Descomenta si quieres que el modelo se descargue al construir la imagen
# RUN python -c "import whisper; whisper.load_model('small')"

# ============================================
# COPIAR CÓDIGO DE LA APLICACIÓN
# ============================================

# Copiar estructura de la aplicación
COPY app/ ./app/

# Crear directorios necesarios
RUN mkdir -p \
    /app/data/input/recursos/fotos-fondo \
    /app/data/input/recursos/musica \
    /app/data/input/recursos/videos-loop/foto1 \
    /app/data/input/recursos/videos-loop/foto2 \
    /app/data/input/recursos/videos-loop/foto3 \
    /app/data/input/recursos/videos-loop/foto4 \
    /app/data/input/recursos/videos-loop/foto5 \
    /app/data/temp/audio \
    /app/data/temp/video \
    /app/data/temp/subtitles \
    /app/data/output/videos \
    /app/data/output/metadata \
    /app/data/logs

# ============================================
# PERMISOS
# ============================================

# Asegurar que todo tenga permisos correctos
RUN chmod -R 755 /app

# ============================================
# HEALTHCHECK
# ============================================

# Verificar que Python y dependencias críticas estén disponibles
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import anthropic, whisper, pandas; print('OK')" || exit 1

# ============================================
# COMANDO POR DEFECTO
# ============================================

# Mantener contenedor vivo (n8n ejecutará comandos)
CMD ["sleep", "infinity"]
