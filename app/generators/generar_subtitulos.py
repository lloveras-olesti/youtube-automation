#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
GENERADOR DE SUBTÍTULOS SRT
Versión adaptada para Docker
"""

import sys
import os
from app.config import settings
from datetime import timedelta

# CONFIGURACIÓN
# Ruta base desde variable de entorno (por defecto /app/data)
DATA_PATH = os.environ.get("DATA_PATH", "/app/data")

# Rutas de archivos (ahora relativas a DATA_PATH)
AUDIO_FILE = str(settings.get_temp_file("audio", "1.wav"))
OUTPUT_FILE = str(settings.get_temp_file("subtitles", "1.srt"))

# Configuración de Whisper
MODEL_SIZE = settings.whisper_model_size  # Modelo que estás usando con buenos resultados
LANGUAGE = settings.whisper_language  # Español - cambia a None si quieres detección automática


def format_timestamp(seconds):
    """Convierte segundos a formato SRT timestamp (HH:MM:SS,mmm)"""
    td = timedelta(seconds=seconds)
    hours = td.seconds // 3600
    minutes = (td.seconds % 3600) // 60
    secs = td.seconds % 60
    millis = td.microseconds // 1000
    return f"{hours:02d}:{minutes:02d}:{secs:02d},{millis:03d}"


def transcribe_audio():
    """Transcribe el archivo de audio usando Whisper local"""
    try:
        import whisper
    except ImportError:
        print("Error: Necesitas instalar whisper primero:")
        print("pip install openai-whisper")
        sys.exit(1)

    print(f"Cargando modelo Whisper '{MODEL_SIZE}'...")
    print("(La primera vez descargará el modelo, puede tardar unos minutos)")
    
    model = whisper.load_model(MODEL_SIZE)

    print(f"Transcribiendo '{AUDIO_FILE}'...")
    print("Esto puede tardar varios minutos dependiendo de la duración del audio...")

    options = {"task": "transcribe", "verbose": False}
    if LANGUAGE:
        options["language"] = LANGUAGE

    result = model.transcribe(AUDIO_FILE, **options)
    return result


def create_srt(segments):
    """Crea el archivo SRT a partir de los segmentos transcritos"""
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        for i, segment in enumerate(segments, start=1):
            start = format_timestamp(segment["start"])
            end = format_timestamp(segment["end"])
            text = segment["text"].strip()

            f.write(f"{i}\n")
            f.write(f"{start} --> {end}\n")
            f.write(f"{text}\n\n")

    print(f"\n✓ Archivo SRT creado exitosamente: {OUTPUT_FILE}")


def main():
    print("=" * 60)
    print("GENERADOR DE SUBTÍTULOS SRT")
    print("=" * 60)
    print(f"\nArchivo de entrada: {AUDIO_FILE}")
    print(f"Archivo de salida:  {OUTPUT_FILE}")
    print(f"Modelo:             {MODEL_SIZE}")
    print(f"Idioma:             {LANGUAGE if LANGUAGE else 'Detección automática'}")
    print("\nIniciando transcripción...\n")

    # Verificar que existe el archivo de audio
    if not os.path.exists(AUDIO_FILE):
        print(f"✗ Error: No se encuentra el archivo de audio en {AUDIO_FILE}")
        sys.exit(1)

    # Transcribir
    result = transcribe_audio()

    # Crear SRT
    create_srt(result["segments"])

    # Resumen
    print("\n" + "=" * 60)
    print("TRANSCRIPCIÓN COMPLETADA")
    print("=" * 60)
    print(f"Idioma detectado: {result.get('language', 'N/A')}")
    print(f"Segmentos generados: {len(result['segments'])}")
    print(f"Duración: {result.get('segments', [{}])[-1].get('end', 0):.1f} segundos")
    print("=" * 60)


if __name__ == "__main__":
    main()
