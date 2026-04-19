#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
================================================
GENERADOR DE AUDIO TTS - XTTS v2
================================================
Convierte guion.txt a audio usando XTTS v2 en local (Windows host).

IMPORTANTE: Este script corre en el HOST Windows, NO dentro del contenedor.
Requiere el entorno virtual en <proyecto>/tts-venv con XTTS v2 instalado.

ESTRUCTURA ESPERADA DEL PROYECTO:
  canal-reli/
  ├── tts-venv/          ← entorno virtual Python con XTTS v2
  ├── tts-model/         ← modelo XTTS v2 descargado localmente
  ├── app/generators/generar_audio.py  ← este script
  └── data/
      └── temp/
          └── audio/
              ├── referencia.wav   ← audio de referencia para clonación de voz
              └── 1.wav            ← output generado

FLUJO:
  1. Lee data/temp/guion.txt
  2. Divide el texto en chunks de max 220 caracteres por oración
  3. Genera audio por chunk usando XTTS v2 + audio de referencia
  4. Normaliza silencios de cada chunk (recorta pausas > MAX_SILENCE_SECS)
  5. Combina todos los chunks en un único WAV
  6. Normaliza silencios del audio final combinado
  7. Guarda resultado en data/temp/audio/1.wav

USO:
  tts-venv\\Scripts\\python.exe app/generators/generar_audio.py
  tts-venv\\Scripts\\python.exe app/generators/generar_audio.py --guion data/temp/guion.txt

PORTABILIDAD:
  Para mover el proyecto a otro PC Windows:
  1. Copiar la carpeta completa del proyecto
  2. Copiar tts-venv/ (o recrearla con los mismos paquetes)
  3. Copiar tts-model/
  No hay rutas hardcodeadas fuera del proyecto.
"""

# ============================================================
# IMPORTS Y MONKEY-PATCHES
# ============================================================
# ORDEN CRÍTICO:
#   1. Stdlib (sys, os, time, etc.)
#   2. Mock de numba (ANTES de torch/torchaudio/TTS)
#   3. Registrar directorio de DLLs de torch en Windows
#   4. Importar torch/torchaudio/soundfile con reintentos (WDAC)
#   5. Monkey-patch torchaudio.load → soundfile backend
#   6. Imports del resto de dependencias
# ============================================================

import os
import re
import sys
import argparse
import logging
import time
import wave
import struct
import numpy as np
from pathlib import Path
from unittest.mock import MagicMock

# ── 1. Mock de numba ANTES de cualquier import de torch/TTS ──────────
#    Evita que la cadena de dependencias de TTS cargue la DLL
#    _helperlib de numba (bloqueada por Application Control en Windows).
numba_mock = MagicMock()
def _dummy_decorator(*args, **kwargs):
    def decorator(func):
        return func
    if len(args) == 1 and callable(args[0]):
        return args[0]
    return decorator
numba_mock.jit = _dummy_decorator
numba_mock.njit = _dummy_decorator
numba_mock.vectorize = _dummy_decorator
numba_mock.guvectorize = _dummy_decorator
numba_mock.cfunc = _dummy_decorator
sys.modules['numba'] = numba_mock
sys.modules['numba.core'] = numba_mock
sys.modules['numba.core.decorators'] = numba_mock

# ── 2. Registrar directorio de DLLs de torch en Windows ──────────────
#    Ayuda al loader de Windows a localizar torch_python.dll y deps
#    antes de que el import de torch lo intente por su cuenta.
_PROJECT_ROOT_EARLY = Path(__file__).parent.parent.parent.absolute()
_torch_lib_dir = _PROJECT_ROOT_EARLY / "tts-venv" / "Lib" / "site-packages" / "torch" / "lib"
if _torch_lib_dir.exists() and hasattr(os, 'add_dll_directory'):
    try:
        os.add_dll_directory(str(_torch_lib_dir))
    except OSError:
        pass  # Ya registrado o sin permisos — no es fatal

# ── 3. Importar torch con reintentos (WDAC puede bloquear DLLs) ──────
#    WinError 4551 = "Una directiva de Control de aplicaciones bloqueó
#    este archivo". Suele ser transitorio; reintentamos con backoff.
_MAX_IMPORT_RETRIES = 3
_RETRY_DELAY_SECS = 5

import soundfile as sf

for _attempt in range(1, _MAX_IMPORT_RETRIES + 1):
    try:
        import torch
        import torchaudio
        break
    except OSError as _import_err:
        if _attempt < _MAX_IMPORT_RETRIES:
            print(
                f"[WARN] torch/torchaudio import falló (intento {_attempt}/{_MAX_IMPORT_RETRIES}): "
                f"{_import_err}  — reintentando en {_RETRY_DELAY_SECS}s...",
                file=sys.stderr, flush=True
            )
            time.sleep(_RETRY_DELAY_SECS)
        else:
            print(
                f"\n{'='*60}\n"
                f"ERROR FATAL: No se pudo cargar torch después de {_MAX_IMPORT_RETRIES} intentos.\n"
                f"Causa: {_import_err}\n\n"
                f"Posibles soluciones:\n"
                f"  1. Reinicia el PC (a veces WDAC libera bloqueos transitorios)\n"
                f"  2. Ejecuta de nuevo la pipeline\n"
                f"  3. Prueba manualmente:\n"
                f"     tts-venv\\Scripts\\python.exe -c \"import torch; print(torch.__version__)\"\n"
                f"{'='*60}",
                file=sys.stderr, flush=True
            )
            sys.exit(1)

# ── 4. Monkey-patch: forzar backend soundfile en torchaudio ───────────
#    Evita que torchaudio intente usar backends nativos que también
#    pueden ser bloqueados por Application Control.
def _load_soundfile(filepath, frame_offset=0, num_frames=-1,
                    normalize=True, channels_first=True, format=None):
    data, sample_rate = sf.read(str(filepath), dtype='float32', always_2d=True)
    tensor = torch.from_numpy(data.T)
    if not channels_first:
        tensor = tensor.T
    return tensor, sample_rate

torchaudio.load = _load_soundfile

# ── 5. TTS (depende de torch, torchaudio, numba-mock) ────────────────
from TTS.api import TTS


# ============================================================
# CONFIGURACIÓN DE RUTAS
# ============================================================

# Ruta base del proyecto — se resuelve automáticamente desde la ubicación
# de este script: app/generators/generar_audio.py → subir 2 niveles
# En Windows: C:\docker\projects\canal-reli (o donde esté el proyecto)
PROJECT_ROOT = Path(__file__).parent.parent.parent.absolute()
DATA_PATH    = PROJECT_ROOT / "data"

GUION_PATH     = DATA_PATH / "temp" / "guion.txt"
AUDIO_OUT_PATH = DATA_PATH / "temp" / "audio" / "1.wav"
AUDIO_REF_PATH = DATA_PATH / "input" / "recursos" / "referencia.wav"
TEMP_CHUNKS    = DATA_PATH / "temp" / "audio" / "tts_chunks"
LOGS_PATH      = DATA_PATH / "logs"

# Directorio del modelo — local al proyecto, no en AppData
# TTS_HOME hace que la librería TTS busque modelos aquí
TTS_MODEL_DIR  = PROJECT_ROOT / "tts-model"
os.environ["TTS_HOME"] = str(TTS_MODEL_DIR)

# Parámetros TTS
MODEL_NAME       = "tts_models/multilingual/multi-dataset/xtts_v2"
LANGUAGE         = "es"
MAX_CHARS        = 220   # Máximo de caracteres por chunk (evita alucinaciones)
TEMPERATURE      = 0.80
REPETITION_PEN   = 2.0
TOP_K            = 50
TOP_P            = 0.90

# Umbral de silencio para normalización (amplitud normalizada 0.0-1.0)
SILENCE_THRESHOLD = 0.01   # Muestras por debajo de esto se consideran silencio

# ── PARÁMETRO PRINCIPAL DE CONTROL DE PAUSAS ──────────────────
# Duración máxima permitida para cualquier silencio en el audio.
# Silencios más largos se recortan hasta este valor.
# Referencia: pausa natural de punto en locución = 0.5-0.6s
#   0.40s → ritmo rápido, casi sin pausas
#   0.55s → locución profesional estándar (recomendado)
#   0.70s → pausas largas, tono solemne
MAX_SILENCE_SECS  = 0.4


# ============================================================
# LOGGING
# ============================================================

LOGS_PATH.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [%(levelname)s]  %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(str(LOGS_PATH / "tts_generator.log"), encoding="utf-8")
    ]
)
logger = logging.getLogger(__name__)


# ============================================================
# FUNCIONES DE TEXTO
# ============================================================

def limpiar_texto(texto: str) -> str:
    """
    Limpia el texto del guion eliminando elementos no pronunciables.
    - Elimina líneas vacías múltiples
    - Normaliza espacios
    - Elimina marcadores especiales de formato
    """
    # Eliminar líneas con solo guiones o asteriscos (separadores)
    texto = re.sub(r'^\s*[-*=_]{3,}\s*$', '', texto, flags=re.MULTILINE)
    # Normalizar múltiples saltos de línea a uno solo
    texto = re.sub(r'\n{3,}', '\n\n', texto)
    # Reemplazar saltos de línea simples por espacio (flujo de párrafo)
    texto = re.sub(r'(?<!\n)\n(?!\n)', ' ', texto)
    # Normalizar espacios múltiples
    texto = re.sub(r' {2,}', ' ', texto)
    return texto.strip()


def dividir_en_chunks(texto: str, max_chars: int = MAX_CHARS) -> list[str]:
    """
    Divide el texto en chunks respetando oraciones completas.
    Nunca corta en medio de una oración.
    El último carácter de cada chunk siempre es puntuación de cierre.
    """
    # Separar por fin de oración: punto, exclamación, interrogación
    # Mantiene el separador al final de cada fragmento
    oraciones = re.split(r'(?<=[.!?])\s+', texto.strip())
    oraciones = [o.strip() for o in oraciones if o.strip()]

    chunks = []
    chunk_actual = ""

    for oracion in oraciones:
        # Si la oración sola ya supera el límite, dividirla por comas
        if len(oracion) > max_chars:
            sub_partes = re.split(r'(?<=,)\s+', oracion)
            for parte in sub_partes:
                if len(chunk_actual) + len(parte) + 1 <= max_chars:
                    chunk_actual = (chunk_actual + " " + parte).strip()
                else:
                    if chunk_actual:
                        chunks.append(chunk_actual)
                    chunk_actual = parte
        elif len(chunk_actual) + len(oracion) + 1 <= max_chars:
            chunk_actual = (chunk_actual + " " + oracion).strip()
        else:
            if chunk_actual:
                chunks.append(chunk_actual)
            chunk_actual = oracion

    if chunk_actual:
        chunks.append(chunk_actual)

    return chunks


# ============================================================
# FUNCIONES DE AUDIO
# ============================================================

def combinar_wavs(archivos: list[Path], salida: Path) -> None:
    """Combina múltiples archivos WAV en un único archivo."""
    datos_combinados = []
    params = None

    for archivo in archivos:
        with wave.open(str(archivo), 'rb') as f:
            if params is None:
                params = f.getparams()
            frames = f.readframes(f.getnframes())
            datos_combinados.append(
                np.frombuffer(frames, dtype=np.int16)
            )

    if not datos_combinados:
        raise ValueError("No hay chunks de audio para combinar")

    audio_final = np.concatenate(datos_combinados)

    with wave.open(str(salida), 'wb') as f:
        f.setparams(params)
        f.writeframes(audio_final.tobytes())


def normalizar_silencios(ruta_wav: Path,
                         threshold: float = SILENCE_THRESHOLD,
                         max_secs: float = MAX_SILENCE_SECS) -> None:
    """
    Recorre todo el audio y recorta cualquier segmento silencioso que supere
    max_secs, dejándolo exactamente en max_secs.
    Aplica tanto a silencios internos como al silencio final.

    Parámetros:
        threshold : amplitud normalizada por debajo de la cual un sample
                    se considera silencio (0.0 - 1.0)
        max_secs  : duración máxima permitida para cualquier pausa.
                    Ver MAX_SILENCE_SECS en la sección de configuración.
    """
    with wave.open(str(ruta_wav), 'rb') as f:
        params     = f.getparams()
        framerate  = f.getframerate()
        n_channels = f.getnchannels()
        sampwidth  = f.getsampwidth()
        frames     = f.readframes(f.getnframes())

    if sampwidth == 2:
        dtype   = np.int16
        max_val = 32768.0
    elif sampwidth == 4:
        dtype   = np.int32
        max_val = 2147483648.0
    else:
        logger.warning("Formato de audio no soportado para normalización de silencios")
        return

    audio = np.frombuffer(frames, dtype=dtype).astype(np.float32)

    # Para detección de silencio usar señal mono
    if n_channels == 2:
        audio_mono = audio.reshape(-1, 2).mean(axis=1)
    else:
        audio_mono = audio

    amplitud_norm = np.abs(audio_mono) / max_val
    es_silencio   = amplitud_norm < threshold

    max_frames_silencio = int(max_secs * framerate)

    # Construir máscara de samples a mantener
    # Para audio multicanal, cada "frame" ocupa n_channels samples
    n_frames = len(audio_mono)
    mantener = np.ones(n_frames, dtype=bool)

    i = 0
    segmentos_recortados = 0
    frames_recortados    = 0

    while i < n_frames:
        if es_silencio[i]:
            # Encontrar el final de este segmento de silencio
            j = i
            while j < n_frames and es_silencio[j]:
                j += 1
            duracion_frames = j - i
            if duracion_frames > max_frames_silencio:
                # Mantener solo max_frames_silencio, descartar el resto
                mantener[i + max_frames_silencio:j] = False
                segmentos_recortados += 1
                frames_recortados    += duracion_frames - max_frames_silencio
            i = j
        else:
            i += 1

    if segmentos_recortados == 0:
        logger.info(f"   Sin silencios excesivos (umbral: {max_secs}s)")
        return

    secs_recortados = frames_recortados / framerate
    logger.info(f"   Normalizados {segmentos_recortados} silencios "
                f"({secs_recortados:.2f}s recortados, umbral: {max_secs}s)")

    # Reconstruir audio aplicando la máscara
    if n_channels == 2:
        # Expandir máscara de frames a samples (cada frame = 2 samples)
        mascara_samples = np.repeat(mantener, 2)
    else:
        mascara_samples = mantener

    audio_normalizado = audio[mascara_samples]

    # Volver al dtype original
    audio_final = audio_normalizado.astype(dtype)

    with wave.open(str(ruta_wav), 'wb') as f:
        f.setparams(params)
        f.writeframes(audio_final.tobytes())


# ============================================================
# FUNCIÓN PRINCIPAL
# ============================================================

def generar_audio(guion_path: Path, audio_out: Path, audio_ref: Path) -> None:

    logger.info("=" * 55)
    logger.info("GENERADOR DE AUDIO TTS — canal-reli")
    logger.info("=" * 55)

    # Verificar archivos necesarios
    if not guion_path.exists():
        logger.error(f"No se encuentra el guion: {guion_path}")
        sys.exit(1)

    if not audio_ref.exists():
        logger.error(f"No se encuentra el audio de referencia: {audio_ref}")
        logger.error(f"Coloca el audio de referencia en {audio_ref}")
        sys.exit(1)

    # Preparar carpeta de chunks temporales
    TEMP_CHUNKS.mkdir(parents=True, exist_ok=True)
    audio_out.parent.mkdir(parents=True, exist_ok=True)

    # Leer y limpiar guion
    logger.info(f"Leyendo guion: {guion_path}")
    with open(guion_path, 'r', encoding='utf-8') as f:
        texto_raw = f.read()

    texto = limpiar_texto(texto_raw)
    logger.info(f"Texto limpiado: {len(texto)} caracteres")

    # Dividir en chunks
    chunks = dividir_en_chunks(texto, MAX_CHARS)
    logger.info(f"Texto dividido en {len(chunks)} chunks (max {MAX_CHARS} chars)")

    # Cargar modelo TTS
    device = "cuda" if torch.cuda.is_available() else "cpu"
    logger.info(f"Cargando modelo XTTS v2 en {device}...")

    tts = TTS(MODEL_NAME).to(device)
    logger.info("Modelo cargado correctamente")

    # Generar audio por chunks
    archivos_chunks = []
    tiempo_inicio   = time.time()

    for i, chunk in enumerate(chunks):
        chunk_path = TEMP_CHUNKS / f"chunk_{i:04d}.wav"
        logger.info(f"   [{i+1}/{len(chunks)}] {len(chunk)} chars: {chunk[:60]}...")

        try:
            tts.tts_to_file(
                text=chunk,
                speaker_wav=str(audio_ref),
                language=LANGUAGE,
                file_path=str(chunk_path),
                split_sentences=True,
                temperature=TEMPERATURE,
                repetition_penalty=REPETITION_PEN,
                top_k=TOP_K,
                top_p=TOP_P
            )
            normalizar_silencios(chunk_path)
            archivos_chunks.append(chunk_path)

        except Exception as e:
            logger.error(f"Error en chunk {i}: {e}")
            logger.error(f"Chunk problemático: {chunk}")
            # Continuar con el siguiente chunk en lugar de abortar
            continue

    if not archivos_chunks:
        logger.error("No se generó ningún chunk de audio")
        sys.exit(1)

    tiempo_generacion = time.time() - tiempo_inicio
    logger.info(f"Audio generado en {tiempo_generacion:.1f}s ({len(archivos_chunks)}/{len(chunks)} chunks OK)")

    # Combinar todos los chunks
    logger.info("Combinando chunks...")
    combinar_wavs(archivos_chunks, audio_out)
    logger.info(f"Audio combinado: {audio_out}")

    # Normalizar silencios del audio final combinado
    logger.info("Normalizando silencios del audio final...")
    normalizar_silencios(audio_out)

    # Limpiar chunks temporales
    logger.info("Limpiando chunks temporales...")
    for chunk_path in archivos_chunks:
        chunk_path.unlink(missing_ok=True)
    try:
        TEMP_CHUNKS.rmdir()
    except OSError:
        pass  # No vacía, ignorar

    # Resumen final
    with wave.open(str(audio_out), 'rb') as f:
        duracion = f.getnframes() / f.getframerate()

    logger.info("=" * 55)
    logger.info(f"✅ Audio generado correctamente")
    logger.info(f"   Archivo:  {audio_out}")
    logger.info(f"   Duración: {duracion:.1f}s ({duracion/60:.1f} min)")
    logger.info(f"   Tiempo:   {tiempo_generacion:.1f}s de procesamiento")
    logger.info("=" * 55)


# ============================================================
# ENTRADA
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="Generador de audio TTS para canal-reli"
    )
    parser.add_argument(
        "--guion",
        type=str,
        default=str(GUION_PATH),
        help=f"Ruta al guion de texto (default: {GUION_PATH})"
    )
    parser.add_argument(
        "--salida",
        type=str,
        default=str(AUDIO_OUT_PATH),
        help=f"Ruta del audio de salida (default: {AUDIO_OUT_PATH})"
    )
    parser.add_argument(
        "--referencia",
        type=str,
        default=str(AUDIO_REF_PATH),
        help=f"Ruta al audio de referencia (default: {AUDIO_REF_PATH})"
    )
    args = parser.parse_args()

    generar_audio(
        guion_path=Path(args.guion),
        audio_out=Path(args.salida),
        audio_ref=Path(args.referencia)
    )


if __name__ == "__main__":
    main()