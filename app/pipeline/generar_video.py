#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
GENERADOR DE VIDEO AUTOMÁTICO CON FFMPEG
Versión adaptada para Docker con selección automática de recursos
+ DEBUGGING MEJORADO PARA SUBTÍTULOS
"""

import os
import subprocess
import sys
import json
import shutil
from pathlib import Path
from app.config import settings

# ==================== CONFIGURACIÓN ====================
# FFmpeg y FFprobe están instalados en el contenedor y disponibles en PATH
FFMPEG_BIN = "ffmpeg"
FFPROBE_BIN = "ffprobe"

# Ruta base desde variable de entorno
DATA_PATH = os.environ.get("DATA_PATH", "/app/data")
FOLDER = str(settings.temp_video_path)

# Archivo de estado para tracking de recursos
ESTADO_FILE = str(settings.estado_recursos_path)

# Carpetas de videos
VIDEO_FOLDERS = [
    "foto1",
    "foto2",
    "foto3",
    "foto4",
    "foto5"
]

# Carpeta de música
MUSIC_FOLDER = "musica"

# Videos en orden (se repetirán en bucle)
VIDEOS = ["1.mp4", "2.mp4", "3.mp4", "4.mp4", "5.mp4"]

# Archivos de audio
AUDIO_VOICE = "1.wav"
AUDIO_MUSIC = "music1.wav"  # Se seleccionará automáticamente

# Subtítulos
SUBTITLES = "1.srt"

# Salida
OUTPUT = "video.mp4"
TEMP_CYCLE = "temp_cycle.mp4"
TEMP_VIDEO = "temp_video_no_subs.mp4"

# Estilo de subtítulos
FONT = "Arial"
FONT_SIZE = 15
FONT_COLOR = "&H00CDFF"  # Amarillo (RGB: 255, 205, 0)
OUTLINE_COLOR = "&H000000"  # Negro
OUTLINE_WIDTH = 1
SUBTITLE_MARGIN = 20

# Resolución
WIDTH = 1920
HEIGHT = 1080

# ==================== FUNCIONES DE TRACKING ====================


def cargar_estado():
    """Carga el estado de tracking de recursos"""
    if not os.path.exists(ESTADO_FILE):
        print(f"❌ Error: No se encuentra {ESTADO_FILE}")
        print("💡 Crea el archivo estado_recursos.json primero")
        sys.exit(1)

    with open(ESTADO_FILE, 'r') as f:
        return json.load(f)


def guardar_estado(estado):
    """Guarda el estado actualizado"""
    with open(ESTADO_FILE, 'w') as f:
        json.dump(estado, f, indent=2)


def seleccionar_recursos():
    """
    Selecciona automáticamente videos y música usando rotación circular.
    Copia los archivos seleccionados a las ubicaciones esperadas.
    """
    print("\n🎨 Seleccionando recursos automáticamente...")

    # Cargar estado
    estado = cargar_estado()

    # Seleccionar videos (1 de cada carpeta)
    for i, carpeta_nombre in enumerate(VIDEO_FOLDERS, 1):
        # Obtener índice del próximo video
        carpeta_key = carpeta_nombre
        ultimo = estado[carpeta_key]["ultimo_usado"]
        total = estado[carpeta_key]["total"]

        # Rotación circular: (ultimo + 1) % total
        proximo = (ultimo + 1) % total

        # Ruta del video seleccionado (los archivos están numerados 1.mp4, 2.mp4, ...)
        # pero el índice es 0-based, así que sumamos 1
        video_numero = proximo + 1
        origen = str(settings.videos_loop_path / carpeta_nombre / f"{video_numero}.mp4")
        destino = str(settings.temp_video_path / f"{i}.mp4")

        # Copiar video
        if not os.path.exists(origen):
            print(f"❌ Error: No se encuentra {origen}")
            sys.exit(1)

        shutil.copy(origen, destino)
        print(f"   ✅ {i}.mp4 ← {carpeta_nombre}/{video_numero}.mp4")

        # Actualizar estado
        estado[carpeta_key]["ultimo_usado"] = proximo

    # Seleccionar música
    carpeta_key = "musica"
    ultimo = estado[carpeta_key]["ultimo_usado"]
    total = estado[carpeta_key]["total"]

    # Rotación circular
    proximo = (ultimo + 1) % total

    # Los archivos de música son music1.WAV, music2.WAV, ...
    # Índice 0 → music1.WAV, índice 1 → music2.WAV
    music_numero = proximo + 1
    origen_music = str(settings.musica_path / f"music{music_numero}.WAV")
    destino_music = str(settings.temp_video_path / AUDIO_MUSIC)

    if not os.path.exists(origen_music):
        print(f"❌ Error: No se encuentra {origen_music}")
        sys.exit(1)

    shutil.copy(origen_music, destino_music)
    print(f"   🎵 music1.WAV ← musica/music{music_numero}.WAV")

    # Actualizar estado de música
    estado[carpeta_key]["ultimo_usado"] = proximo

    # Guardar estado actualizado
    guardar_estado(estado)

    print("✅ Recursos seleccionados y copiados correctamente\n")

# ==================== FUNCIONES ORIGINALES ====================


def convert_srt_to_uppercase(srt_path):
    """Convierte todos los textos del SRT a mayúsculas"""
    temp_srt = srt_path.replace('.srt', '_upper.srt')

    with open(srt_path, 'r', encoding='utf-8') as f:
        content = f.read()

    lines = content.split('\n')
    converted = []

    for line in lines:
        # Las líneas que NO son números ni timestamps se convierten a mayúsculas
        if line.strip() and not line[0].isdigit() and '-->' not in line:
            converted.append(line.upper())
        else:
            converted.append(line)

    with open(temp_srt, 'w', encoding='utf-8') as f:
        f.write('\n'.join(converted))

    return temp_srt


def get_duration(file_path):
    """Obtiene la duración de un archivo de audio/video en segundos"""
    cmd = [
        FFPROBE_BIN,
        "-v", "error",
        "-show_entries", "format=duration",
        "-of", "default=noprint_wrappers=1:nokey=1",
        file_path
    ]
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, check=True)
        return float(result.stdout.strip())
    except Exception as e:
        print(f"Error obteniendo duración de {file_path}: {e}")
        sys.exit(1)


def verificar_soporte_subtitulos():
    """Verifica que FFmpeg tiene soporte para subtítulos"""
    print("\n🔍 Verificando soporte de subtítulos en FFmpeg...")

    # Verificar filtros disponibles
    cmd_filters = [FFMPEG_BIN, "-filters"]
    result = subprocess.run(cmd_filters, capture_output=True, text=True)

    has_subtitles = "subtitles" in result.stdout
    has_ass = "ass" in result.stdout

    print(
        f"   - Filtro 'subtitles': {'✅ Disponible' if has_subtitles else '❌ NO disponible'}")
    print(
        f"   - Filtro 'ass': {'✅ Disponible' if has_ass else '❌ NO disponible'}")

    # Verificar fuentes disponibles
    cmd_fonts = ["fc-list", ":", "family"]
    try:
        result_fonts = subprocess.run(
            cmd_fonts, capture_output=True, text=True, check=False)
        if result_fonts.returncode == 0:
            fonts = result_fonts.stdout.strip().split('\n')
            print(f"   - Fuentes instaladas: {len(fonts)} encontradas")
            print(f"   - Primeras 5 fuentes: {fonts[:5]}")
        else:
            print("   ⚠️ fontconfig no disponible (fc-list no encontrado)")
    except FileNotFoundError:
        print("   ⚠️ fontconfig no instalado")

    if not has_subtitles and not has_ass:
        print("\n⚠️ ADVERTENCIA: FFmpeg no tiene soporte para subtítulos")
        print("   Posible solución: Usa una imagen con FFmpeg compilado con libass")

    return has_subtitles or has_ass


def main():
    print("=" * 60)
    print("GENERADOR DE VIDEO AUTOMÁTICO CON FFMPEG")
    print("=" * 60)

    # ==================== VERIFICACIÓN DE SOPORTE ====================
    verificar_soporte_subtitulos()

    # ==================== SELECCIÓN AUTOMÁTICA DE RECURSOS ====================
    # Esto selecciona y copia automáticamente:
    # - 5 videos (1 de cada carpeta foto1-foto5) como 1.mp4, 2.mp4, ...
    # - 1 música (rotando entre las disponibles) como music1.WAV
    seleccionar_recursos()

    # ==================== CÓDIGO ORIGINAL (SIN CAMBIOS) ====================

    # Rutas completas
    audio_voice_path = str(settings.get_temp_file("audio", AUDIO_VOICE))
    audio_music_path = str(settings.temp_video_path / AUDIO_MUSIC)
    subtitles_path = str(settings.get_temp_file("subtitles", SUBTITLES))
    output_path = str(settings.get_output_file("videos", OUTPUT))
    temp_cycle_path = str(settings.temp_video_path / TEMP_CYCLE)
    temp_video_path = str(settings.temp_video_path / TEMP_VIDEO)

    # Verificar que existen los archivos obligatorios
    for file_path, name in [
        (audio_voice_path, "Audio de voz"),
        (audio_music_path, "Música"),
    ]:
        if not os.path.exists(file_path):
            print(f"✗ Error: No se encuentra {name} en {file_path}")
            sys.exit(1)

    # Subtítulos: opcionales — si no existen, el vídeo se genera sin ellos
    use_subtitles = os.path.exists(subtitles_path)
    if use_subtitles:
        print(f"   ✓ Subtítulos encontrados: {subtitles_path}")
    else:
        print(f"   ⚠️  Sin subtítulos (no se encuentra {subtitles_path}) — vídeo sin subtítulos")

    video_paths = []
    for video in VIDEOS:
        video_path = os.path.join(FOLDER, video)
        if not os.path.exists(video_path):
            print(f"✗ Error: No se encuentra el video {video}")
            sys.exit(1)
        video_paths.append(video_path)

    print("\n✓ Todos los archivos encontrados")

    # Obtener duración de los audios
    print("\n📊 Analizando duración de archivos de audio...")
    duration_voice = get_duration(audio_voice_path)
    duration_music = get_duration(audio_music_path)
    target_duration = duration_voice + 2.0

    print(f"   - Audio de voz: {duration_voice:.2f} segundos")
    print(f"   - Música: {duration_music:.2f} segundos")
    print(f"   - Duración objetivo (voz + 2s): {target_duration:.2f} segundos")

    # PASO 1: Crear UN CICLO completo (5 videos concatenados)
    print("\n🔄 Paso 1/3: Creando ciclo único de 5 videos...")

    # Construir comando para concatenar los 5 videos en un ciclo
    cmd_inputs = [FFMPEG_BIN]
    for video_path in video_paths:
        cmd_inputs.extend(["-i", video_path])

    # Construir filtro para normalizar y concatenar los 5 videos
    filter_parts = []
    for i in range(len(video_paths)):
        filter_parts.append(
            f"[{i}:v]scale={WIDTH}:{HEIGHT}:force_original_aspect_ratio=decrease,"
            f"pad={WIDTH}:{HEIGHT}:(ow-iw)/2:(oh-ih)/2,setsar=1,fps=60[v{i}]"
        )

    concat_inputs = "".join(f"[v{i}]" for i in range(len(video_paths)))
    filter_parts.append(
        f"{concat_inputs}concat=n={len(video_paths)}:v=1:a=0[vout]")
    filter_complex = ";".join(filter_parts)

    cmd_cycle = cmd_inputs + [
        "-filter_complex", filter_complex,
        "-map", "[vout]",
        "-c:v", "libx264",
        "-preset", "medium",
        "-crf", "23",
        "-an",  # Sin audio por ahora
        "-y",
        temp_cycle_path
    ]

    try:
        subprocess.run(cmd_cycle, check=True)
        cycle_duration = get_duration(temp_cycle_path)
        print(f"   ✓ Ciclo creado ({cycle_duration:.2f} segundos)")
    except subprocess.CalledProcessError as e:
        print(f"\n✗ Error en paso 1: {e}")
        sys.exit(1)

    # PASO 2: Hacer loop del ciclo + añadir audio
    print("\n🎬 Paso 2/3: Generando video completo con audio en loop...")
    print("   (Esto puede tardar varios minutos...)")

    # Calcular cuántos loops necesitamos
    video_loops = int(target_duration / cycle_duration) + 1
    music_loops = int(target_duration / duration_music) + 1

    print(f"   - Loops de video necesarios: {video_loops}")
    print(f"   - Loops de música necesarios: {music_loops}")

    cmd_step2 = [
        FFMPEG_BIN,
        "-stream_loop", str(video_loops),
        "-i", temp_cycle_path,
        "-stream_loop", str(music_loops),
        "-i", audio_music_path,
        "-i", audio_voice_path,
        "-filter_complex",
        f"[0:v]trim=0:{target_duration},setpts=PTS-STARTPTS[v];"
        f"[1:a]atrim=0:{target_duration},asetpts=PTS-STARTPTS[music];"
        f"[2:a]asetpts=PTS-STARTPTS[voice];"
        f"[music][voice]amix=inputs=2:duration=first:dropout_transition=0[a]",
        "-map", "[v]",
        "-map", "[a]",
        "-t", str(target_duration),
        "-c:v", "libx264",
        "-preset", "medium",
        "-crf", "23",
        "-c:a", "aac",
        "-b:a", "192k",
        "-y",
        temp_video_path
    ]

    try:
        subprocess.run(cmd_step2, check=True)
        print("   ✓ Video base creado")
    except subprocess.CalledProcessError as e:
        print(f"\n✗ Error en paso 2: {e}")
        sys.exit(1)

    # PASO 3: Subtítulos (opcional)
    if not use_subtitles:
        # Sin subtítulos: el output del paso 2 es el vídeo final
        import shutil
        shutil.move(temp_video_path, output_path)
        print("\n✅ ¡Vídeo generado sin subtítulos!")
        print(f"📁 Ubicación: {output_path}")
        os.remove(temp_cycle_path)
        print("\n🧹 Archivos temporales eliminados")
        return

    # Convertir subtítulos a mayúsculas
    print("\n🔤 Convirtiendo subtítulos a mayúsculas...")
    subtitles_path = convert_srt_to_uppercase(subtitles_path)
    print(f"   ✓ Archivo temporal creado: {subtitles_path}")

    # Verificar contenido del SRT
    with open(subtitles_path, 'r', encoding='utf-8') as f:
        srt_preview = f.read()[:500]  # Primeros 500 caracteres
    print(f"\n📄 Vista previa del SRT (primeros 500 caracteres):")
    print("-" * 60)
    print(srt_preview)
    print("-" * 60)

    # PASO 3: Añadir subtítulos
    print("\n📝 Paso 3/3: Añadiendo subtítulos...")

    # EN LINUX NO NECESITAMOS ESTA CONVERSIÓN
    # La ruta ya está en formato Unix: /app/data/reli/1_upper.srt
    # NO hacemos replace de \ ni :
    subs_path_ffmpeg = subtitles_path  # Usar ruta tal cual

    print(f"   📍 Ruta del SRT: {subs_path_ffmpeg}")

    subtitle_style = (
        f"FontName={FONT},"
        f"FontSize={FONT_SIZE},"
        f"PrimaryColour={FONT_COLOR},"
        f"OutlineColour={OUTLINE_COLOR},"
        f"Outline={OUTLINE_WIDTH},"
        f"Bold=1,"
        f"Alignment=2,"
        f"MarginV={SUBTITLE_MARGIN}"
    )

    print(f"   🎨 Estilo de subtítulos: {subtitle_style}")

    cmd_step3 = [
        FFMPEG_BIN,
        "-i", temp_video_path,
        "-vf", f"subtitles='{subs_path_ffmpeg}':force_style='{subtitle_style}'",
        "-c:a", "copy",
        "-c:v", "libx264",
        "-preset", "medium",
        "-crf", "23",
        "-y",
        output_path
    ]

    print("\n🔧 Comando FFmpeg completo:")
    print(" ".join(cmd_step3))

    try:
        # Capturar stderr para ver errores de FFmpeg
        print("\n⏳ Ejecutando FFmpeg (esto puede tardar)...")
        result = subprocess.run(cmd_step3, check=True,
                                capture_output=True, text=True)

        # Guardar logs de FFmpeg para debugging
        log_file = str(settings.logs_path / "ffmpeg_step3.log")
        with open(log_file, 'w', encoding='utf-8') as f:
            f.write("=== COMANDO ===\n")
            f.write(" ".join(cmd_step3) + "\n\n")
            f.write("=== STDERR ===\n")
            f.write(result.stderr + "\n\n")
            f.write("=== STDOUT ===\n")
            f.write(result.stdout + "\n")

        print(f"\n📊 Log de FFmpeg guardado en: {log_file}")

        # Mostrar últimas líneas del stderr (suelen tener info importante)
        stderr_lines = result.stderr.strip().split('\n')
        print(f"\n📋 Últimas 10 líneas del log de FFmpeg:")
        print("-" * 60)
        for line in stderr_lines[-10:]:
            print(line)
        print("-" * 60)

        print("\n✅ ¡Video generado exitosamente!")
        print(f"📁 Ubicación: {output_path}")

        # Limpiar archivos temporales
        os.remove(temp_cycle_path)
        os.remove(temp_video_path)
        print("\n🧹 Archivos temporales eliminados")

    except subprocess.CalledProcessError as e:
        print(f"\n✗ Error en paso 3: {e}")
        print(f"\n⚠️ STDERR de FFmpeg:")
        print("-" * 60)
        print(e.stderr if hasattr(e, 'stderr') else "No stderr disponible")
        print("-" * 60)
        print("\n⚠️ Los archivos temporales están disponibles en:")
        print(f"   {temp_cycle_path}")
        print(f"   {temp_video_path}")
        sys.exit(1)


if __name__ == "__main__":
    main()