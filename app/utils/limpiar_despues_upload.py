#!/usr/bin/env python3
"""
Script de limpieza post-upload
- Elimina archivos temporales del video
- Borra la primera fila de datos (fila 2) del calendario.csv
- Elimina backups antiguos del calendario.csv
- Elimina datos binarios de ejecuciones anteriores de n8n
"""

import os
import csv
import shutil
import glob
from datetime import datetime
from app.config import settings

# ─────────────────────────────────────────────
# RUTAS — ajusta si es necesario
# ─────────────────────────────────────────────
TEMP_DIR = str(settings.temp_path)
CALENDARIO_CSV = str(settings.calendario_path)

# Ruta interna del contenedor donde n8n guarda los datos binarios.
# Comprueba en tu docker-compose.yml a qué ruta del contenedor mapeas
# el volumen de n8n_data. Ejemplos habituales:
#   /home/node/.n8n/binaryData/
#   /root/.n8n/binaryData/
#   /data/binaryData/
N8N_BINARY_DATA_DIR = "/n8n_data/binaryData/workflows"

# ID del workflow (la carpeta que aparece en la ruta de Windows)
N8N_WORKFLOW_ID = "p-GNi6l4XFP_14EPehyUR"

# ─────────────────────────────────────────────
# Archivos temporales a eliminar
# ─────────────────────────────────────────────
def limpiar_archivos_temporales():
    """Elimina archivos temporales del directorio reli."""
    print("Limpiando archivos temporales...")

    eliminados = []
    no_encontrados = []

    # Lista explícita de archivos individuales
    archivos_sueltos = [
        settings.temp_path / "guion.txt",
        settings.temp_path / "intro_temp.txt",
        settings.temp_path / "oracion_final.txt",
        settings.output_videos_path / "video.mp4",
        settings.logs_path / "ffmpeg_step3.log",
    ]

    # Carpetas que queremos vaciar completamente
    carpetas_a_vaciar = [
        settings.temp_audio_path,
        settings.temp_subtitles_path,
        settings.temp_video_path
    ]

    # 1. Borrar archivos sueltos
    for archivo in archivos_sueltos:
        if archivo.exists():
            try:
                archivo.unlink()
                eliminados.append(archivo.name)
                print(f"  ✓ Eliminado: {archivo.name}")
            except Exception as e:
                print(f"  ✗ Error eliminando {archivo.name}: {e}")
        else:
            no_encontrados.append(archivo.name)
            print(f"  · No encontrado: {archivo.name}")

    # 2. Vaciar carpetas
    for carpeta in carpetas_a_vaciar:
        if carpeta.exists() and carpeta.is_dir():
            for filepath in carpeta.iterdir():
                try:
                    if filepath.is_file():
                        filepath.unlink()
                        eliminados.append(filepath.name)
                        print(f"  ✓ Eliminado ({carpeta.name}): {filepath.name}")
                except Exception as e:
                    print(f"  ✗ Error eliminando {filepath.name}: {e}")

    return eliminados, no_encontrados


def borrar_primera_fila_csv():
    """Borra la primera fila de datos (fila 2) del calendario.csv."""
    print(f"\nProcesando calendario: {CALENDARIO_CSV}")

    if not os.path.exists(CALENDARIO_CSV):
        print(f"  ✗ ERROR: No se encuentra {CALENDARIO_CSV}")
        return False

    # Hacer backup
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = f"{CALENDARIO_CSV}.backup_{timestamp}"

    try:
        shutil.copy2(CALENDARIO_CSV, backup_path)
        print(f"  💾 Backup creado: {os.path.basename(backup_path)}")
    except Exception as e:
        print(f"  · No se pudo crear backup: {e}")
        backup_path = None

    # Leer CSV
    try:
        with open(CALENDARIO_CSV, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            filas = list(reader)

        if len(filas) <= 1:
            print("  · El CSV solo tiene encabezados, no hay datos para borrar")
            return True

        fila_borrada = filas[1] if len(filas) > 1 else None
        if fila_borrada:
            titulo_borrado = fila_borrada[1] if len(
                fila_borrada) > 1 else "Sin título"
            print(f"  ✓ Borrando video: {titulo_borrado}")

        nuevas_filas = [filas[0]] + filas[2:]

        with open(CALENDARIO_CSV, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerows(nuevas_filas)

        print(f"  ✓ Fila eliminada. Filas restantes: {len(nuevas_filas) - 1}")
        return True

    except Exception as e:
        print(f"  ✗ ERROR procesando CSV: {e}")
        if backup_path and os.path.exists(backup_path):
            shutil.copy2(backup_path, CALENDARIO_CSV)
            print("  · Backup restaurado")
        return False


def limpiar_backups_csv():
    """
    Elimina todos los archivos de backup del calendario (*.backup_*).
    Se ejecuta DESPUÉS de borrar_primera_fila_csv, por lo que el backup
    recién creado también se borrará. Si quieres conservar el último
    backup cambia el parámetro keep_last a True.
    """
    print(f"\nLimpiando backups de calendario...")

    patron = f"{CALENDARIO_CSV}.backup_*"
    backups = glob.glob(patron)

    if not backups:
        print("  · No hay backups que eliminar")
        return 0

    eliminados = 0
    for backup in sorted(backups):
        try:
            os.remove(backup)
            eliminados += 1
            print(f"  ✓ Eliminado: {os.path.basename(backup)}")
        except Exception as e:
            print(f"  ✗ Error eliminando {os.path.basename(backup)}: {e}")

    return eliminados


def limpiar_binary_data_n8n():
    """
    Elimina las carpetas de datos binarios de ejecuciones anteriores de n8n.

    n8n guarda una copia del archivo binario (p. ej. el vídeo) por cada
    ejecución del workflow. Una vez subido el vídeo a YouTube estos datos
    ya no son necesarios y pueden eliminarse con seguridad.

    IMPORTANTE: verifica que N8N_BINARY_DATA_DIR apunte a la ruta correcta
    dentro de tu contenedor antes de ejecutar este script.
    """
    print(f"\nLimpiando datos binarios de n8n...")

    executions_dir = os.path.join(
        N8N_BINARY_DATA_DIR, N8N_WORKFLOW_ID, "executions")

    if not os.path.exists(executions_dir):
        print(f"  · Directorio no encontrado: {executions_dir}")
        print(f"    (Comprueba N8N_BINARY_DATA_DIR en la configuración del script)")
        return 0

    carpetas = [
        d for d in os.listdir(executions_dir)
        if os.path.isdir(os.path.join(executions_dir, d))
    ]

    if not carpetas:
        print("  · No hay carpetas de ejecución que eliminar")
        return 0

    eliminadas = 0
    espacio_liberado = 0

    for carpeta in sorted(carpetas):
        ruta_carpeta = os.path.join(executions_dir, carpeta)
        try:
            # Calcular tamaño antes de borrar
            tamaño = sum(
                os.path.getsize(os.path.join(dp, f))
                for dp, _, filenames in os.walk(ruta_carpeta)
                for f in filenames
            )
            shutil.rmtree(ruta_carpeta)
            espacio_liberado += tamaño
            eliminadas += 1
            print(
                f"  ✓ Eliminada ejecución {carpeta} ({tamaño / 1_048_576:.1f} MB)")
        except Exception as e:
            print(f"  ✗ Error eliminando ejecución {carpeta}: {e}")

    print(f"  💾 Espacio liberado: {espacio_liberado / 1_048_576:.1f} MB")
    return eliminadas


def main():
    print("=" * 60)
    print("LIMPIEZA POST-UPLOAD")
    print("=" * 60)

    # Paso 1: Limpiar archivos temporales del vídeo
    eliminados, no_encontrados = limpiar_archivos_temporales()

    # Paso 2: Borrar primera fila del CSV
    csv_ok = borrar_primera_fila_csv()

    # Paso 3: Eliminar backups del CSV
    backups_eliminados = limpiar_backups_csv()

    # Paso 4: Eliminar datos binarios de ejecuciones anteriores de n8n
    ejecuciones_eliminadas = limpiar_binary_data_n8n()

    # Resumen
    print("\n" + "=" * 60)
    print("📊 RESUMEN")
    print("=" * 60)
    print(f"  Archivos temporales eliminados : {len(eliminados)}")
    print(f"  Archivos no encontrados        : {len(no_encontrados)}")
    print(f"  CSV actualizado                : {'SÍ' if csv_ok else 'NO'}")
    print(f"  Backups CSV eliminados         : {backups_eliminados}")
    print(f"  Ejecuciones n8n limpiadas      : {ejecuciones_eliminadas}")
    print("=" * 60)

    return csv_ok


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
