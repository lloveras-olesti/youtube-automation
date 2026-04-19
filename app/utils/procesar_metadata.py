#!/usr/bin/env python3
"""
Transform: Procesar Metadata
==============================
Recibe el output de extraer_metadata.py (vía stdin o archivo JSON),
lo normaliza al formato exacto que espera subir_video.py y la API de YouTube,
y lo escribe en stdout para que el runner lo pase al siguiente paso.

Este script corre en el HOST (no en Docker) como paso de tipo "transform".

Ubicación: C:\\docker\\projects\\canal-reli\\app\\utils\\procesar_metadata.py
"""

from __future__ import annotations  # compatibilidad con Python 3.8

import json
import re
import sys


# ── NOTA: NO reasignamos sys.stdin con TextIOWrapper.
# El runner (run_pipeline.py) ya llama a subprocess.run con encoding="utf-8",
# por lo que stdin llega correctamente codificado. Reasignarlo causa que el
# buffer interno se pierda y stdin.read() devuelva vacío. ──────────────────

# Límites de la API de YouTube para tags
YT_TAG_MAX_CHARS     = 30   # máx caracteres por tag individual
YT_TAGS_TOTAL_CHARS  = 500  # máx caracteres totales (suma de todos los tags)
YT_TAGS_MAX_COUNT    = 30   # máx número de tags


def extraer_json_de_texto(texto: str) -> str:
    """
    Extrae el bloque JSON de un texto que puede contener líneas de log mezcladas.
    Busca el primer '{' y el último '}' balanceados.
    """
    stripped = texto.strip()
    if stripped.startswith("{"):
        return stripped

    match = re.search(r'(\{.*\})', texto, re.DOTALL)
    if match:
        return match.group(1)

    return stripped


def _eliminar_emojis(texto: str) -> str:
    """
    Elimina emojis y símbolos Unicode fuera del rango latino/básico.
    Cubre el rango de emojis modernos (U+1F000 en adelante) y símbolos misceláneos.
    """
    emoji_pattern = re.compile(
        "["
        "\U0001F600-\U0001F64F"  # emoticons
        "\U0001F300-\U0001F5FF"  # símbolos y pictogramas
        "\U0001F680-\U0001F6FF"  # transporte y mapa
        "\U0001F1E0-\U0001F1FF"  # banderas
        "\U00002702-\U000027B0"  # dingbats
        "\U000024C2-\U0001F251"  # símbolos varios
        "\U0001F900-\U0001F9FF"  # símbolos suplementarios
        "\U00002500-\U00002BEF"  # símbolos box drawing y misc
        "\U00010000-\U0010FFFF"  # suplementarios generales
        "]+",
        flags=re.UNICODE
    )
    return emoji_pattern.sub('', texto)


def sanitizar_tag(tag: str) -> str:
    """
    Limpia un tag individual para cumplir con las reglas de la API de YouTube:
    - Elimina emojis y símbolos Unicode fuera de rangos seguros
    - Elimina caracteres prohibidos: < > " '
    - Elimina saltos de línea y tabuladores
    - Colapsa espacios múltiples
    - Trunca a YT_TAG_MAX_CHARS caracteres
    """
    tag = _eliminar_emojis(tag)
    tag = re.sub(r'[<>"\']', '', tag)
    tag = re.sub(r'[\n\r\t]', ' ', tag)
    tag = re.sub(r' {2,}', ' ', tag).strip()

    # Truncar al límite por tag
    if len(tag) > YT_TAG_MAX_CHARS:
        tag = tag[:YT_TAG_MAX_CHARS].strip()

    return tag


def limpiar_tags(raw_tags) -> list[str]:
    """
    Normaliza y sanitiza etiquetas a lista de strings válidos para YouTube.

    Límites aplicados:
      - Máx YT_TAG_MAX_CHARS (30) caracteres por tag individual
      - Máx YT_TAGS_TOTAL_CHARS (500) caracteres totales
      - Máx YT_TAGS_MAX_COUNT (30) tags
      - Mínimo 2 caracteres por tag (descartar residuos)
    """
    if not raw_tags:
        return []
    if isinstance(raw_tags, str):
        tags = [t.strip() for t in raw_tags.split(",") if t.strip()]
    elif isinstance(raw_tags, list):
        tags = [str(t).strip() for t in raw_tags if t]
    else:
        tags = []

    # Sanitizar cada tag y descartar los que queden vacíos o muy cortos
    tags = [sanitizar_tag(t) for t in tags]
    tags = [t for t in tags if len(t) >= 2]

    # Aplicar límites globales de YouTube
    result = []
    total_chars = 0
    for tag in tags[:YT_TAGS_MAX_COUNT]:
        # +1 por la coma separadora
        if total_chars + len(tag) + 1 <= YT_TAGS_TOTAL_CHARS:
            result.append(tag)
            total_chars += len(tag) + 1

    return result


def truncar(texto: str, max_chars: int) -> str:
    """Trunca un texto al límite de caracteres de YouTube"""
    if len(texto) <= max_chars:
        return texto
    return texto[:max_chars - 3] + "..."


def procesar(raw: dict) -> dict:
    """
    Normaliza el dict de metadata al formato esperado por subir_video.py.

    Campos que puede recibir (output de extraer_metadata.py):
      titulo, title, TÍTULO         → título del vídeo
      descripcion, description      → descripción
      etiquetas, tags               → tags (string CSV o lista)
      salmo                         → número de salmo (para enriquecer título/desc)
      fecha                         → fecha de publicación (YYYY-MM-DD)
      privacy_status                → public | private | unlisted
      category_id                   → ID categoría YouTube (22 = People & Blogs)
      video_filename                → nombre del archivo .mp4

    Límites de la API de YouTube:
      Título:      máx 100 caracteres
      Descripción: máx 5000 caracteres
      Tags:        máx 500 caracteres totales, máx 30 tags, máx 30 chars/tag
    """
    # ── Título ────────────────────────────────────────────────────────────
    titulo = (
        raw.get("titulo")
        or raw.get("title")
        or raw.get("TÍTULO")
        or "Oración Cristiana"
    )
    titulo = truncar(str(titulo), 100)

    # ── Descripción ────────────────────────────────────────────────────────
    descripcion = (
        raw.get("descripcion")
        or raw.get("description")
        or raw.get("DESCRIPCION")
        or ""
    )
    canal_url = raw.get("canal_url", "")
    if canal_url and "suscríbete" not in descripcion.lower():
        descripcion = descripcion + f"\n\n✨ Suscríbete para más oraciones: {canal_url}"
    descripcion = truncar(str(descripcion), 5000)

    # ── Tags ──────────────────────────────────────────────────────────────
    raw_tags = raw.get("etiquetas") or raw.get("tags") or raw.get("ETIQUETAS") or []
    tags = limpiar_tags(raw_tags)

    # Si hay número de salmo, añadirlo como tag si no está ya (y si cabe en 30 chars)
    salmo = raw.get("salmo") or raw.get("SALMO")
    if salmo:
        salmo_tag = f"Salmo {salmo}"
        if len(salmo_tag) <= YT_TAG_MAX_CHARS and salmo_tag.lower() not in [t.lower() for t in tags]:
            tags.insert(0, salmo_tag)

    # ── Fecha de publicación ──────────────────────────────────────────────
    fecha = (
        raw.get("fecha")
        or raw.get("FECHA")
        or ""
    )

    # ── Resto de campos ───────────────────────────────────────────────────
    return {
        "titulo":         titulo,
        "descripcion":    descripcion,
        "tags":           tags,
        "fecha":          fecha,
        "category_id":    str(raw.get("category_id", "22")),
        "privacy_status": raw.get("privacy_status", "private"),
        "video_filename": raw.get("video_filename") or raw.get("filename"),
        "canal_url":      canal_url,
        "_raw": raw,
    }


def main():
    """
    Lee JSON de stdin (output del paso anterior en el pipeline).
    Escribe JSON procesado en stdout.
    """
    raw_input = ""
    try:
        raw_input = sys.stdin.read().strip()

        if not raw_input:
            sys.stderr.write("⚠️  procesar_metadata: stdin vacío, no hay metadata que procesar.\n")
            print(json.dumps({}))
            return

        json_text = extraer_json_de_texto(raw_input)
        raw = json.loads(json_text)

        if isinstance(raw, list):
            processed = [procesar(item) for item in raw]
        else:
            processed = procesar(raw)

        print(json.dumps(processed, ensure_ascii=False))

    except json.JSONDecodeError as e:
        sys.stderr.write(f"⚠️  Input no es JSON válido: {e}\n")
        sys.stderr.write(f"   Input recibido (primeros 500 chars): {raw_input[:500]}\n")
        print(json.dumps({}))

    except Exception as e:
        sys.stderr.write(f"❌ Error en procesar_metadata: {e}\n")
        import traceback
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()