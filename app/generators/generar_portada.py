#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
GENERAR PORTADA - Canal Reli

Flujo:
  1. Lee primera fila disponible de calendario.csv (o --fecha concreta)
  2. Lee TEMATICA -> selecciona carpeta de recursos
  3. Consulta estado_portadas.json -> rotacion circular
  4. Carga imagen de fondo desde recursos/portadas/TEMATICA/
  5. Compone portada final (degradado + texto + borde rojo)
  6. Guarda en data/output/portadas/portada.jpg
  7. Actualiza estado_portadas.json (incrementa indice de esa tematica)

Prerequisitos:
  - formatear_portadas.py: rellena TAMANOS y COLORES en calendario.csv
  - inicializar_estado_portadas.py: al anadir/quitar imagenes

Uso:
    python generar_portada.py                   # primera fila disponible
    python generar_portada.py --fecha 2026-04-01
    python generar_portada.py --test            # mock sin CSV
    python generar_portada.py --dry-run         # sin actualizar estado

Ubicacion: C:\\docker\\projects\\canal-reli\\app\\generators\\generar_portada.py
"""

import argparse
import csv
import json
import math
import sys
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont

# =============================================================================
# RUTAS
# =============================================================================
PROJECT_ROOT    = Path(__file__).parent.parent.parent
DATA_BASE       = PROJECT_ROOT / "data"
CALENDARIO_PATH = DATA_BASE / "input" / "calendario.csv"
PORTADAS_ROOT   = DATA_BASE / "input" / "recursos" / "portadas"
ESTADO_PATH     = PORTADAS_ROOT / "estado_portadas.json"
OUTPUT_DIR      = DATA_BASE / "output" / "portadas"
FONT_PATH       = Path(__file__).resolve().parent.parent / "fonts" / "Anton-Regular.ttf"

# =============================================================================
# CONFIGURACION DEL CANVAS
# =============================================================================
THUMBNAIL_W         = 1280
THUMBNAIL_H         = 720
BORDER_COLOR        = (255, 0, 0)
BORDER_PCT          = 0.02
GRADIENT_START_PCT  = 0.50
GRADIENT_END_PCT    = 0.72
TEXT_ZONE_START_PCT = 0.54
LINE_PADDING_PX     = 8
BORDER_TEXT_PAD_PX  = 16

COLOR_MAP = {
    "amarillo": (255, 205,   0),
    "rojo":     (255,   0,   0),
    "blanco":   (255, 255, 255),
}
STROKE_COLOR = (0, 0, 0)
STROKE_WIDTH = 2


# =============================================================================
# PARSEO DE DATOS DEL CALENDARIO
# =============================================================================

def leer_fila_calendario(fecha=None) -> dict:
    """
    Lee una fila del calendario.
    fecha=None -> primera fila con CONTENIDO y TAMANOS rellenos.
    fecha=str  -> busca esa fecha exacta.
    """
    with open(CALENDARIO_PATH, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    if fecha:
        for row in rows:
            if row["FECHA"].strip() == fecha:
                return row
        raise ValueError(f"Fecha '{fecha}' no encontrada en calendario.csv")

    for row in rows:
        tiene_contenido = row.get("CONTENIDO", "").strip()
        tiene_tamanos   = row.get("TAMANOS", "").strip() or row.get("TAMAÑOS", "").strip()
        if tiene_contenido and tiene_tamanos:
            return row

    raise ValueError(
        "No hay filas con CONTENIDO y TAMANOS en calendario.csv. "
        "Ejecuta primero: python formatear_portadas.py"
    )


def _get_col(row, *keys) -> str:
    """Lee la primera clave no vacia (maneja TAMANOS / TAMAÑOS)."""
    for k in keys:
        v = row.get(k, "").strip()
        if v:
            return v
    return ""


def parsear_contenido(raw: str) -> list:
    raw = raw.strip()
    try:
        result = json.loads(raw)
        if isinstance(result, list):
            return result
    except (json.JSONDecodeError, TypeError):
        pass
    return [l.strip() for l in raw.split("|") if l.strip()]


def parsear_tamanos(raw: str) -> list:
    raw = raw.strip()
    try:
        result = json.loads(raw)
        if isinstance(result, list):
            return [int(x) for x in result]
    except (json.JSONDecodeError, TypeError, ValueError):
        pass
    return [int(x.strip()) for x in raw.strip("[]").split(",") if x.strip()]


def parsear_colores(raw: str) -> list:
    raw = raw.strip()
    try:
        result = json.loads(raw)
        nombres = result if isinstance(result, list) else [raw]
    except (json.JSONDecodeError, TypeError):
        # Limpiar comillas simples y dobles del nombre
        nombres = [x.strip().strip("'" ).strip('"') for x in raw.strip("[]").split(",")]

    result = []
    for key in nombres:
        key = str(key).lower().strip()
        if key not in COLOR_MAP:
            print(f"  Color desconocido '{key}' -> usando blanco")
            key = "blanco"
        result.append(COLOR_MAP[key])
    return result


# =============================================================================
# SISTEMA DE ROTACION (estado_portadas.json)
# =============================================================================

def leer_estado() -> dict:
    if not ESTADO_PATH.exists():
        raise FileNotFoundError(
            f"No se encuentra estado_portadas.json en:\n  {ESTADO_PATH}\n\n"
            "Ejecuta primero:\n"
            "  python app/utils/inicializar_estado_portadas.py\n"
            "O doble clic en inicializar_estado_portadas.bat"
        )
    with open(ESTADO_PATH, encoding="utf-8") as f:
        return json.load(f)


def guardar_estado(estado: dict):
    with open(ESTADO_PATH, "w", encoding="utf-8") as f:
        json.dump(estado, f, ensure_ascii=False, indent=2)


def seleccionar_imagen(tematica: str, estado: dict):
    """
    Rotacion circular identica a generar_video.py:
        proximo = (ultimo_usado + 1) % total
        archivo = carpeta / f"{proximo + 1}.ext"  (numerados desde 1)

    Devuelve (ruta_imagen, tematica_key, nuevo_ultimo_usado).
    """
    tematica_upper = tematica.strip().upper()

    # Localizar carpeta (match exacto o case-insensitive)
    carpeta      = PORTADAS_ROOT / tematica_upper
    tematica_key = tematica_upper
    if not carpeta.exists():
        for sub in PORTADAS_ROOT.iterdir():
            if sub.is_dir() and sub.name.upper() == tematica_upper:
                carpeta      = sub
                tematica_key = sub.name
                break
        else:
            disponibles = [s.name for s in PORTADAS_ROOT.iterdir() if s.is_dir()]
            raise FileNotFoundError(
                f"No existe carpeta de tematica '{tematica_upper}' "
                f"en {PORTADAS_ROOT}\nCarpetas disponibles: {disponibles}"
            )

    # Buscar clave en estado (puede tener tildes o no)
    if tematica_key not in estado:
        for k in estado:
            if k.upper() == tematica_upper:
                tematica_key = k
                break
        else:
            raise KeyError(
                f"Tematica '{tematica_upper}' no encontrada en estado_portadas.json.\n"
                "Ejecuta: python app/utils/inicializar_estado_portadas.py"
            )

    ultimo  = estado[tematica_key]["ultimo_usado"]
    total   = estado[tematica_key]["total"]
    proximo = (ultimo + 1) % total   # 0-based
    numero  = proximo + 1             # nombre de archivo (1-based)

    for ext in (".jpg", ".jpeg", ".png", ".webp"):
        candidato = carpeta / f"{numero}{ext}"
        if candidato.exists():
            return candidato, tematica_key, proximo

    raise FileNotFoundError(
        f"No se encontro imagen '{numero}' (.jpg/.jpeg/.png/.webp) en {carpeta}\n"
        "Verifica que los archivos esten numerados: 1.jpg, 2.jpg, ...\n"
        "y ejecuta inicializar_estado_portadas.py para actualizar el total."
    )


def actualizar_estado(estado: dict, tematica_key: str, nuevo_ultimo: int):
    estado[tematica_key]["ultimo_usado"] = nuevo_ultimo
    guardar_estado(estado)


# =============================================================================
# COMPOSICION CON PILLOW
# =============================================================================

def _cargar_fuente(tamano: int) -> ImageFont.FreeTypeFont:
    # Anclar búsqueda al propio script para que funcione en Windows y Docker.
    # En Windows:  .../canal-reli/app/generators/ → sube a .../canal-reli/app/fonts/
    #              y luego a .../canal-reli/fonts/  (donde está la fuente real)
    # En Docker:   /app/generators/ → sube a /app/fonts/
    _script_dir = Path(__file__).resolve().parent
    rutas = [
        str(_script_dir.parent / "fonts" / "Anton-Regular.ttf"),        # app/fonts/   (Docker ✓ / Windows si copiada ahí)
        str(_script_dir.parent.parent / "fonts" / "Anton-Regular.ttf"), # project_root/fonts/  (Windows ✓)
        str(FONT_PATH),                                                  # fallback calculado
        "/app/fonts/Anton-Regular.ttf",                                  # Docker absoluto explícito
        "/usr/share/fonts/truetype/google-fonts/Poppins-Bold.ttf",
    ]
    for ruta in rutas:
        try:
            return ImageFont.truetype(ruta, tamano)
        except (IOError, OSError):
            continue
    print("  Fuente Anton no encontrada — usando default")
    return ImageFont.load_default()


def generar_imagen_mock() -> Image.Image:
    """Imagen sintetica para --test."""
    W, H = 896, 512
    img  = Image.new("RGB", (W, H), (0, 0, 0))
    draw = ImageDraw.Draw(img)
    for y in range(H):
        r = int(5  + (y / H) * 15)
        g = int(10 + (y / H) * 20)
        b = int(50 + (y / H) * 30)
        draw.line([(0, y), (W, y)], fill=(r, g, b))
    for radius in range(150, 0, -1):
        t         = 1 - (radius / 150)
        intensity = int(t * 180)
        cx, cy    = W // 3, H // 2
        draw.ellipse([cx-radius, cy-radius, cx+radius, cy+radius],
                     fill=(intensity//4, intensity//3, intensity))
    cx, cy = W // 3, H // 2
    draw.ellipse([cx-30, cy-80, cx+30, cy-20], fill=(15, 15, 20))
    draw.polygon([(cx-45, cy+80),(cx+45, cy+80),(cx+25, cy-20),(cx-25, cy-20)],
                 fill=(15, 15, 20))
    for angle in range(0, 360, 18):
        rad = math.radians(angle)
        x2  = cx + int(math.cos(rad) * 250)
        y2  = cy + int(math.sin(rad) * 250)
        draw.line([(cx, cy), (x2, y2)], fill=(30, 40, 90), width=1)
    return img


def componer_portada(imagen_fondo, lineas, tamanos, colores) -> Image.Image:
    """
    Compone la portada final 1280x720.
    Layout: borde rojo | imagen fondo | degradado | negro + texto
    """
    W, H = THUMBNAIL_W, THUMBNAIL_H
    border_px = max(1, int(H * BORDER_PCT))

    canvas = Image.new("RGB", (W, H), (0, 0, 0))

    # Imagen ocupa el canvas completo (1280x720).
    # El borde rojo se dibuja ENCIMA al final, igual que en los recursos.
    img_scaled = imagen_fondo.resize((W, H), Image.LANCZOS)
    canvas.paste(img_scaled, (0, 0))

    grad      = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    grad_draw = ImageDraw.Draw(grad)
    gx_start  = int(W * GRADIENT_START_PCT)
    gx_end    = int(W * GRADIENT_END_PCT)
    for x in range(gx_start, W):
        if x <= gx_end:
            t     = (x - gx_start) / (gx_end - gx_start)
            alpha = int(t * 255)
        else:
            alpha = 255
        grad_draw.line([(x, border_px), (x, H - border_px)], fill=(0, 0, 0, alpha))
    canvas_rgba = canvas.convert("RGBA")
    canvas_rgba.alpha_composite(grad)
    canvas = canvas_rgba.convert("RGB")

    draw  = ImageDraw.Draw(canvas)
    tz_x1 = int(W * TEXT_ZONE_START_PCT)
    tz_x2 = W - border_px - BORDER_TEXT_PAD_PX
    tz_y1 = border_px + BORDER_TEXT_PAD_PX
    tz_y2 = H - border_px - BORDER_TEXT_PAD_PX
    tz_h  = tz_y2 - tz_y1
    tz_w  = tz_x2 - tz_x1

    fonts  = [_cargar_fuente(s) for s in tamanos]
    bboxes = [draw.textbbox((0, 0), l.upper(), font=f, stroke_width=STROKE_WIDTH)
              for l, f in zip(lineas, fonts)]

    for i, (linea, tam, bbox) in enumerate(zip(lineas, tamanos, bboxes)):
        if bbox[2] - bbox[0] > tz_w:
            factor    = tz_w / (bbox[2] - bbox[0])
            new_size  = max(32, int(tam * factor))
            fonts[i]  = _cargar_fuente(new_size)
            bboxes[i] = draw.textbbox((0, 0), linea.upper(),
                                      font=fonts[i], stroke_width=STROKE_WIDTH)

    draw_ys = [0]
    for i in range(1, len(lineas)):
        next_y = draw_ys[-1] + bboxes[i-1][3] - bboxes[i][1] + LINE_PADDING_PX
        draw_ys.append(next_y)

    visual_top    = draw_ys[0]  + bboxes[0][1]
    visual_bottom = draw_ys[-1] + bboxes[-1][3]
    visual_height = visual_bottom - visual_top
    offset_y      = tz_y1 + (tz_h - visual_height) // 2 - visual_top

    for linea, font, color, bbox, dy in zip(lineas, fonts, colores, bboxes, draw_ys):
        texto = linea.upper()
        tw    = bbox[2] - bbox[0]
        x     = tz_x2 - tw
        draw.text((x, offset_y + dy), texto, font=font, fill=color,
                  stroke_width=STROKE_WIDTH, stroke_fill=STROKE_COLOR)

    draw = ImageDraw.Draw(canvas)
    draw.rectangle([0, 0, W-1, border_px-1],            fill=BORDER_COLOR)
    draw.rectangle([0, H-border_px, W-1, H-1],          fill=BORDER_COLOR)
    draw.rectangle([0, 0, border_px-1, H-1],            fill=BORDER_COLOR)
    draw.rectangle([W-border_px, 0, W-1, H-1],          fill=BORDER_COLOR)
    return canvas


# =============================================================================
# MAIN
# =============================================================================

TEST_DATA = {
    "lineas":  ["SALMO 35", "DESTRUYE", "A TU PEOR", "ENEMIGO", "EN 24 HORAS"],
    "tamanos": [160, 150, 130, 150, 110],
    "colores": ["amarillo", "blanco", "blanco", "rojo", "amarillo"],
}


def main():
    parser = argparse.ArgumentParser(
        description="Genera portada para Canal Reli",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--fecha",   default=None, help="Fecha del video (YYYY-MM-DD)")
    parser.add_argument("--test",    action="store_true",
                        help="Datos hardcoded + imagen mock")
    parser.add_argument("--dry-run", action="store_true",
                        help="Generar portada sin actualizar estado_portadas.json")
    parser.add_argument("--output",  default=None,
                        help="Ruta de salida alternativa")
    args = parser.parse_args()

    sep = "=" * 56
    print(f"\n{sep}")
    print("  GENERAR PORTADA — Canal Reli")
    print(f"{sep}\n")

    if args.test:
        print("Modo TEST: datos hardcoded, imagen mock")
        lineas       = TEST_DATA["lineas"]
        tamanos      = TEST_DATA["tamanos"]
        colores_rgb  = parsear_colores(json.dumps(TEST_DATA["colores"]))
        imagen_fondo = generar_imagen_mock()
        tematica_key = None
        nuevo_ultimo = None
        estado       = None
        print(f"   Lineas  : {lineas}")
        print(f"   Tamanos : {tamanos}")
        colores_test = TEST_DATA["colores"]
        print(f"   Colores : {colores_test}")

    else:
        if args.fecha:
            print(f"Fecha: {args.fecha}")
        else:
            print("Sin --fecha: leyendo primera fila disponible...")

        print("1. Leyendo calendario.csv...")
        fila = leer_fila_calendario(args.fecha)

        tam_raw = _get_col(fila, "TAMANOS", "TAMAÑOS")
        col_raw = _get_col(fila, "COLORES")
        if not tam_raw or not col_raw:
            print("\n  TAMANOS o COLORES vacios.")
            hint = f"--fecha {args.fecha}" if args.fecha else ""
            print(f"  Ejecuta: python formatear_portadas.py {hint}".strip())
            sys.exit(1)

        tematica    = fila.get("TEMATICA", "").strip()
        lineas      = parsear_contenido(fila["CONTENIDO"])
        tamanos     = parsear_tamanos(tam_raw)
        colores_rgb = parsear_colores(col_raw)
        fecha_label = fila["FECHA"].strip()

        print(f"   Fecha    : {fecha_label}")
        print(f"   Tematica : {tematica}")
        print(f"   Lineas   : {lineas}")
        print(f"   Tamanos  : {tamanos}  (suma: {sum(tamanos)})")
        print(f"   Colores  : {json.loads(col_raw)}")

        print(f"\n2. Seleccionando imagen para tematica '{tematica}'...")
        estado = leer_estado()
        ruta_imagen, tematica_key, nuevo_ultimo = seleccionar_imagen(tematica, estado)
        numero_archivo = nuevo_ultimo + 1
        total_imgs = estado[tematica_key]['total']
        print(f"   Imagen: {ruta_imagen.name}  (indice {numero_archivo}/{total_imgs})")

        print("3. Cargando imagen de fondo...")
        imagen_fondo = Image.open(ruta_imagen).convert("RGB")
        print(f"   Original: {imagen_fondo.size[0]}x{imagen_fondo.size[1]}px")

    paso = "4" if not args.test else "2"
    print(f"\n{paso}. Componiendo portada con Pillow...")
    portada = componer_portada(imagen_fondo, lineas, tamanos, colores_rgb)

    out_dir = Path(args.output) if args.output else OUTPUT_DIR
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "portada.jpg"
    portada.save(out_file, "JPEG", quality=95)
    print(f"\n  Portada: {out_file}")
    print(f"  Dimensiones: {portada.size[0]}x{portada.size[1]}px")

    if not args.test:
        if args.dry_run:
            print(f"\n  [DRY-RUN] estado_portadas.json NO actualizado")
            print(f"  En ejecucion real: {tematica_key} ultimo_usado -> {nuevo_ultimo}")
        else:
            actualizar_estado(estado, tematica_key, nuevo_ultimo)
            print(f"\n  estado_portadas.json actualizado: {tematica_key} ultimo_usado -> {nuevo_ultimo}")


if __name__ == "__main__":
    main()