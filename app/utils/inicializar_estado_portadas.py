"""
inicializar_estado_portadas.py
==============================
Genera o regenera estado_portadas.json escaneando las carpetas de imágenes
de portada en recursos/portadas/.

Ubicación:  C:\\docker\\projects\\canal-reli\\app\\utils\\inicializar_estado_portadas.py
Ejecutar:   python app/utils/inicializar_estado_portadas.py
            (o doble clic en inicializar_estado_portadas.bat desde la carpeta portadas)

Comportamiento:
  - Si estado_portadas.json NO existe → lo crea desde cero (ultimo_usado=0).
  - Si estado_portadas.json YA existe → preserva los valores de ultimo_usado
    existentes y solo actualiza 'total' según los archivos que haya en cada
    carpeta ahora mismo. Así, añadir o quitar imágenes no resetea el contador.
  - Las carpetas que ya no existen en disco se eliminan del JSON.
  - Las carpetas nuevas que no estaban en el JSON se añaden con ultimo_usado=0.

Formatos de imagen aceptados: .jpg .jpeg .png .webp
"""

import json
import sys
from pathlib import Path

# ─── Rutas ──────────────────────────────────────────────────────────────────
# El script vive en app/utils/ → subir tres niveles llega a la raíz del proyecto
PROJECT_ROOT   = Path(__file__).parent.parent.parent
PORTADAS_ROOT  = PROJECT_ROOT / "data" / "input" / "recursos" / "portadas"
ESTADO_PATH    = PORTADAS_ROOT / "estado_portadas.json"

EXTENSIONES    = {".jpg", ".jpeg", ".png", ".webp"}

# ─── Funciones ───────────────────────────────────────────────────────────────

def contar_imagenes(carpeta: Path) -> int:
    """Cuenta archivos de imagen válidos dentro de una carpeta."""
    return sum(
        1 for f in carpeta.iterdir()
        if f.is_file() and f.suffix.lower() in EXTENSIONES
    )


def escanear_carpetas() -> dict[str, int]:
    """Devuelve {nombre_carpeta: total_imagenes} para cada subcarpeta válida."""
    resultado = {}
    if not PORTADAS_ROOT.exists():
        print(f"ERROR: No existe la carpeta de portadas: {PORTADAS_ROOT}")
        sys.exit(1)

    for sub in sorted(PORTADAS_ROOT.iterdir()):
        if not sub.is_dir():
            continue
        total = contar_imagenes(sub)
        if total == 0:
            print(f"  AVISO: '{sub.name}' está vacía — se omite")
            continue
        resultado[sub.name] = total

    return resultado


def cargar_estado_existente() -> dict:
    """Lee el JSON actual; devuelve {} si no existe o está corrupto."""
    if not ESTADO_PATH.exists():
        return {}
    try:
        with open(ESTADO_PATH, encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        print(f"  AVISO: No se pudo leer estado_portadas.json ({e}) — se crea desde cero")
        return {}


def generar_estado(carpetas: dict[str, int], estado_actual: dict) -> dict:
    """
    Combina el escaneo de disco con el estado existente:
      - Carpetas nuevas  → ultimo_usado = 0
      - Carpetas ya conocidas → conserva ultimo_usado, actualiza total
      - Carpetas desaparecidas → se eliminan
    """
    nuevo_estado = {}
    for nombre, total in carpetas.items():
        if nombre in estado_actual:
            ultimo = estado_actual[nombre].get("ultimo_usado", 0)
            # Salvaguarda: si ultimo_usado >= total nuevo, resetear a 0
            if ultimo >= total:
                print(f"  AVISO: '{nombre}' ultimo_usado={ultimo} >= total={total} → reseteado a 0")
                ultimo = 0
        else:
            ultimo = 0
        nuevo_estado[nombre] = {"ultimo_usado": ultimo, "total": total}
    return nuevo_estado


def guardar_estado(estado: dict):
    """Escribe el JSON con indentación legible."""
    ESTADO_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(ESTADO_PATH, "w", encoding="utf-8") as f:
        json.dump(estado, f, ensure_ascii=False, indent=2)


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    print("=" * 56)
    print("  INICIALIZAR ESTADO PORTADAS — Canal Reli")
    print("=" * 56)
    print(f"Directorio de portadas: {PORTADAS_ROOT}\n")

    # 1. Escanear carpetas
    carpetas = escanear_carpetas()
    if not carpetas:
        print("ERROR: No se encontraron carpetas con imágenes en:", PORTADAS_ROOT)
        sys.exit(1)

    print(f"Carpetas encontradas: {len(carpetas)}")
    for nombre, total in carpetas.items():
        print(f"  {nombre:<20} → {total} imagen(es)")

    # 2. Cargar estado existente (si lo hay)
    estado_actual = cargar_estado_existente()
    if estado_actual:
        print(f"\nEstado previo encontrado ({len(estado_actual)} entradas) — se preservan contadores")
    else:
        print("\nNo había estado previo — creando desde cero")

    # 3. Generar nuevo estado
    nuevo_estado = generar_estado(carpetas, estado_actual)

    # 4. Guardar
    guardar_estado(nuevo_estado)
    print(f"\nestado_portadas.json guardado en:\n  {ESTADO_PATH}")

    # 5. Resumen final
    print("\nEstado resultante:")
    for nombre, datos in nuevo_estado.items():
        print(f"  {nombre:<20} ultimo_usado={datos['ultimo_usado']:>2}  total={datos['total']}")

    print("\nListo. Ejecuta este script cada vez que añadas o elimines imágenes.")


if __name__ == "__main__":
    main()
