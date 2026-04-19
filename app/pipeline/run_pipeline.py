#!/usr/bin/env python3
"""
Runner Genérico de Pipelines
==============================
Motor de ejecución para cualquier workflow definido en workflows/*.json.
Lee la definición del pipeline y ejecuta cada paso en orden,
respetando dependencias.

DÓNDE EJECUTAR: En tu máquina Windows (HOST), NO dentro del contenedor Docker.

Uso básico:
#   python app/pipeline/run_pipeline.py --pipeline workflows/video-pipeline.json --fila 1
#
# Opciones completas:
#   --pipeline  Ruta al JSON del workflow (default: workflows/video-pipeline.json)
#   --fila      Fila del calendario a procesar (default: 1)
#   --dry-run   Simular sin ejecutar comandos reales
#   --no-upload Evitar el paso de subida a YouTube (útil para tests locales)
#   --desde     Reanudar desde un step_id concreto (útil si un paso falló)
#
# Ejemplos:
#   python app/pipeline/run_pipeline.py --pipeline workflows/video-pipeline.json --fila 2
#   python app/pipeline/run_pipeline.py --pipeline workflows/video-pipeline.json --fila 1 --dry-run
#   python app/pipeline/run_pipeline.py --pipeline workflows/video-pipeline.json --fila 1 --no-upload
#   python app/pipeline/run_pipeline.py --pipeline workflows/video-pipeline.json --fila 1 --desde subir-video

Integración con UI (project-manager):
  La UI lee workflows/*.json para listar workflows disponibles.
  Para ejecutar, llama a este script con --pipeline <ruta_al_json>.
  El output de stdout puede parsearse: líneas con "✅" indican éxito,
  líneas con "❌" indican fallo, "[PASO]" prefija el nombre del paso activo.

Cómo funciona:
  - Pasos type "command"   → docker exec canal-reli python <script> <args>
  - Pasos type "transform" → python <script> en Windows, recibe input por stdin
  - captures_output: true  → guarda stdout en data/temp/pipeline/<step_id>_output.json
  - input_from             → inyecta output del paso anterior como --input <archivo>

Ubicación: C:\\docker\\projects\\canal-reli\\app\\pipeline\\run_pipeline.py
"""

from __future__ import annotations
import os
import argparse
import json
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

# ── Rutas ────────────────────────────────────────────────────────────────────
# Fallback logic for PROJECT_ROOT and DATA_DIR
PROJECT_ROOT = Path(os.getenv("PROJECT_ROOT", Path(__file__).parent.parent.parent.absolute()))
DATA_DIR     = Path(os.getenv("DATA_DIR", PROJECT_ROOT / "data"))

DEFAULT_PIPELINE  = PROJECT_ROOT / "workflows" / "video-pipeline.json"
TEMP_PIPELINE     = DATA_DIR / "temp" / "pipeline"
LOGS_DIR          = DATA_DIR / "logs" / "pipeline"
CONTAINER         = os.getenv("CONTAINER_NAME", "canal-reli")

# Raíz de data/ dentro del contenedor (según docker-compose: ./data:/app/data)
CONTAINER_DATA_ROOT = "/app/data"


def to_container_path(host_path: Path) -> str:
    """
    Convierte una ruta Windows del host a la ruta equivalente en el contenedor.
    """
    try:
        return CONTAINER_DATA_ROOT + "/" + host_path.relative_to(DATA_DIR).as_posix()
    except ValueError:
        return host_path.as_posix()


def restore_outputs_from_disk() -> dict[str, str]:
    """
    Cuando se reanuda con --desde, carga en memoria los outputs de pasos
    anteriores que el runner guardó en data/temp/pipeline/*.json.
    Sin esto, --input nunca se inyecta al reanudar y subir_video falla.
    """
    restored: dict[str, str] = {}
    if not TEMP_PIPELINE.exists():
        return restored
    for f in TEMP_PIPELINE.glob("*_output.json"):
        step_id = f.stem.replace("_output", "")
        try:
            restored[step_id] = f.read_text(encoding="utf-8")
        except Exception:
            pass
    if restored:
        print(f"   💾 Outputs restaurados desde disco: {list(restored.keys())}", flush=True)
    return restored


# ── Utilidades de log ─────────────────────────────────────────────────────────

def log(msg: str, level: str = "INFO"):
    ts     = datetime.now().strftime("%H:%M:%S")
    prefix = {
        "INFO":  "  ",
        "OK":    "✅",
        "ERR":   "❌",
        "WARN":  "⚠️ ",
        "STEP":  "▶️ ",
    }.get(level, "  ")
    line = f"[{ts}] {prefix} {msg}"
    print(line, flush=True)   # flush=True para que la UI reciba líneas en tiempo real


# ── Carga del pipeline ────────────────────────────────────────────────────────

def load_pipeline(pipeline_path: Path) -> dict:
    if not pipeline_path.exists():
        log(f"No se encuentra el pipeline: {pipeline_path}", "ERR")
        log("Verifica que el archivo JSON existe en workflows/", "ERR")
        sys.exit(1)
    with open(pipeline_path, encoding="utf-8") as f:
        return json.load(f)


# ── Ordenación topológica (respeta depends_on) ────────────────────────────────

def topological_sort(steps: list[dict]) -> list[dict]:
    step_map = {s["id"]: s for s in steps}
    visited  = set()
    result   = []

    def visit(step_id: str):
        if step_id in visited:
            return
        visited.add(step_id)
        for dep in step_map[step_id].get("depends_on", []):
            visit(dep)
        result.append(step_map[step_id])

    for s in steps:
        visit(s["id"])
    return result


# ── Verificación de Docker ────────────────────────────────────────────────────

def check_docker() -> bool:
    result = subprocess.run(
        ["docker", "inspect", "--format", "{{.State.Running}}", CONTAINER],
        capture_output=True, text=True, encoding="utf-8", errors="replace"
    )
    return result.returncode == 0 and result.stdout.strip() == "true"


# ── Ejecución de pasos ────────────────────────────────────────────────────────

def docker_exec(
    script_path: str,
    args: list[str],
    capture: bool,
    dry_run: bool
) -> tuple[bool, str]:
    """Ejecuta un script Python dentro del contenedor Docker."""
    cmd = ["docker", "exec", CONTAINER, "python", script_path] + args
    log(f"docker exec {CONTAINER} python {script_path} {' '.join(args)}")

    if dry_run:
        log("[DRY RUN] — comando no ejecutado", "WARN")
        return True, ""

    result = subprocess.run(cmd, capture_output=capture, text=True, encoding="utf-8", errors="replace")

    if not capture:
        pass  # output directo a terminal (visible en tiempo real en la UI)
    else:
        if result.stdout:
            print(result.stdout, flush=True)
        if result.stderr:
            print(result.stderr, flush=True)

    success = result.returncode == 0
    if not success:
        log(f"El paso falló (exit code {result.returncode})", "ERR")
        if result.stderr:
            print(result.stderr[-2000:], flush=True)

    return success, result.stdout if capture else ""


def run_transform(
    script_path: str,
    input_data: str,
    dry_run: bool
) -> tuple[bool, str]:
    """
    Ejecuta un script de transformación en Windows (host).
    Recibe input_data por stdin, escribe resultado en stdout.
    La ruta es relativa al PROJECT_ROOT.
    """
    full_path = PROJECT_ROOT / script_path
    if not full_path.exists():
        log(f"Transform no encontrado: {full_path}", "ERR")
        return False, ""

    log(f"python {script_path} (transform local)")

    if dry_run:
        log("[DRY RUN] — transform no ejecutado", "WARN")
        return True, input_data  # pass-through en dry run

    result = subprocess.run(
        [sys.executable, str(full_path)],
        input=input_data,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace"
    )
    success = result.returncode == 0
    if not success:
        log(f"Transform falló: {result.stderr}", "ERR")
    return success, result.stdout


def save_step_output(step_id: str, output: str):
    TEMP_PIPELINE.mkdir(parents=True, exist_ok=True)
    (TEMP_PIPELINE / f"{step_id}_output.json").write_text(output, encoding="utf-8")


def execute_step(
    step: dict,
    outputs: dict,
    fila: int,
    dry_run: bool,
    no_upload: bool = False
) -> bool:
    """Ejecuta un paso del pipeline. Retorna True si tuvo éxito."""
    step_id   = step["id"]
    step_type = step.get("type", "command")
    step_name = step.get("name", step_id)
    captures  = step.get("captures_output", False)

    log("─" * 50)
    log(f"[PASO] {step_name}  (id: {step_id})", "STEP")

    # ── Paso desactivado ──────────────────────────────────────────────────────
    if step.get("disabled", False):
        log(f"[SKIP] Paso desactivado en el JSON (disabled: true)", "WARN")
        return True

    if step_id == "subir-video" and (no_upload or dry_run):
        log(f"[SKIP] Saltando subida a YouTube (no_upload={no_upload}, dry_run={dry_run})", "WARN")
        return True

    # ── command: corre dentro del contenedor ──────────────────────────────
    if step_type == "command":
        raw_cmd = step.get("command", "")
        parts   = raw_cmd.split()
        script  = parts[1] if parts[0] == "python" else parts[0]

        args = list(step.get("args", []))

        # Sobreescribir --fila con el valor real si ya viene en el JSON
        if "--fila" in args:
            args[args.index("--fila") + 1] = str(fila)
        # Inyectar --fila en pasos que lo necesitan aunque no venga en el JSON
        elif step_id in ("generar-guion", "generar-seo", "extraer-metadata", "subir-video"):
            args += ["--fila", str(fila)]

        # Inyectar output del paso anterior si este paso lo consume
        input_from = step.get("input_from")
        if input_from and input_from in outputs:
            input_file = TEMP_PIPELINE / f"{input_from}_output.json"
            save_step_output(input_from, outputs[input_from])
            args += ["--input", to_container_path(input_file)]

        success, stdout = docker_exec(script, args, captures, dry_run)

        if success and captures and stdout.strip():
            outputs[step_id] = stdout.strip()
            save_step_output(step_id, stdout.strip())
            log(f"Output capturado ({len(stdout)} chars)")

        return success

    # ── transform: corre en Windows (host) ────────────────────────────────
    elif step_type == "transform":
        script     = step.get("script", "")
        input_from = step.get("input_from")
        input_data = outputs.get(input_from, "") if input_from else ""

        success, stdout = run_transform(script, input_data, dry_run)

        if success and stdout.strip():
            outputs[step_id] = stdout.strip()
            save_step_output(step_id, stdout.strip())

        return success

    # ── host_command: corre en Windows (host) con ejecutable específico ───────
    elif step_type == "host_command":
        raw_cmd = step.get("command", "")
        # El comando tiene formato: "exe script" (ej: "tts-venv\Scripts\python.exe app/generators/generar_audio.py")
        parts  = raw_cmd.split(None, 1)
        exe    = parts[0]
        script = parts[1] if len(parts) > 1 else ""
        args   = list(step.get("args", []))

        full_cmd = [str(PROJECT_ROOT / exe)] + script.split() + args
        log(f"host: {' '.join(full_cmd)}")

        if dry_run:
            log("[DRY RUN] — host_command no ejecutado", "WARN")
            return True

        result = subprocess.run(
            full_cmd,
            cwd=str(PROJECT_ROOT),
            text=True,
            encoding="utf-8",
            errors="replace"
        )
        if result.returncode != 0:
            log(f"host_command falló (exit code {result.returncode})", "ERR")
            return False
        return True

    # ── http: no soportado fuera de n8n ───────────────────────────────────
    elif step_type == "http":
        log(f"Tipo 'http' no soportado por el runner local.", "ERR")
        log(f"Convierte '{step_id}' a type 'command' en el JSON del workflow.", "ERR")
        return False

    else:
        log(f"Tipo de paso desconocido: '{step_type}'", "ERR")
        return False


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Runner genérico de pipelines — canal-reli",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos:
  python app/pipeline/run_pipeline.py --pipeline workflows/video-pipeline.json --fila 1
  python app/pipeline/run_pipeline.py --pipeline workflows/video-pipeline.json --fila 1 --dry-run
  python app/pipeline/run_pipeline.py --pipeline workflows/video-pipeline.json --fila 2 --desde subir-video
        """
    )
    parser.add_argument(
        "--pipeline",
        type=str,
        default=str(DEFAULT_PIPELINE),
        help="Ruta al archivo JSON del workflow (default: workflows/video-pipeline.json)"
    )
    parser.add_argument(
        "--fila",
        type=int,
        default=1,
        help="Fila del calendario a procesar (default: 1)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simular ejecución sin correr comandos reales"
    )
    parser.add_argument(
        "--no-upload",
        action="store_true",
        help="Evitar el paso de subida a YouTube"
    )
    parser.add_argument(
        "--desde",
        type=str,
        default=None,
        help="Reanudar desde este step_id (omite los pasos anteriores)"
    )
    args = parser.parse_args()

    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    pipeline_path = Path(args.pipeline)
    # Si la ruta es relativa, resolverla desde PROJECT_ROOT
    if not pipeline_path.is_absolute():
        pipeline_path = PROJECT_ROOT / pipeline_path

    print(flush=True)
    print("=" * 55, flush=True)
    print(f"🎬 PIPELINE RUNNER — canal-reli", flush=True)
    print(f"   Workflow: {pipeline_path.name}", flush=True)
    print(f"   Fila:     {args.fila}", flush=True)
    print(f"   Dry run:  {args.dry_run}", flush=True)
    print(f"   No upload: {args.no_upload}", flush=True)
    if args.desde:
        print(f"   Desde:    {args.desde}", flush=True)
    print("=" * 55, flush=True)
    print(flush=True)

    # Verificar Docker (excepto en dry-run)
    if not args.dry_run:
        log("Verificando contenedor Docker...")
        if not check_docker():
            log(f"El contenedor '{CONTAINER}' no está corriendo.", "ERR")
            log("Ejecuta desde C:\\docker:  docker compose up -d", "ERR")
            sys.exit(1)
        log(f"Contenedor '{CONTAINER}' activo", "OK")

    # Cargar y ordenar pipeline
    pipeline      = load_pipeline(pipeline_path)
    steps_ordered = topological_sort(pipeline["steps"])
    log(f"Pipeline: {pipeline['name']} v{pipeline.get('version', '?')} — {len(steps_ordered)} pasos", "OK")

    # Filtrar pasos si se usa --desde
    if args.desde:
        ids = [s["id"] for s in steps_ordered]
        if args.desde not in ids:
            log(f"Step '{args.desde}' no encontrado.", "ERR")
            log(f"IDs disponibles: {ids}", "ERR")
            sys.exit(1)
        steps_ordered = steps_ordered[ids.index(args.desde):]
        log(f"Reanudando desde '{args.desde}' ({len(steps_ordered)} pasos restantes)", "WARN")

    # Ejecutar
    # Si se reanuda desde un paso intermedio, restaurar outputs previos desde disco
    # para que los pasos siguientes reciban correctamente --input
    outputs: dict[str, str] = restore_outputs_from_disk() if args.desde else {}
    start_time:  float          = time.time()
    failed_step: str | None     = None

    for step in steps_ordered:
        step_start = time.time()
        success    = execute_step(step, outputs, args.fila, args.dry_run, args.no_upload)
        elapsed    = time.time() - step_start

        if success:
            log(f"'{step['name']}' completado ({elapsed:.1f}s)", "OK")
        else:
            log(f"'{step['name']}' FALLÓ ({elapsed:.1f}s)", "ERR")
            failed_step = step["id"]
            if pipeline.get("on_failure") == "abort":
                log("Pipeline abortado (on_failure: abort)", "ERR")
                log(
                    f"Reanudar: python app/pipeline/run_pipeline.py "
                    f"--pipeline {args.pipeline} --fila {args.fila} --desde {step['id']}"
                )
                break
        print(flush=True)

    total = time.time() - start_time
    print("=" * 55, flush=True)
    if failed_step:
        print(f"❌ PIPELINE FALLIDO en '{failed_step}' ({total:.1f}s)", flush=True)
        print(
            f"   Reanudar: python app/pipeline/run_pipeline.py "
            f"--pipeline {args.pipeline} --fila {args.fila} --desde {failed_step}",
            flush=True
        )
        sys.exit(1)
    else:
        print(f"✅ PIPELINE COMPLETADO en {total:.1f}s", flush=True)
        print(f"   Fila {args.fila} procesada correctamente.", flush=True)


if __name__ == "__main__":
    main()