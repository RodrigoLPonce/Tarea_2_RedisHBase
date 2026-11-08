"""Corre la tarea de punta a punta (con los contenedores arriba). Lo llaman run.sh / run.ps1.

  0. comprueba Redis y HBase
  1. carga histórica -> HBase (recrea las tablas)
  2. D.2: reinicio de Redis con las dos configuraciones de persistencia
  3. hora en vivo: productor (20x) + consumidores ingesta y tablero en paralelo
  4. verificación contra los números de control
  5. consultas P1-P4 y P6
  6. P5 hotspotting / salting
  7. D.1: la ingesta muere entre put y XACK, y se rescata
  8. verificación final

La salida de cada paso queda en resultados/NN_paso.txt (y los números en resultados/*.json).

  python run_all.py               # todo
  python run_all.py --sin-carga   # reutiliza el histórico ya cargado (desarrollo)
  python run_all.py --sin-fallas  # omite D.1 y D.2
"""
import argparse
import os
import subprocess
import sys
import time

from comun import RAIZ, RESULTADOS, STREAM, hbase_conn, redis_conn

ENV = {**os.environ, "PYTHONIOENCODING": "utf-8", "PYTHONUNBUFFERED": "1"}
TIEMPOS = []


def paso(n, nombre, script, *args):
    print(f"\n{'=' * 70}\n[{n}] {nombre}\n{'=' * 70}", flush=True)
    t0 = time.perf_counter()
    archivo = RESULTADOS / f"{n:02d}_{script.replace('.py', '')}.txt"
    with open(archivo, "w", encoding="utf-8") as log:
        proc = subprocess.Popen([sys.executable, script, *args], cwd=RAIZ, env=ENV,
                                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                text=True, encoding="utf-8", errors="replace")
        for linea in proc.stdout:
            print(linea, end="", flush=True)
            log.write(linea)
        proc.wait()
    seg = time.perf_counter() - t0
    TIEMPOS.append((nombre, seg))
    if proc.returncode != 0:
        raise SystemExit(f"El paso '{nombre}' falló (código {proc.returncode}). Ver {archivo}")
    print(f"--> {nombre}: {seg:.1f} s")


def hora_en_vivo(n):
    print(f"\n{'=' * 70}\n[{n}] Hora en vivo: productor 20x + ingesta + tablero\n{'=' * 70}", flush=True)
    r = redis_conn()
    r.flushall()                               # Redis desde cero (HBase conserva el histórico)
    t0 = time.perf_counter()
    archivo = RESULTADOS / f"{n:02d}_hora_en_vivo.txt"
    with open(archivo, "w", encoding="utf-8") as log:
        consumidores = [subprocess.Popen([sys.executable, s], cwd=RAIZ, env=ENV, stdout=log,
                                         stderr=subprocess.STDOUT) for s in ("ingesta.py", "tablero.py")]
        time.sleep(2)                          # que los grupos existan antes del primer XADD
        prod = subprocess.run([sys.executable, "productor.py"], cwd=RAIZ, env=ENV,
                              capture_output=True, text=True, encoding="utf-8", errors="replace")
        print(prod.stdout, end="")
        log.write(prod.stdout + prod.stderr)
        if prod.returncode != 0:
            raise SystemExit(f"productor falló:\n{prod.stderr}")
        for c in consumidores:
            try:
                c.wait(timeout=300)
            except subprocess.TimeoutExpired:
                c.kill()
                raise SystemExit("Un consumidor no terminó en 5 min. Ver " + str(archivo))
            if c.returncode != 0:
                raise SystemExit("Un consumidor falló. Ver " + str(archivo))
    print(open(archivo, encoding="utf-8", errors="replace").read()[-600:])
    seg = time.perf_counter() - t0
    TIEMPOS.append(("Hora en vivo", seg))
    print(f"--> Hora en vivo: {seg:.1f} s (stream {STREAM}: {r.xlen(STREAM)} entradas)")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sin-carga", action="store_true")
    ap.add_argument("--sin-fallas", action="store_true")
    a = ap.parse_args()
    inicio = time.perf_counter()

    print("[0] Comprobando servicios...")
    redis_conn().ping()
    print("    Redis OK; tablas HBase:", [t.decode() for t in hbase_conn().tables()])

    if not a.sin_carga:
        paso(1, "Carga histórica", "carga_historica.py")
    if not a.sin_fallas:
        paso(2, "D.2 Reinicio de Redis (AOF vs sin persistencia)", "d2_reinicio.py")
    hora_en_vivo(3)
    paso(4, "Verificación", "verificar.py")
    paso(5, "Consultas P1-P4, P6", "consultas.py")
    paso(6, "P5 Hotspotting y salting", "p5_hotspotting.py")
    if not a.sin_fallas:
        paso(7, "D.1 Falla de la ingesta entre put y XACK", "d1_falla.py")
        paso(8, "D.1 Rescate con XAUTOCLAIM", "d1_rescate.py")
        paso(9, "Verificación final", "verificar.py")

    total = time.perf_counter() - inicio
    resumen = "\n".join(f"  {n:50s} {s:8.1f} s" for n, s in TIEMPOS)
    texto = f"Tiempos por paso:\n{resumen}\n  {'TOTAL':50s} {total:8.1f} s ({total / 60:.1f} min)\n"
    print("\n" + texto)
    (RESULTADOS / "00_tiempos.txt").write_text(texto, encoding="utf-8")


if __name__ == "__main__":
    main()
