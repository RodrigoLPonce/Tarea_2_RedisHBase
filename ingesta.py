"""B.2 Consumidor `ingesta`: lee el stream en lotes y lo baja a HBase (las dos tablas).

Orden estricto: put en HBase -> XACK. Si el proceso muere entre ambos, las entradas
quedan en XPENDING y se reprocesan; como la row key sale de la medición, reprocesar
sobrescribe la misma celda (idempotente) en vez de duplicar filas.

    python ingesta.py                          # consumidor c1 del grupo ingesta
    python ingesta.py --fallar-en-lote 3       # D.1: muere tras el put del lote 3, antes del XACK
"""
import argparse
import os

from comun import (STREAM, columnas, crear_grupo, grupo_terminado, hbase_conn, rk_lecturas,
                   rk_tipo, redis_conn)

GRUPO = "ingesta"


def escribir_lote(t_lecturas, t_tipo, entradas):
    with t_lecturas.batch() as b1, t_tipo.batch() as b2:
        for _, ev in entradas:
            cols = columnas(ev, ev.get("llegada", ""))
            b1.put(rk_lecturas(ev), cols)
            b2.put(rk_tipo(ev), cols)


def consumir(stream=STREAM, grupo=GRUPO, consumidor="c1", count=200, fallar_en_lote=None,
             salir_al_terminar=True, verbose=True):
    r = redis_conn()
    crear_grupo(r, stream, grupo)
    conn = hbase_conn()
    t_lecturas, t_tipo = conn.table("lecturas"), conn.table("lecturas_tipo")
    lotes = procesadas = 0
    desde = "0"  # primero lo que quedó pendiente a mi nombre (si me reiniciaron), luego ">"
    if verbose:
        print(f"ingesta/{consumidor}: esperando eventos en {stream}...", flush=True)
    while True:
        resp = r.xreadgroup(grupo, consumidor, {stream: desde}, count=count, block=1000)
        entradas = resp[0][1] if resp else []
        if desde == "0" and not entradas:
            desde = ">"
            continue
        if not entradas:
            if salir_al_terminar and grupo_terminado(r, stream, grupo):
                break
            continue
        escribir_lote(t_lecturas, t_tipo, entradas)           # 1) put
        lotes += 1
        if fallar_en_lote and lotes == fallar_en_lote:
            print(f"ingesta/{consumidor}: put del lote {lotes} hecho ({len(entradas)} entradas, "
                  f"{entradas[0][0]} .. {entradas[-1][0]}). MUERO ANTES DEL XACK.", flush=True)
            os._exit(1)                                       # D.1: muerte abrupta
        r.xack(stream, grupo, *[i for i, _ in entradas])      # 2) XACK, siempre después
        procesadas += len(entradas)
    if verbose:
        print(f"ingesta/{consumidor}: fin, {procesadas} entradas en {lotes} lotes.")
    return procesadas


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--stream", default=STREAM)
    ap.add_argument("--grupo", default=GRUPO)
    ap.add_argument("--consumidor", default="c1")
    ap.add_argument("--count", type=int, default=200)
    ap.add_argument("--fallar-en-lote", type=int, default=None)
    ap.add_argument("--sin-fin", action="store_true", help="no terminar al acabar el stream")
    a = ap.parse_args()
    consumir(a.stream, a.grupo, a.consumidor, a.count, a.fallar_en_lote, not a.sin_fin)
