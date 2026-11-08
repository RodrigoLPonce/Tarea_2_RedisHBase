"""B.3 Verificación: compara lo que quedó en Redis y HBase con los números de control.

Imprime: eventos producidos, entradas procesadas y XPENDING por grupo, filas en cada tabla,
y si algo no calza lo dice y lo explica.
"""
import json
import time

from comun import (CONTROL, N_VIVO, RESULTADOS, STREAM, contar_filas, eventos_con_llegada,
                   hbase_conn, redis_conn)


def control_desde_generador():
    """Recalcula los números de control con la semilla (≈3 s), para no confiar a ciegas."""
    vistos, claves = set(), set()
    for _, ev in eventos_con_llegada(0, 864_000 + N_VIVO):
        vistos.add(tuple(ev.values()))
        claves.add((ev["estacion_id"], ev["timestamp"]))
    return {"eventos": 864_000 + N_VIVO, "repetidos": 864_000 + N_VIVO - len(vistos),
            "claves": len(claves), "colisiones_misma_clave": len(vistos) - len(claves)}


def verificar():
    ok = True
    r = redis_conn()
    res = {"control_recalculado": control_desde_generador()}
    print("Números de control recalculados desde el generador:", res["control_recalculado"])

    xlen = r.xlen(STREAM)
    res["producidos"] = int(r.get(f"productor:fin:{STREAM}") or 0)
    res["xlen"] = xlen
    print(f"\nEventos producidos: {res['producidos']} (esperado {N_VIVO}) · XLEN {STREAM} = {xlen}")
    if res["producidos"] != N_VIVO:
        ok = False
        print("  !! El productor no terminó la hora en vivo.")

    res["grupos"] = {}
    for g in r.xinfo_groups(STREAM):
        info = {"entries_read": g.get("entries-read"), "pending": g["pending"], "lag": g.get("lag")}
        res["grupos"][g["name"]] = info
        estado = "OK" if g["pending"] == 0 and g.get("lag") == 0 else "!!"
        print(f"  grupo {g['name']:8s}: procesadas {info['entries_read']}, XPENDING {g['pending']}, "
              f"lag {info['lag']}  [{estado}]")
        if estado != "OK":
            ok = False
            print("    -> hay entradas sin confirmar o sin leer: un consumidor murió o no terminó.")

    tablero = {"ultimo:*": len(r.keys("ultimo:*")), "ventana:*": len(r.keys("ventana:*")),
               "ranking:flujo": r.zcard("ranking:flujo"), "tipo:*": len(r.keys("tipo:*"))}
    res["tablero"] = tablero
    print(f"\nTablero en Redis: {tablero} (esperado 16 / 8 / 8 / 3)")

    print("\nContando filas en HBase (FirstKeyOnlyFilter AND KeyOnlyFilter)...")
    conn = hbase_conn()
    esperado = CONTROL["total"]["claves"]
    for t in ["lecturas", "lecturas_tipo"]:
        t0 = time.perf_counter()
        n = contar_filas(conn.table(t))
        res[f"filas_{t}"] = n
        estado = "OK" if n == esperado else "!!"
        print(f"  {t:14s}: {n:,} filas (esperado {esperado:,}) [{estado}]  ({time.perf_counter() - t0:.1f} s)")
        if n != esperado:
            ok = False
            if n < esperado:
                print(f"    -> faltan {esperado - n:,}: ¿carga histórica incompleta o la ingesta no terminó?")
            else:
                print(f"    -> sobran {n - esperado:,}: la clave no es idempotente (¿usa la llegada?).")
    c = CONTROL["total"]
    print(f"\nPor qué {c['eventos']:,} eventos dan {c['claves']:,} filas: {c['repetidos']:,} son repetidos "
          f"tal cual (misma clave, mismo valor -> se sobrescribe la misma celda) y "
          f"{res['control_recalculado']['colisiones_misma_clave']} pares tienen misma estación y mismo ms "
          f"con distinto valor (HBase se queda con el último escrito).")
    res["ok"] = ok
    print("\nRESULTADO:", "TODO CALZA" if ok else "HAY DIFERENCIAS (ver arriba)")
    (RESULTADOS / "verificar.json").write_text(json.dumps(res, indent=2))
    return ok


if __name__ == "__main__":
    t0 = time.time()
    verificar()
    print(f"Tiempo de verificación: {time.time() - t0:.1f} s")
