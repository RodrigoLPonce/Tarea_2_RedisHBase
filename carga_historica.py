"""B.1 Carga histórica: 864.000 eventos directo a HBase (sin pasar por Redis), con batch.

Recrea las tablas `lecturas` y `lecturas_tipo` pre-divididas (desde cero en cada corrida).
"""
import json
import time

from comun import (RESULTADOS, SPLITS_LECTURAS, SPLITS_TIPO, columnas, hbase_conn,
                   hbase_shell, historico, llegada_iso, rk_lecturas, rk_tipo)

TABLAS = ["lecturas", "lecturas_tipo"]


def crear_tablas():
    conn = hbase_conn()
    existentes = {t.decode() for t in conn.tables()}
    cmds = []
    for t in TABLAS:
        if t in existentes:
            cmds += [f"disable '{t}'", f"drop '{t}'"]
    # VERSIONS => 3 para poder mostrar en D.1 qué quedó en la celda al reprocesar
    cmds.append(f"create 'lecturas', {{NAME => 'm', VERSIONS => 3}}, SPLITS => {SPLITS_LECTURAS}")
    cmds.append(f"create 'lecturas_tipo', {{NAME => 'm', VERSIONS => 3}}, SPLITS => {SPLITS_TIPO}")
    salida = hbase_shell("\n".join(cmds))
    conn = hbase_conn()
    faltan = [t for t in TABLAS if t.encode() not in conn.tables()]
    if faltan:
        raise SystemExit(f"No se pudieron crear {faltan}:\n{salida}")
    for t in TABLAS:
        regiones = [r["start_key"].decode() or "(inicio)" for r in conn.table(t).regions()]
        print(f"Tabla {t}: {len(regiones)} regiones, inicios = {regiones}")


def cargar():
    crear_tablas()
    conn = hbase_conn()
    t1, t2 = conn.table("lecturas"), conn.table("lecturas_tipo")
    n = 0
    inicio = time.perf_counter()
    with t1.batch(batch_size=5000) as b1, t2.batch(batch_size=5000) as b2:
        for i, ev in historico():
            cols = columnas(ev, llegada_iso(i))
            b1.put(rk_lecturas(ev), cols)
            b2.put(rk_tipo(ev), cols)
            n += 1
            if n % 100_000 == 0:
                print(f"  {n:,} eventos...", flush=True)
    seg = time.perf_counter() - inicio
    res = {"eventos": n, "segundos": round(seg, 1), "eventos_por_s": round(n / seg),
           "puts_por_s": round(2 * n / seg)}
    print(f"Carga histórica: {n:,} eventos (x2 tablas) en {seg:.1f} s "
          f"= {n / seg:,.0f} eventos/s ({2 * n / seg:,.0f} puts/s)")
    (RESULTADOS / "carga_historica.json").write_text(json.dumps(res, indent=2))


if __name__ == "__main__":
    cargar()
