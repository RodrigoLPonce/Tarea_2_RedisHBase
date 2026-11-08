"""P5: hotspotting y salting. La hora en vivo en 3 tablas con la misma información y distinta clave.

  p5_est  : estacion#ts          pre-dividida por estación (4 regiones de 4 estaciones)
  p5_ts   : ts#estacion          pre-dividida por día
  p5_salt : salt#ts#estacion     salt = crc32(estacion) mod 4, pre-dividida por salt

Escrituras por región: métricas JMX del RegionServer (writeRequestCount por región).
Lectura: todas las lecturas de todas las estaciones entre 00:10 y 00:20 del 07-nov
(16 rangos / 1 rango / 4 rangos).
"""
import json
import re
import time
import urllib.request

from comun import (ESTACIONES, RESULTADOS, SPLITS_LECTURAS, en_vivo, hbase_conn, hbase_shell,
                   maquina, medir, salt)

JMX = "http://localhost:16010/jmx?qry=Hadoop:service=HBase,name=RegionServer,sub=Regions"
TABLAS = {
    "p5_est": SPLITS_LECTURAS,
    "p5_ts": [f"2026-11-0{d}" for d in range(3, 9)],
    "p5_salt": ["1", "2", "3"],
}
INI, FIN = "2026-11-07T00:10", "2026-11-07T00:20"


def preparar():
    conn = hbase_conn()
    existentes = {t.decode() for t in conn.tables()}
    cmds = []
    for t, splits in TABLAS.items():
        if t in existentes:
            cmds += [f"disable '{t}'", f"drop '{t}'"]
        cmds.append(f"create '{t}', 'm', SPLITS => {splits}")
    hbase_shell("\n".join(cmds))


def regiones(tabla):
    conn = hbase_conn()
    out = []
    for reg in conn.table(tabla).regions():
        cifrado = reg["name"].decode().rstrip(".").rsplit(".", 1)[1]
        out.append((reg["start_key"].decode() or "(inicio)", cifrado))
    return out


def escrituras_por_region():
    beans = json.load(urllib.request.urlopen(JMX, timeout=10))["beans"]
    metricas = beans[0] if beans else {}
    res = {}
    for t in TABLAS:
        res[t] = {}
        for inicio, cifrado in regiones(t):
            patron = re.compile(rf"_table_{t}_region_{cifrado}_metric_writeRequestCount$")
            res[t][inicio] = next((v for k, v in metricas.items() if patron.search(k)), 0)
    return res


def inyectar():
    conn = hbase_conn()
    tb = {t: conn.table(t) for t in TABLAS}
    n = 0
    t0 = time.perf_counter()
    with tb["p5_est"].batch(batch_size=1000) as b1, tb["p5_ts"].batch(batch_size=1000) as b2, \
            tb["p5_salt"].batch(batch_size=1000) as b3:
        for _, ev in en_vivo():
            est, ts = ev["estacion_id"], ev["timestamp"]
            cols = {b"m:valor": str(ev["valor"]).encode()}
            b1.put(f"{est}#{ts}".encode(), cols)
            b2.put(f"{ts}#{est}".encode(), cols)
            b3.put(f"{salt(est)}#{ts}#{est}".encode(), cols)
            n += 1
    return n, time.perf_counter() - t0


_CONN = {}


def _tabla(nombre):
    """Una sola conexión reutilizada: así se mide la consulta y no el connect."""
    if "c" not in _CONN:
        _CONN["c"] = hbase_conn()
    return _CONN["c"].table(nombre)


def leer_est():
    t = _tabla("p5_est")
    return sum(1 for est in ESTACIONES for _ in
               t.scan(row_start=f"{est}#{INI}".encode(), row_stop=f"{est}#{FIN}".encode()))


def leer_ts():
    t = _tabla("p5_ts")
    return sum(1 for _ in t.scan(row_start=INI.encode(), row_stop=FIN.encode()))


def leer_salt():
    t = _tabla("p5_salt")
    return sum(1 for s in range(4) for _ in
               t.scan(row_start=f"{s}#{INI}".encode(), row_stop=f"{s}#{FIN}".encode()))


if __name__ == "__main__":
    print("Creando p5_est, p5_ts, p5_salt...")
    preparar()
    print("Salt por estación:", {e: salt(e) for e in ESTACIONES})
    n, seg = inyectar()
    print(f"Escritos {n} eventos x 3 tablas en {seg:.1f} s")

    # JMX publica cada ~10 s: esperar hasta que las métricas reflejen todas las escrituras.
    # Se lee hasta que los totales dejan de cambiar entre dos lecturas (y no son 0).
    previo, res = None, {}
    for intento in range(12):
        time.sleep(10)
        res = escrituras_por_region()
        totales = {t: sum(v.values()) for t, v in res.items()}
        print(f"  JMX intento {intento + 1}: totales = {totales}")
        if totales == previo and all(totales.values()):
            break
        previo = totales

    print("\n--- Escrituras por región (writeRequestCount, JMX) ---")
    for t, regs in res.items():
        total = sum(regs.values()) or 1
        print(f"{t}:")
        for inicio, v in regs.items():
            print(f"   región que empieza en {inicio:12s}: {v:6d} ({100 * v / total:5.1f} %)")

    print(f"\n--- Leer todas las estaciones entre {INI} y {FIN} ---")
    tiempos = {}
    for nombre, f, rangos in [("estacion#ts", leer_est, 16), ("ts#estacion", leer_ts, 1),
                              ("salt#ts#estacion", leer_salt, 4)]:
        filas, t, _ = medir(f)
        tiempos[nombre] = {"rangos": rangos, "filas": filas, "t_s": t}
        print(f"  {nombre:18s} {rangos:2d} rangos: {filas} filas, mediana {t * 1000:.2f} ms")

    (RESULTADOS / "p5.json").write_text(json.dumps(
        {"maquina": maquina(), "salt": {e: salt(e) for e in ESTACIONES}, "escrituras": res,
         "lecturas": tiempos}, indent=2, ensure_ascii=False))
