"""Parte C: preguntas P1, P2, P3, P4 y P6 (P5 está en p5_hotspotting.py).

Cada consulta se mide con la mediana de 5 corridas (más una corrida previa descartada,
que calienta cachés). Resultados en resultados/consultas.json.
"""
import json
import subprocess
from collections import defaultdict
from datetime import datetime, timedelta

from comun import (ESTACIONES, RESULTADOS, STREAM, hbase_conn, hbase_shell, maquina, medir,
                   ms_iso, redis_conn, ts_ms)

r = redis_conn()
conn = hbase_conn()
lecturas = conn.table("lecturas")
lecturas_tipo = conn.table("lecturas_tipo")
OUT = {"maquina": maquina()}


def fmt(seg):
    return f"{seg * 1000:.2f} ms"


# ---------------------------------------------------------------- P1
def p1_redis():
    p = r.pipeline(transaction=False)
    for est in ESTACIONES:
        p.hgetall(f"ultimo:{est}")
    p.zrevrange("ranking:flujo", 0, 4, withscores=True)
    res = p.execute()
    ultimos = {e: {"valor": float(h["valor"]), "ts": ms_iso(int(h["ts"]))}
               for e, h in zip(ESTACIONES, res[:-1])}
    return ultimos, [(e, v) for e, v in res[-1]]


def ultima_fila(est):
    """Última fila por medición de una estación: scan inverso con límites explícitos
    ('EST#~' es mayor que cualquier 'EST#2026-...'), limit=1. No depende de cómo cada
    versión de happybase trate row_prefix + reverse."""
    for k, v in lecturas.scan(row_start=f"{est}#~".encode(), row_stop=f"{est}#".encode(),
                              reverse=True, limit=1):
        return k, v
    return None, None


def p1_hbase():
    ultimos = {}
    for est in ESTACIONES:
        k, v = ultima_fila(est)
        ultimos[est] = {"valor": float(v[b"m:valor"]), "ts": k.decode().split("#", 1)[1],
                        "tipo": v[b"m:tipo"].decode()}
    top5 = sorted(((e, u["valor"]) for e, u in ultimos.items() if u["tipo"] == "flujo_vehicular"),
                  key=lambda x: -x[1])[:5]
    return {e: {"valor": u["valor"], "ts": u["ts"]} for e, u in ultimos.items()}, top5


def p1_por_llegada_redis():
    """Último por llegada = la última entrada del stream de cada estación (XREVRANGE)."""
    vistos = {}
    for _, ev in r.xrevrange(STREAM, "+", "-"):
        if ev["estacion_id"] not in vistos:
            vistos[ev["estacion_id"]] = {"valor": float(ev["valor"]), "ts": ev["timestamp"],
                                         "llegada": ev["llegada"]}
            if len(vistos) == len(ESTACIONES):
                break
    return vistos


def p1_por_llegada_hbase(est):
    """En HBase: entre las filas de los últimos ~3 min de medición, la de mayor m:llegada
    (una tardía llega a lo más 2 min después de medida)."""
    k, _ = ultima_fila(est)
    ult_ms = ts_ms(k.decode().split("#", 1)[1])
    desde = f"{est}#{ms_iso(ult_ms - 180_000)}".encode()
    mejor = max(lecturas.scan(row_start=desde, row_stop=f"{est}#~".encode()),
                key=lambda kv: kv[1][b"m:llegada"])
    return {"valor": float(mejor[1][b"m:valor"]), "ts": mejor[0].decode().split("#", 1)[1],
            "llegada": mejor[1][b"m:llegada"].decode()}


def p1():
    (u_r, top_r), t_r, _ = medir(p1_redis)
    (u_h, top_h), t_h, _ = medir(p1_hbase)
    llegada, t_ll, _ = medir(p1_por_llegada_redis)
    distintas = []
    for est in ESTACIONES:
        if llegada[est]["ts"] != u_r[est]["ts"]:
            distintas.append({"estacion": est, "por_medicion": u_r[est], "por_llegada": llegada[est],
                              "por_llegada_hbase": p1_por_llegada_hbase(est)})
    coinciden = all(abs(u_r[e]["valor"] - u_h[e]["valor"]) < 1e-9 for e in ESTACIONES)
    OUT["P1"] = {"ultimos_redis": u_r, "top5_redis": top_r, "top5_hbase": top_h,
                 "redis_y_hbase_coinciden": coinciden, "t_redis_s": t_r, "t_hbase_s": t_h,
                 "veces_mas_lento_hbase": t_h / t_r, "t_ultimo_por_llegada_redis_s": t_ll,
                 "estaciones_medicion_vs_llegada": distintas}
    print("\n=== P1: el 'ahora' ===")
    for e in ESTACIONES:
        print(f"  {e:8s} {u_r[e]['valor']:6.1f}  ts={u_r[e]['ts']}")
    print(f"  Top-5 flujo (Redis): {top_r}")
    print(f"  Top-5 flujo (HBase): {top_h}")
    print(f"  Redis y HBase coinciden en las 16: {coinciden}")
    print(f"  Tiempo Redis {fmt(t_r)} | HBase {fmt(t_h)} | HBase {t_h / t_r:.0f}x más lento")
    for d in distintas:
        print(f"  Cambia según el reloj: {d['estacion']}\n    por medición: {d['por_medicion']}"
              f"\n    por llegada : {d['por_llegada']}\n    (HBase, por m:llegada: {d['por_llegada_hbase']})")


# ---------------------------------------------------------------- P2
def p2_redis():
    ancla = int(r.get("tablero:max_ts"))
    desde = ancla - 600_000
    estaciones = sorted(r.smembers("tipo:flujo_vehicular"))
    p = r.pipeline(transaction=False)
    for est in estaciones:
        p.zrangebyscore(f"ventana:{est}", desde, ancla)
    res = {}
    for est, miembros in zip(estaciones, p.execute()):
        vals = [float(m.split(":", 1)[1]) for m in miembros]
        res[est] = {"n": len(vals), "promedio": round(sum(vals) / len(vals), 2) if vals else None}
    return ancla, res


def p2():
    (ancla, prom), t, _ = medir(p2_redis)
    mem = {est: r.memory_usage(f"ventana:{est}") for est in prom}
    # Sin recorte: misma estructura con TODAS las lecturas de flujo de la hora en vivo
    est_ej = "CC-001"
    tmp = "tmp:ventana_sin_recorte:" + est_ej
    r.delete(tmp)
    for _, ev in r.xrange(STREAM, "-", "+"):
        if ev["estacion_id"] == est_ej:
            m = ts_ms(ev["timestamp"])
            r.zadd(tmp, {f"{m}:{ev['valor']}": m})
    n_sin, mem_sin = r.zcard(tmp), r.memory_usage(tmp)
    r.delete(tmp)
    n_con = r.zcard(f"ventana:{est_ej}")
    bytes_por_entrada = mem_sin / n_sin
    por_dia = 2 * 86_400 / 16  # ~2 ev/s repartidos en 16 estaciones -> lecturas/día/estación
    OUT["P2"] = {"ancla": ms_iso(ancla), "promedios": prom, "t_s": t, "memory_usage_bytes": mem,
                 "ejemplo": {"estacion": est_ej, "entradas_recortada": n_con,
                             "bytes_recortada": mem[est_ej], "entradas_1h_sin_recorte": n_sin,
                             "bytes_1h_sin_recorte": mem_sin,
                             "bytes_por_entrada": round(bytes_por_entrada, 1),
                             "proyeccion_1_anio_MB_por_estacion":
                                 round(bytes_por_entrada * por_dia * 365 / 1e6, 1)}}
    print("\n=== P2: ventana de 10 minutos (Redis) ===")
    print(f"  Ancla (medición más reciente del stream): {ms_iso(ancla)}")
    for est, v in prom.items():
        print(f"  {est:8s} n={v['n']:3d} promedio={v['promedio']}  MEMORY USAGE={mem[est]} B")
    print(f"  Tiempo {fmt(t)}")
    print(f"  {est_ej}: recortada {n_con} entradas / {mem[est_ej]} B; sin recortar (1 h) {n_sin} "
          f"entradas / {mem_sin} B -> {OUT['P2']['ejemplo']['proyeccion_1_anio_MB_por_estacion']} MB "
          f"por estación en un año si nunca se recorta")


# ---------------------------------------------------------------- P3
INI3, FIN3 = b"CC-001#2026-11-04T08:00", b"CC-001#2026-11-04T10:00"


def por_cuarto(filas):
    grupos = defaultdict(list)
    for k, v in filas:
        dt = datetime.fromisoformat(k.decode().split("#", 1)[1])
        grupos[f"{dt.hour:02d}:{dt.minute // 15 * 15:02d}"].append(float(v[b"m:valor"]))
    return {c: {"n": len(x), "promedio": round(sum(x) / len(x), 2)} for c, x in sorted(grupos.items())}


def p3_rango():
    filas = list(lecturas.scan(row_start=INI3, row_stop=FIN3))
    return len(filas), por_cuarto(filas)


def p3_tabla_entera():
    # Recorre TODA la tabla; el filtro (en el servidor) solo decide qué filas devolver.
    filtro = b"RowFilter(=, 'regexstring:^CC-001#2026-11-04T0[89]:')"
    filas = [(k, v) for k, v in lecturas.scan(filter=filtro, batch_size=5000) if INI3 <= k < FIN3]
    return len(filas), por_cuarto(filas)


def p3():
    (n1, c1), t1, _ = medir(p3_rango)
    (n2, c2), t2, _ = medir(p3_tabla_entera)
    OUT["P3"] = {"n": n1, "n_tabla_entera": n2, "cuartos": c1, "iguales": c1 == c2,
                 "t_rango_s": t1, "t_tabla_entera_s": t2, "veces_mas_lento": t2 / t1}
    print("\n=== P3: CC-001, 04-nov [08:00, 10:00) ===")
    print(f"  {n1} lecturas (tabla entera + filtro: {n2}); promedio por cuarto de hora:")
    for c, v in c1.items():
        print(f"    {c}  n={v['n']:3d}  promedio={v['promedio']}")
    print(f"  Scan por rango {fmt(t1)} | tabla entera + filtro {fmt(t2)} | {t2 / t1:.0f}x más lento")


# ---------------------------------------------------------------- P4
def resumen_pm25(filas_por_est):
    out = {}
    for est, filas in sorted(filas_por_est.items()):
        vals = [v for _, v in filas]
        mx = max(vals)
        primera = next(ts for ts, v in filas if v == mx)  # filas en orden de medición
        out[est] = {"n": len(vals), "promedio": round(sum(vals) / len(vals), 2), "maximo": mx,
                    "hora_maximo": primera}
    return out


def p4_a():
    filas = {}
    for est in r.smembers("tipo:calidad_aire"):             # el QUÉ sale del catálogo de Redis
        filas[est] = [(k.decode().split("#", 1)[1], float(v[b"m:valor"])) for k, v in
                      lecturas.scan(row_start=f"{est}#2026-11-05T07:00".encode(),
                                    row_stop=f"{est}#2026-11-05T09:00".encode())]
    return resumen_pm25(filas)


def p4_b():
    filas = defaultdict(list)
    for k, v in lecturas_tipo.scan(row_start=b"calidad_aire#2026-11-05T07:00",
                                   row_stop=b"calidad_aire#2026-11-05T09:00"):
        _, ts, est = k.decode().split("#")
        filas[est].append((ts, float(v[b"m:valor"])))
    return resumen_pm25(filas)


def tamano_tablas():
    hbase_shell("flush 'lecturas'\nflush 'lecturas_tipo'")
    out = subprocess.run(["docker", "compose", "exec", "-T", "hbase", "sh", "-c",
                          "du -sk /data/hbase/data/default/lecturas /data/hbase/data/default/lecturas_tipo"],
                         capture_output=True, text=True)
    return {linea.split()[1].rsplit("/", 1)[1]: int(linea.split()[0]) for linea in out.stdout.splitlines()
            if linea.strip()}


def p4():
    a, ta, _ = medir(p4_a)
    b, tb, _ = medir(p4_b)
    try:
        disco = tamano_tablas()
    except Exception as e:  # noqa: BLE001
        disco = {"error": str(e)}
    OUT["P4"] = {"a_lecturas": a, "b_lecturas_tipo": b, "iguales": a == b, "t_a_s": ta, "t_b_s": tb,
                 "disco_kb": disco}
    print("\n=== P4: PM2.5, 05-nov [07:00, 09:00) ===")
    for est, v in a.items():
        print(f"  {est:8s} n={v['n']:3d} promedio={v['promedio']:6.2f} máx={v['maximo']:5.1f} "
              f"a las {v['hora_maximo']}")
    print(f"  a) lecturas + catálogo Redis {fmt(ta)} | b) lecturas_tipo (1 scan) {fmt(tb)} | "
          f"resultados iguales: {a == b}")
    print(f"  Espacio en disco (KB, tras flush): {disco}")


# ---------------------------------------------------------------- P6
DIAS_HIST = [f"2026-11-0{d}" for d in range(2, 7)]  # los 5 días del histórico


def p6_consulta():
    """¿Qué tan raro es el "ahora" de cada estación comparado con la MISMA HORA de los días anteriores?
    Redis decide el QUÉ y el CUÁNDO de la pregunta: qué estaciones existen (catálogo tipo:*),
    su último valor por medición y a qué hora del día fue (ultimo:*).
    HBase aporta la referencia histórica: un scan por rango por estación y por día, solo de esa
    hora (estacion#AAAA-MM-DDTHH -> estacion#AAAA-MM-DDTHH+1). Resultado: percentil del valor
    actual dentro de la historia de esa estación a esa hora."""
    estaciones = sorted(set().union(*[r.smembers(f"tipo:{t}") for t in
                                      ("flujo_vehicular", "calidad_aire", "ocupacion_estacionamiento")]))
    p = r.pipeline(transaction=False)
    for est in estaciones:
        p.hgetall(f"ultimo:{est}")
    out = {}
    for est, h in zip(estaciones, p.execute()):
        val = float(h["valor"])
        dt = datetime.fromisoformat(ms_iso(int(h["ts"])))
        hh = f"T{dt.hour:02d}"
        hist = []
        for dia in DIAS_HIST:
            fin = datetime.fromisoformat(f"{dia}{hh}:00:00-03:00") + timedelta(hours=1)
            hist += [float(v[b"m:valor"]) for _, v in lecturas.scan(
                row_start=f"{est}#{dia}{hh}".encode(),
                row_stop=f"{est}#{fin.isoformat(timespec='milliseconds')[:13]}".encode(),
                columns=[b"m:valor"], batch_size=500)]
        hist.sort()
        pct = 100 * sum(1 for x in hist if x <= val) / len(hist)
        out[est] = {"tipo": h["tipo"], "valor_ahora": val, "hora": f"{dt.hour:02d}:00-{(dt.hour + 1) % 24:02d}:00",
                    "n_hist": len(hist), "media_hist": round(sum(hist) / len(hist), 2),
                    "p10_hist": hist[int(0.1 * len(hist))], "p90_hist": hist[int(0.9 * len(hist))],
                    "percentil_ahora": round(pct, 1),
                    "fuera_de_lo_normal": pct >= 90 or pct <= 10}
    return out


def p6():
    res, t, _ = medir(p6_consulta)
    OUT["P6"] = {"resultado": res, "t_s": t}
    print("\n=== P6: ¿qué tan raro es el 'ahora' frente a la misma hora de los 5 días anteriores? ===")
    for est, v in sorted(res.items(), key=lambda x: -x[1]["percentil_ahora"]):
        marca = "  <-- fuera de lo normal" if v["fuera_de_lo_normal"] else ""
        print(f"  {est:8s} {v['tipo']:26s} ahora={v['valor_ahora']:6.1f}  histórico {v['hora']}: "
              f"media={v['media_hist']:6.2f} p10={v['p10_hist']:5.1f} p90={v['p90_hist']:5.1f} "
              f"(n={v['n_hist']}) -> percentil {v['percentil_ahora']:5.1f}{marca}")
    print(f"  Tiempo {fmt(t)} (16 lecturas en Redis + 80 scans por rango en HBase)")


if __name__ == "__main__":
    p1()
    p2()
    p3()
    p4()
    p6()
    (RESULTADOS / "consultas.json").write_text(json.dumps(OUT, indent=2, ensure_ascii=False))
    print("\nResultados guardados en resultados/consultas.json")
