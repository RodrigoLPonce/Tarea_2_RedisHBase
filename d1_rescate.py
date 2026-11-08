"""D.1 (parte 2): otro consumidor recupera las entradas pendientes y termina el trabajo.

1. XPENDING: cuántas entradas y a nombre de quién.
2. XAUTOCLAIM: el consumidor c2 se apropia de ellas, las reescribe en HBase y hace XACK.
3. c2 sigue con el resto del stream (el mismo código de ingesta.py).
4. Conteo final de filas (debe ser 853.953 en ambas tablas) y versiones de una celda
   reprocesada: reprocesar no crea filas nuevas, agrega una versión a la misma celda.
"""
import json

from comun import RESULTADOS, CONTROL, contar_filas, hbase_conn, redis_conn, rk_lecturas
from d1_falla import GRUPO, STREAM_D1
from ingesta import consumir, escribir_lote

if __name__ == "__main__":
    r = redis_conn()
    conn = hbase_conn()
    t1, t2 = conn.table("lecturas"), conn.table("lecturas_tipo")

    resumen = r.xpending(STREAM_D1, GRUPO)
    print(f"1) XPENDING antes del rescate: {resumen['pending']} entradas, {resumen['consumers']}")

    resp = r.xautoclaim(STREAM_D1, GRUPO, "c2", min_idle_time=0, start_id="0-0", count=1000)
    reclamadas, borradas = resp[1], (resp[2] if len(resp) > 2 else [])
    print(f"2) XAUTOCLAIM: c2 reclamó {len(reclamadas)} entradas (IDs ya recortados del stream: "
          f"{len(borradas)})")
    escribir_lote(t1, t2, reclamadas)                         # put ...
    r.xack(STREAM_D1, GRUPO, *[i for i, _ in reclamadas])     # ... y recién ahí XACK
    print(f"   reprocesadas y confirmadas. XPENDING ahora: {r.xpending(STREAM_D1, GRUPO)['pending']}")

    n = consumir(STREAM_D1, GRUPO, "c2", verbose=False)
    print(f"3) c2 procesó el resto del stream: {n} entradas más. "
          f"XPENDING final: {r.xpending(STREAM_D1, GRUPO)['pending']}")

    f1, f2 = contar_filas(t1), contar_filas(t2)
    esperado = CONTROL["total"]["claves"]
    print(f"4) Filas finales: lecturas={f1:,} lecturas_tipo={f2:,} (esperado {esperado:,}) -> "
          f"{'OK, ni faltan ni sobran' if f1 == f2 == esperado else 'NO CALZA'}")

    # Versiones de una celda del lote reprocesado
    ej_id, ej = reclamadas[0]
    fila = rk_lecturas(ej)
    versiones = t1.cells(fila, b"m:valor", versions=3, include_timestamp=True)
    print(f"   Celda {fila.decode()} m:valor (VERSIONS=3):")
    for valor, ts in versiones:
        print(f"     valor={valor.decode()}  timestamp de escritura={ts}")
    print("   -> misma fila, mismo valor: el reproceso agregó una versión, no una fila.")

    (RESULTADOS / "d1_rescate.json").write_text(json.dumps(
        {"pending_antes": resumen["pending"], "consumers_antes": resumen["consumers"],
         "reclamadas": len(reclamadas), "resto_procesado_por_c2": n,
         "filas_finales": [f1, f2], "celda_ejemplo": fila.decode(),
         "versiones": [[v.decode(), ts] for v, ts in versiones]}, indent=2))
    r.delete(STREAM_D1, "productor:fin:" + STREAM_D1)
