"""D.1 (parte 1): la ingesta muere a mitad de lote, después del put y antes del XACK.

1. Borra de HBase las filas de la hora en vivo (vuelven al estado "solo histórico": 846.906).
2. Escribe la hora en vivo en un stream propio (sensores:t2:d1) sin esperar el ritmo.
3. Lanza `ingesta.py --fallar-en-lote 3` como proceso aparte: hace put de 3 lotes de 200,
   confirma 2 y muere con os._exit(1) antes del XACK del tercero.
4. Muestra qué quedó en XPENDING. La recuperación está en d1_rescate.py.
"""
import json
import subprocess
import sys
from pathlib import Path

from comun import (RESULTADOS, contar_filas, en_vivo, hbase_conn, redis_conn, rk_lecturas,
                   rk_tipo)
from productor import producir

STREAM_D1, GRUPO = "sensores:t2:d1", "ingesta"


def borrar_hora_en_vivo():
    conn = hbase_conn()
    t1, t2 = conn.table("lecturas"), conn.table("lecturas_tipo")
    with t1.batch(batch_size=2000) as b1, t2.batch(batch_size=2000) as b2:
        for _, ev in en_vivo():
            b1.delete(rk_lecturas(ev))
            b2.delete(rk_tipo(ev))
    return contar_filas(t1), contar_filas(t2)


if __name__ == "__main__":
    r = redis_conn()
    f1, f2 = borrar_hora_en_vivo()
    print(f"1) Filas tras borrar la hora en vivo: lecturas={f1:,} lecturas_tipo={f2:,} (esperado 846.906)")

    r.delete(STREAM_D1)
    producir(STREAM_D1, ritmo=0, r=r, verbose=False)
    print(f"2) Stream {STREAM_D1}: {r.xlen(STREAM_D1)} entradas")

    print("3) Lanzando ingesta con --fallar-en-lote 3 ...")
    proc = subprocess.run([sys.executable, str(Path(__file__).with_name("ingesta.py")),
                           "--stream", STREAM_D1, "--grupo", GRUPO, "--consumidor", "c1",
                           "--count", "200", "--fallar-en-lote", "3"],
                          capture_output=True, text=True, encoding="utf-8", errors="replace")
    print("   " + proc.stdout.strip().replace("\n", "\n   "))
    print(f"   código de salida del proceso: {proc.returncode} (1 = murió con os._exit(1))")

    resumen = r.xpending(STREAM_D1, GRUPO)
    detalle = r.xpending_range(STREAM_D1, GRUPO, min="-", max="+", count=1000)
    print(f"4) XPENDING: {resumen['pending']} entradas; por consumidor: {resumen['consumers']}")
    print(f"   rango de IDs pendientes: {resumen['min']} .. {resumen['max']}")
    for d in detalle[:3]:
        print(f"   {d['message_id']}  consumidor={d['consumer']}  entregas={d['times_delivered']}")
    f1b = contar_filas(hbase_conn().table("lecturas"))
    print(f"   Filas en lecturas tras la caída: {f1b:,} (= 846.906 + claves distintas de 600 eventos)")
    (RESULTADOS / "d1_falla.json").write_text(json.dumps(
        {"filas_tras_borrar": [f1, f2], "codigo_salida": proc.returncode,
         "xpending": {"pending": resumen["pending"], "consumers": resumen["consumers"],
                      "min": resumen["min"], "max": resumen["max"]},
         "filas_tras_caida": f1b}, indent=2))
