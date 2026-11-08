"""D.2 Redis se reinicia a mitad de camino, con dos configuraciones de persistencia.

  aof     : la elegida (docker-compose.yml): AOF + appendfsync everysec, sin RDB, volumen redis_data
  ninguna : la contraria (docker-compose.sin-persistencia.yml): sin AOF y sin RDB

Para cada una: FLUSHALL, se producen 3.000 eventos, los grupos ingesta y tablero los procesan
(dejando 100 entradas "en vuelo" sin XACK en el grupo ingesta), foto del estado,
`docker compose restart redis`, y foto otra vez. Al final se vuelve a la configuración elegida.
"""
import json
import subprocess
import time

from comun import RESULTADOS, STREAM, crear_grupo, hbase_conn, redis_conn
from productor import producir
import ingesta
import tablero

BASE = ["docker", "compose", "-f", "docker-compose.yml"]
CONFIGS = {
    "aof (elegida)": BASE,
    "ninguna (contraria)": BASE + ["-f", "docker-compose.sin-persistencia.yml"],
}


def compose(args, *extra):
    subprocess.run(args + list(extra), check=True, capture_output=True)


def esperar_redis():
    for _ in range(60):
        try:
            r = redis_conn()
            if r.ping():
                while r.info("persistence").get("loading", 0):
                    time.sleep(0.5)
                return r
        except Exception:  # noqa: BLE001
            time.sleep(1)
    raise SystemExit("Redis no volvió")


def foto(r):
    try:
        grupos = {g["name"]: {"pending": g["pending"], "last_delivered": g["last-delivered-id"]}
                  for g in r.xinfo_groups(STREAM)}
    except Exception:  # noqa: BLE001  (el stream no existe)
        grupos = {}
    ventanas = r.keys("ventana:*")
    return {"xlen": r.xlen(STREAM), "grupos": grupos, "ultimo:*": len(r.keys("ultimo:*")),
            "ventana:*": len(ventanas), "entradas_en_ventanas": sum(r.zcard(k) for k in ventanas),
            "ranking:flujo": r.zcard("ranking:flujo"), "tipo:*": len(r.keys("tipo:*")),
            "config": {k: r.config_get(k)[k] for k in ("appendonly", "appendfsync", "save")}}


def experimento(nombre, args):
    print(f"\n### Configuración: {nombre}")
    compose(args, "up", "-d", "--wait", "redis")       # recrea el contenedor con esa config
    r = esperar_redis()
    r.flushall()
    producir(STREAM, n=3000, ritmo=0, r=r, verbose=False)
    tablero.consumir(STREAM, salir_al_terminar=True, verbose=False)
    # ingesta: procesa todo salvo las últimas 100, que quedan entregadas sin XACK ("en vuelo")
    crear_grupo(r, STREAM, "ingesta")
    conn = hbase_conn()
    t1, t2 = conn.table("lecturas"), conn.table("lecturas_tipo")
    restantes = 2900
    while restantes:
        lote = r.xreadgroup("ingesta", "c1", {STREAM: ">"}, count=min(200, restantes))[0][1]
        ingesta.escribir_lote(t1, t2, lote)
        r.xack(STREAM, "ingesta", *[i for i, _ in lote])
        restantes -= len(lote)
    r.xreadgroup("ingesta", "c_en_vuelo", {STREAM: ">"}, count=100)  # leídas, sin put ni XACK
    time.sleep(1.5)  # > 1 s: deja pasar al menos un fsync de everysec
    antes = foto(r)
    print("  antes  :", antes)
    compose(args, "restart", "redis")
    r = esperar_redis()
    despues = foto(r)
    print("  después:", despues)
    return {"antes": antes, "despues": despues}


if __name__ == "__main__":
    res = {nombre: experimento(nombre, args) for nombre, args in CONFIGS.items()}
    print("\nVolviendo a la configuración elegida (AOF)...")
    compose(BASE, "up", "-d", "--wait", "redis")
    esperar_redis().flushall()
    (RESULTADOS / "d2.json").write_text(json.dumps(res, indent=2, ensure_ascii=False))
    print("\nResumen (antes -> después del restart):")
    for nombre, d in res.items():
        a, b = d["antes"], d["despues"]
        print(f"  {nombre:20s} stream {a['xlen']}->{b['xlen']} | grupos {list(a['grupos'])}->{list(b['grupos'])}"
              f" | pendientes ingesta {a['grupos'].get('ingesta', {}).get('pending')}->"
              f"{b['grupos'].get('ingesta', {}).get('pending')} | ultimo:* {a['ultimo:*']}->{b['ultimo:*']}"
              f" | ventanas {a['ventana:*']}->{b['ventana:*']}")
