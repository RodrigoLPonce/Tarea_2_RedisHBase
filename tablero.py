"""B.2 Consumidor `tablero`: mantiene en Redis el estado "de ahora".

Claves que deja (revisables con redis-cli):
  ultimo:<estacion>  hash {valor, ts (medición, ms), tipo}
  ranking:flujo      zset estacion -> último valor (solo flujo vehicular)
  ventana:<estacion> zset member "ts_ms:valor", score = ts de medición (ms), solo flujo,
                     recortado a los últimos 10 min de medición
  tipo:<tipo>        set con las estaciones de cada tipo (catálogo)
  tablero:max_ts     medición más reciente vista en el stream (ms), ancla de la ventana

"Último" = último por MEDICIÓN. Cada evento se aplica con un script Lua atómico que
compara el ts de medición nuevo con el guardado y solo escribe si no retrocede. Al ser
atómico y evaluado evento por evento en el servidor, una lectura tardía dentro del mismo
lote tampoco puede pisar a una más nueva (no hay "leer y después escribir" desde Python).
"""
import argparse

from comun import STREAM, crear_grupo, grupo_terminado, redis_conn, ts_ms

GRUPO = "tablero"
VENTANA_MS = 10 * 60 * 1000

# KEYS: ultimo, ranking, ventana, tipo, max_ts   ARGV: estacion, tipo, valor, ts_ms
LUA = """
local est, tipo, valor, ts = ARGV[1], ARGV[2], ARGV[3], tonumber(ARGV[4])
redis.call('SADD', KEYS[4], est)
local maxts = tonumber(redis.call('GET', KEYS[5]) or '0')
if ts > maxts then maxts = ts; redis.call('SET', KEYS[5], ARGV[4]) end
local prev = redis.call('HGET', KEYS[1], 'ts')
local actualizado = 0
if (not prev) or ts >= tonumber(prev) then
  redis.call('HSET', KEYS[1], 'valor', valor, 'ts', ARGV[4], 'tipo', tipo)
  if tipo == 'flujo_vehicular' then redis.call('ZADD', KEYS[2], valor, est) end
  actualizado = 1
end
if tipo == 'flujo_vehicular' then
  redis.call('ZADD', KEYS[3], ARGV[4], ARGV[4] .. ':' .. valor)
  redis.call('ZREMRANGEBYSCORE', KEYS[3], '-inf', '(' .. (maxts - %d))
end
return actualizado
""" % VENTANA_MS


def aplicar_lote(r, script, entradas):
    """Aplica un lote al tablero (pipeline de EVALSHA). Devuelve cuántos 'ultimo' cambiaron."""
    p = r.pipeline(transaction=False)
    for _, ev in entradas:
        est, tipo = ev["estacion_id"], ev["tipo_sensor"]
        script(keys=[f"ultimo:{est}", "ranking:flujo", f"ventana:{est}", f"tipo:{tipo}",
                     "tablero:max_ts"],
               args=[est, tipo, ev["valor"], ts_ms(ev["timestamp"])], client=p)
    return sum(p.execute())


def consumir(stream=STREAM, grupo=GRUPO, consumidor="c1", count=200, salir_al_terminar=True,
             verbose=True):
    r = redis_conn()
    crear_grupo(r, stream, grupo)
    script = r.register_script(LUA)
    procesadas = ignoradas = 0
    desde = "0"
    if verbose:
        print(f"tablero/{consumidor}: esperando eventos en {stream}...", flush=True)
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
        cambios = aplicar_lote(r, script, entradas)
        r.xack(stream, grupo, *[i for i, _ in entradas])
        procesadas += len(entradas)
        ignoradas += len(entradas) - cambios
    if verbose:
        print(f"tablero/{consumidor}: fin, {procesadas} entradas; {ignoradas} no movieron "
              f"ultimo:* (tardías o más viejas que lo ya guardado).")
    return procesadas


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--stream", default=STREAM)
    ap.add_argument("--consumidor", default="c1")
    ap.add_argument("--sin-fin", action="store_true")
    a = ap.parse_args()
    consumir(a.stream, consumidor=a.consumidor, salir_al_terminar=not a.sin_fin)
