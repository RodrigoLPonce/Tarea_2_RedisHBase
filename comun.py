"""Constantes y utilidades compartidas por todo el pipeline de la Tarea 2.

Todo lo que es "parámetro de la tarea" vive aquí, para que ningún script tenga
rutas, hosts o semillas repetidas a mano.
"""
import itertools
import os
import statistics
import subprocess
import time
import zlib
from datetime import datetime, timedelta, timezone
from pathlib import Path

from generador import ESTACIONES as _ESTACIONES, PASO, eventos

# --- Conexiones (se pueden cambiar por variables de entorno, sin editar código) ---
REDIS_HOST = os.environ.get("REDIS_HOST", "127.0.0.1")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))
HBASE_HOST = os.environ.get("HBASE_HOST", "127.0.0.1")
HBASE_PORT = int(os.environ.get("HBASE_PORT", "9090"))

# --- Parámetros de la tarea ---
SEMILLA = 2026
CHILE = timezone(timedelta(hours=-3))
T0 = datetime(2026, 11, 2, 0, 0, tzinfo=CHILE)
N_HIST = 864_000            # 5 días x 172.800
N_VIVO = 7_200              # 1 hora
RITMO_VIVO = 0.025          # 20x: un evento cada 25 ms
STREAM = "sensores:t2"
MAXLEN = 10_000             # ver README: ~250 s de atraso tolerado a 20x (~83 min a ritmo real)

ESTACIONES = [e for e, _ in _ESTACIONES]
TIPO_DE = dict(_ESTACIONES)

# Números de control del enunciado
CONTROL = {
    "hist": {"eventos": 864_000, "repetidos": 17_090, "claves": 846_906},
    "vivo": {"eventos": 7_200, "repetidos": 153, "claves": 7_047},
    "total": {"eventos": 871_200, "repetidos": 17_243, "claves": 853_953},
}

# --- Tablas HBase y sus cortes (SPLITS) ---
# lecturas: 16 estaciones, cada una recibe ~1/16 de los eventos (rng.choice uniforme).
#   Cortes que dejan 4 regiones con 4 estaciones cada una.
SPLITS_LECTURAS = ["CC-005", "HUA-001", "SPP-002"]
# lecturas_tipo: 4 estaciones de aire, 8 de flujo, 4 de ocupación -> flujo pesa el doble,
#   así que su rango se corta por la mitad del tiempo. 4 regiones de ~1/4 del volumen.
SPLITS_TIPO = ["flujo_vehicular", "flujo_vehicular#2026-11-04T12", "ocupacion_estacionamiento"]

RAIZ = Path(__file__).resolve().parent
os.chdir(RAIZ)  # docker compose y los archivos de resultados se resuelven desde el repo
RESULTADOS = RAIZ / "resultados"
RESULTADOS.mkdir(exist_ok=True)


# --- Eventos ---
def llegada_iso(i):
    """Reloj de llegada del evento i (el generador avanza PASO s por evento)."""
    return (T0 + timedelta(seconds=i * PASO)).isoformat(timespec="milliseconds")


def eventos_con_llegada(desde, hasta):
    """Eventos [desde, hasta) con su índice global (para calcular la llegada)."""
    gen = itertools.islice(eventos(T0, semilla=SEMILLA), desde, hasta)
    return ((desde + k, ev) for k, ev in enumerate(gen))


def historico():
    return eventos_con_llegada(0, N_HIST)


def en_vivo(n=N_VIVO):
    return eventos_con_llegada(N_HIST, N_HIST + n)


def ts_ms(iso):
    return int(datetime.fromisoformat(iso).timestamp() * 1000)


def ms_iso(ms):
    return datetime.fromtimestamp(ms / 1000, CHILE).isoformat(timespec="milliseconds")


# --- Row keys (salen de la estación y de la MEDICIÓN, nunca de la llegada) ---
def rk_lecturas(ev):
    return f"{ev['estacion_id']}#{ev['timestamp']}".encode()


def rk_tipo(ev):
    return f"{ev['tipo_sensor']}#{ev['timestamp']}#{ev['estacion_id']}".encode()


def salt(estacion):
    """Salt estable entre procesos (hash() de Python cambia en cada ejecución)."""
    return zlib.crc32(estacion.encode()) % 4


def columnas(ev, llegada):
    return {b"m:valor": str(ev["valor"]).encode(),
            b"m:tipo": ev["tipo_sensor"].encode(),
            b"m:llegada": llegada.encode()}


# --- Conexiones ---
def redis_conn():
    import redis
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


def hbase_conn():
    import happybase
    return happybase.Connection(HBASE_HOST, HBASE_PORT, timeout=120_000)


def hbase_shell(comandos):
    """Ejecuta comandos en el shell de HBase del contenedor (docker compose)."""
    res = subprocess.run(["docker", "compose", "exec", "-T", "hbase", "hbase", "shell"],
                         input=comandos + "\nexit\n", capture_output=True, text=True,
                         encoding="utf-8", errors="replace")
    return res.stdout + res.stderr


def contar_filas(tabla):
    return sum(1 for _ in tabla.scan(filter=b"FirstKeyOnlyFilter() AND KeyOnlyFilter()",
                                     batch_size=10_000))


# --- Consumer groups ---
def crear_grupo(r, stream, grupo):
    import redis
    try:
        r.xgroup_create(stream, grupo, id="0", mkstream=True)
    except redis.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise


def grupo_terminado(r, stream, grupo):
    """True si el productor terminó y el grupo leyó y confirmó todo el stream."""
    if not r.exists(f"productor:fin:{stream}"):
        return False
    ultimo = r.xinfo_stream(stream)["last-generated-id"]
    for g in r.xinfo_groups(stream):
        if g["name"] == grupo:
            return g["last-delivered-id"] == ultimo and g["pending"] == 0
    return False


# --- Medición ---
def medir(func, *args, corridas=5):
    """Mediana de 5 corridas descartando la primera (calienta cachés)."""
    tiempos, res = [], None
    for _ in range(corridas + 1):
        t = time.perf_counter()
        res = func(*args)
        tiempos.append(time.perf_counter() - t)
    return res, statistics.median(tiempos[1:]), tiempos[0]


def maquina():
    import platform
    return {"sistema": platform.platform(), "procesador": platform.processor(),
            "cpus_logicas": os.cpu_count(), "python": platform.python_version()}
