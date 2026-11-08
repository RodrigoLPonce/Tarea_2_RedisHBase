"""B.2 Productor: escribe la hora en vivo en el stream con XADD a 20x.

    python productor.py                    # 7.200 eventos, 1 cada 25 ms, stream sensores:t2
    python productor.py --n 3000 --ritmo 0 # usado por los experimentos de la Parte D
"""
import argparse
import time

from comun import MAXLEN, N_VIVO, RITMO_VIVO, STREAM, en_vivo, llegada_iso, redis_conn


def clave_fin(stream):
    return f"productor:fin:{stream}"


def producir(stream=STREAM, n=N_VIVO, ritmo=RITMO_VIVO, r=None, verbose=True):
    r = r or redis_conn()
    r.delete(clave_fin(stream))
    inicio = time.perf_counter()
    for k, (i, ev) in enumerate(en_vivo(n)):
        # 'llegada' = reloj de llegada simulado (el del generador). El stream ID también
        # ordena por llegada; este campo sirve para comparar "último por llegada" en HBase.
        r.xadd(stream, {**ev, "llegada": llegada_iso(i)}, maxlen=MAXLEN, approximate=True)
        if ritmo:
            time.sleep(max(0.0, inicio + (k + 1) * ritmo - time.perf_counter()))
        if verbose and (k + 1) % 1000 == 0:
            print(f"Producidos {k + 1}/{n} eventos...", flush=True)
    r.set(clave_fin(stream), n)  # señal para que los consumidores terminen
    seg = time.perf_counter() - inicio
    if verbose:
        print(f"Producción finalizada: {n} eventos en {seg:.1f} s ({n / seg:.0f} ev/s)")
    return n


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--stream", default=STREAM)
    ap.add_argument("--n", type=int, default=N_VIVO)
    ap.add_argument("--ritmo", type=float, default=RITMO_VIVO)
    a = ap.parse_args()
    producir(a.stream, a.n, a.ritmo)
