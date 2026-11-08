# Tarea 2 — Redis + HBase, del stream a la historia

Pipeline completo: la historia (864.000 eventos) va directo a HBase; la hora en vivo (7.200 eventos)
entra a un stream de Redis y dos grupos de consumidores la reparten: `ingesta` la baja a HBase y
`tablero` mantiene el estado "de ahora" en Redis. Las preguntas se responden desde el motor que
corresponde, midiendo cuánto cuesta ponerlas en el equivocado.

## Cómo reproducir desde cero

Requisitos: Docker con Docker Compose v2 (~4 GB de RAM asignados) y Python 3.9+.
Con Docker abierto, ejecutar **un solo comando** según el sistema operativo:

| Sistema | Comando |
|---|---|
| Linux / macOS | `./run.sh` |
| Windows (PowerShell) | `.\run.ps1` |
| Windows (doble clic) | `run.cmd` |

Los tres hacen lo mismo: `docker compose up -d --build --wait` (Redis 7 + HBase 2.5.15 del Lab 07,
ambos con healthcheck), crean el `venv`, instalan `requirements.txt` y ejecutan `run_all.py`, que corre
en orden: carga histórica → D.2 → hora en vivo (productor 20× + `ingesta` + `tablero` en paralelo)
→ verificación → consultas P1–P4 y P6 → P5 → D.1 → verificación final.
La salida de cada paso queda en `resultados/NN_paso.txt` y los números en `resultados/*.json`.

**Cuánto tarda:** ~8 min en el PC de prueba (detalle en `resultados/00_tiempos.txt`): carga histórica
~1 min, hora en vivo ~3 min a 20×. La primera vez hay que sumar el `docker compose build` de HBase (~340 MB).

Opciones: `--sin-carga` reutiliza el histórico que ya está en el volumen; `--sin-fallas` omite la Parte D.
Los scripts también se pueden correr sueltos (`python consultas.py`, etc.) con los contenedores arriba.

| Archivo | Qué hace |
|---|---|
| `run.sh` | comando único en Linux/macOS |
| `run.ps1` | comando único en Windows (PowerShell); guarda el log en `resultados/run_log.txt` |
| `run.cmd` | lanza `run.ps1` con doble clic en Windows |
| `run_all.py` | orquestador: lo llaman los tres anteriores y corre todos los pasos en orden |
| `comun.py` | parámetros de la tarea (semilla 2026, t0, splits, MAXLEN), row keys, conexiones, medición |
| `generador.py` | generador del Lab 06 (sin cambios; se llama con `semilla=2026`) |
| `carga_historica.py` | B.1: crea las tablas pre-divididas y carga el histórico con `batch` |
| `productor.py` | B.2: `XADD` de la hora en vivo a `sensores:t2`, 1 evento cada 25 ms |
| `ingesta.py` | B.2: grupo `ingesta` → HBase (put, después XACK) |
| `tablero.py` | B.2: grupo `tablero` → `ultimo:*`, `ranking:flujo`, `ventana:*`, `tipo:*` |
| `verificar.py` | B.3: compara contra los números de control y explica diferencias |
| `consultas.py` | P1, P2, P3, P4, P6 |
| `p5_hotspotting.py` | P5 |
| `d1_falla.py`, `d1_rescate.py` | D.1 |
| `d2_reinicio.py`, `docker-compose.sin-persistencia.yml` | D.2 |

## Decisiones

### Row keys (HBase)

| Tabla | Row key | Rápida | Lenta |
|---|---|---|---|
| `lecturas` | `estacion#timestamp_medicion` | lecturas de una estación en un rango de tiempo (un scan contiguo) | todo lo que cruza estaciones: "todas las estaciones a las 08:00", "todas las de un tipo" (un rango por estación o la tabla entera) |
| `lecturas_tipo` | `tipo#timestamp_medicion#estacion` | lecturas de un tipo en un rango de tiempo (P4b, un solo scan) | una estación concreta (hay que leer todo su tipo en ese rango y filtrar) |

El timestamp es ISO 8601 con milisegundos y el mismo offset `-03:00` (`2026-11-07T00:59:59.500-03:00`):
como ancho fijo, ordena igual como bytes que como tiempo. **Idempotencia:** la clave sale de la
estación y de la medición, nunca de la llegada ni de un contador. Un evento repetido produce la misma
clave y sobrescribe la misma celda; uno tardío cae en su lugar cronológico. Por eso 871.200 eventos dan
853.953 filas: 17.243 repetidos tal cual + 4 pares con la misma estación y el mismo ms y distinto valor
(HBase se queda con el último escrito). Cada fila guarda `m:valor`, `m:tipo` y `m:llegada` (el reloj
de llegada simulado, solo para poder comparar "último por llegada" en P1). La familia `m` tiene
`VERSIONS => 3` para mostrar en D.1 qué queda en la celda al reprocesar.

### Regiones (SPLITS)

* `lecturas`: `['CC-005', 'HUA-001', 'SPP-002']` → 4 regiones de 4 estaciones cada una. El generador
  elige la estación de forma uniforme, así que cada región recibe ~25 % de las filas.
* `lecturas_tipo`: `['flujo_vehicular', 'flujo_vehicular#2026-11-04T12', 'ocupacion_estacionamiento']`
  → aire (4 estaciones) | flujo, primera mitad | flujo, segunda mitad | ocupación (4). Flujo tiene 8
  estaciones (el doble de volumen), por eso se corta a la mitad del tiempo. ~25 % por región.

### Stream y MAXLEN

`XADD sensores:t2 MAXLEN ~ 10000`. A 20× (40 ev/s) son ~250 s de atraso tolerado; a ritmo real
(2 ev/s), ~83 minutos. Cabe la hora en vivo completa, así que en la tarea no se recorta nada. Si un
consumidor se atrasa más que eso, `XADD` recorta entradas que su grupo nunca leyó: desaparecen del
stream sin error (`XAUTOCLAIM` las devuelve como "borradas") y, como la única copia durable es HBase,
esa entrada se pierde si nadie la bajó. El tamaño se elige por la caída de consumidor más larga que
se quiere aguantar, contra la RAM que cuesta (~150 B por entrada).

### "Último" por medición en el tablero

`tablero.py` aplica cada evento con un script Lua atómico: compara el `ts` de medición nuevo con el de
`ultimo:<est>` y solo escribe (y actualiza `ranking:flujo`) si no retrocede. Como la comparación ocurre
dentro de Redis evento por evento, una lectura tardía no puede hacer retroceder el tablero, ni siquiera
si viene en el mismo lote que una más nueva. En empate de ms gana la última llegada (igual que HBase).
`ventana:<est>` usa score = ts de medición y member `ts:valor` (un repetido tal cual no cuenta dos veces);
en cada escritura se borra lo anterior a `tablero:max_ts − 10 min`.

### Persistencia de Redis: AOF, `appendfsync everysec`, sin RDB

Decisión en `docker-compose.yml`. Redis guarda el stream y el estado de los grupos (qué se leyó y qué
quedó pendiente): si eso se pierde, se pierden eventos que todavía no llegaron a HBase. Con AOF +
`everysec` la pérdida máxima ante un corte es ~1 s de escrituras, a un costo de rendimiento bajo;
`always` (fsync por comando) es más seguro pero mucho más lento, y RDB pierde todo lo posterior al
último snapshot (minutos). Se desactiva RDB (`--save ""`) para que el experimento D.2 mida solo AOF.
Se pone a prueba en D.2 contra la contraria (sin persistencia).

## Qué no funcionó / lo que cambiamos en el camino

* La primera versión del README pedía abrir 3 terminales: no era reproducible con un comando. Se
  agregó `run_all.py` (y `run.sh` / `run.ps1`) y los consumidores terminan solos cuando el productor
  terminó y su grupo leyó y confirmó todo.
* El tablero leía `ultimo:*` desde Python y escribía en un pipeline: dentro de un mismo lote, una
  tardía podía pisar a una más nueva. Se reemplazó por el script Lua atómico.
* `hash()` de Python cambia entre procesos (`PYTHONHASHSEED`): el salt de P5 no era reproducible. Se usa `crc32`.
* `scan(row_prefix=..., reverse=True)` no es confiable entre versiones de happybase (puede devolver la
  última fila de la estación anterior). Se usan límites explícitos `EST#~` → `EST#`.
* `docker compose restart` hace un SHUTDOWN limpio, y la imagen `redis:7` sin archivo de configuración
  trae RDB activo por defecto: ese SHUTDOWN guardaría un `dump.rdb` y la configuración "sin persistencia"
  sobreviviría al reinicio, falseando D.2. Por eso las dos configuraciones llevan `--save ""` explícito
  (y la contraria `--dir /tmp`, para no cargar un dump viejo que haya quedado en el volumen).
* La primera P6 ("¿hace cuánto no se veía un valor así?") daba siempre "hace minutos": el generador es
  un proceso que vuelve a su nivel base, así que casi cualquier valor se repite pronto. Se cambió por
  el percentil del valor actual frente a la misma hora de los días anteriores.
* P5 esperaba que JMX contara 7.200 escrituras, pero `writeRequestCount` contó 7.048 (las filas
  repetidas dentro de un mismo batch se combinan), así que esperaba los 2 minutos completos. Ahora
  espera a que los contadores dejen de cambiar.
