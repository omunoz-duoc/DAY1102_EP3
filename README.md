# Pipeline de Datos – Caso TNR

**Prueba de concepto funcional · Evaluación Parcial EP5**
**Asignatura:** DAY1102 – Administración y Almacenamiento de Datos
**Año:** 2025

> Implementación de la arquitectura de datos de TechNova Retail 360: un pipeline de ingesta por fuente que conserva la entrada en Bronze, publica tablas con contrato en Silver y construye marts Gold para dos dashboards, más un carril *speed* para los eventos web y los movimientos de inventario.

---

## Tabla de contenidos

1. [Descripción del proyecto](#1-descripción-del-proyecto)
2. [Contexto y problema abordado](#2-contexto-y-problema-abordado)
3. [Patrón de proceso de datos implementado](#3-patrón-de-proceso-de-datos-implementado)
4. [Arquitectura del pipeline](#4-arquitectura-del-pipeline)
5. [Tecnologías utilizadas](#5-tecnologías-utilizadas)
6. [Estructura del repositorio](#6-estructura-del-repositorio)
7. [Requisitos e instalación](#7-requisitos-e-instalación)
8. [Ejecución](#8-ejecución)
9. [Etapas del pipeline](#9-etapas-del-pipeline)
10. [Controles de calidad de datos](#10-controles-de-calidad-de-datos)
11. [Salida funcional para el negocio](#11-salida-funcional-para-el-negocio)
12. [Evaluación de resultados](#12-evaluación-de-resultados)
13. [Limitaciones](#13-limitaciones)
14. [Recomendaciones de mejora](#14-recomendaciones-de-mejora)
15. [Equipo](#15-equipo)

---

## 1. Descripción del proyecto

Este repositorio contiene la tercera entrega integradora del caso semestral. Es un paquete Python (`src/tnr`) con una CLI que:

- Ingiere **diez fuentes** de forma independiente: SQL, CSV, XLSX, JSON, JSON Lines, una API REST paginada y un receptor HTTP de eventos.
- Archiva cada entrada original en **Bronze** con huella SHA-256, `run_id` y manifiesto.
- Tipa, valida, deduplica y pone en cuarentena en staging, y publica **Silver** en SQLite con transacciones atómicas e idempotencia.
- Resuelve identidad de clientes (RUT, correo y teléfono) con un mapa estable `id_crm → customer_id`, y publica solo `customer_id` en los marts.
- Construye cinco marts **Gold**: ventas, Cliente 360, conciliación, checkout por minuto y stock por ubicación.
- Expone una capa **Serving** donde lo cerrado (Gold) tiene precedencia sobre lo provisional (Silver).
- Exporta snapshots **Parquet** versionados (PyArrow) y sirve **dos dashboards** en Plotly Dash.

## 2. Contexto y problema abordado

- **Organización / caso:** TechNova Retail S.A. (TNR), retail omnicanal de tecnología con 18 tiendas, e-commerce y un CD.
- **Problema de negocio:** la "Crisis del 140%". Marketing reportaba cerca del 140% de las ventas que Finanzas validaba, por clientes y transacciones duplicadas en un *Data Swamp* sin control. Además faltaba una vista unificada del cliente y una alerta oportuna de fallas de checkout y quiebres de stock.
- **Preguntas de negocio que responde la solución:**
  1. ¿Cuánto vendió realmente TNR en 2025 y cómo se concilia esa cifra desde el dato crudo, pasando por duplicados, rechazos, estados y notas de crédito?
  2. ¿Quién es cada cliente (Cliente 360), con qué frecuencia, recencia, monto y canal compra, y qué experiencia de postventa tiene?
  3. ¿Está fallando el checkout web ahora mismo, en qué dispositivo y a cuántas sesiones afecta?
  4. ¿Qué SKU de qué ubicación está bajo su umbral de reposición?

### 2.1 Datos de origen (`data/`)

Simulación de **TechNova Retail (TNR)** sobre datos docentes de 2025. Los atributos y fuentes nuevas son sintéticos y conservan anomalías intencionales para el perfilado. Los CSV usan UTF-8 y separador `;`, y los montos están en CLP enteros. El detalle de carga y los casos verificables está en [`data/LEEME.md`](data/LEEME.md).

| Archivo | Formato | Tamaño | Pipeline (`python -m tnr ingest …`) |
|---------|---------|-------:|-----------|
| `ventas_db.sql` | SQL (SQLite) | 37.971 filas | `ventas --input sql` |
| `ventas_2025_tnr_ampliadas.csv` | CSV | 37.971 filas | `ventas --input csv`, entrada equivalente a la anterior |
| `productos_tnr_ampliados.xlsx` | Excel (hoja `productos`) | 45 SKU | `productos` |
| `clientes_crm.csv` | CSV | 2.530 filas | `clientes` |
| `despachos.csv` | CSV | 60 filas | `despachos` |
| `notas_credito.csv` | CSV | 47 filas | `notas_credito` |
| `inventario_inicial.csv` | CSV | 10 filas | `inventario` |
| `movimientos_inventario.jsonl` | JSON Lines | 36 recepciones | `movimientos_inventario` o `replay movimientos` |
| `logs_web_tnr.json` | JSON anidado | 5.000 eventos | `web_historico` |
| `eventos_web_stream.jsonl` | JSON Lines | 78 recepciones | `serve web` + `replay web` (`ingest web_stream` recupera desde Bronze) |
| `postventa_pagina_{1..3}.json` + `postventa_mock.py` | API REST paginada | 30 órdenes | `postventa`, cliente HTTP contra `TNR_POSTVENTA_URL` |
| `referencia/dataset_tnr_original.xlsx` | Excel | — | Referencia docente para la conciliación |

## 3. Patrón de proceso de datos implementado

Arquitectura **medallion con carril batch y carril speed** (estilo Lambda):

| Capa | Propósito | Implementación |
|------|-----------|----------------|
| **Bronze** | Entrada original inmutable: archivos, lotes, páginas HTTP y recepciones | `data/bronze/<fuente>/<fecha>/<run_id>/` con `_manifest.json`; las fuentes continuas usan JSONL *append-only* con `fsync` |
| **Silver** | Registros tipados, validados y deduplicados con llaves comunes; un único escritor por relación | Tablas `silver_*` en `warehouse.sqlite`; snapshot Parquet por lote |
| **Gold** | Resultados cerrados por corte o ventana, versionados y trazables a sus `run_id` | Tablas `gold_mart_*` + `gold_publicaciones`; Parquet por publicación |
| **Serving** | Interfaz estable para lo provisional o cerrado de una misma ventana | Vistas `serving_checkout_minuto`, `vw_checkout_provisional`, `vw_stock_provisional` |

**Justificación:** Bronze permite reconstruir cualquier cifra (`--reproceso`) y auditarla hasta el dato recibido. Silver aísla las reglas de calidad por fuente. Gold publica de forma atómica por corte, y Serving deja mostrar datos en tiempo casi real sin perder la cifra cerrada.

## 4. Arquitectura del pipeline

```text
Fuente → Bronze → Silver → Gold → Dashboard
Eventos web → Bronze → Silver → Serving provisional → Dashboard
```

```text
 SQL/CSV/XLSX/JSON ─┐                         ┌─ gold_mart_ventas ───────────┐
 API postventa ─────┼─▶ Bronze ─▶ staging ─▶ Silver ─┼─ gold_mart_cliente_360 ──────┼─▶ Dashboard comercial
 (lotes, run_id)    │   (SHA-256)  (rechazos,   (SQLite) └─ gold_mart_conciliacion ─────┘
                    │              exclusiones,
 POST /eventos ─────┤              revisión)        ┌─ vw_checkout_provisional ─┐
 consumidor movs ───┘   Bronze JSONL ─▶ Silver ─────┤                           ├─ Serving ─▶ Dashboard operaciones
                        (id_recepcion)      cierre ─▶ gold_mart_checkout_minuto ─┘   (cada 3 s)
                                            desde Bronze  gold_mart_stock_ubicacion
```

## 5. Tecnologías utilizadas

| Herramienta | Uso en el proyecto |
|-------------|--------------------|
| **Python 3.10+** (probado en 3.13) | Lenguaje del pipeline; paquete `tnr` instalable con `pip install -e .` |
| **Pandas + openpyxl** | Lectura de XLSX y consultas de los dashboards |
| **SQLite + SQLAlchemy** | Silver, Gold, Serving y control; WAL, `busy_timeout`, `foreign_keys` y transacciones cortas |
| **PyArrow** | Snapshots Parquet versionados de Silver y Gold |
| **Flask** | Receptor HTTP `POST /eventos` |
| **requests** | Cliente de la API de postventa (reintentos acotados) y productor de eventos |
| **Plotly Dash** | Dashboards comercial y de operaciones |
| **tzdata / zoneinfo** | Zona `America/Santiago` y resolución de horas de cambio de horario |
| **pytest** | Pruebas de aceptación de las evidencias verificables |

## 6. Estructura del repositorio

```text
├── pyproject.toml / requirements.txt
├── src/tnr/
│   ├── cli.py, tasks.py      CLI y tareas invocables (idempotentes, retornan JSON → orquestador)
│   ├── config.py             INPUT_DIR, BRONZE_DIR, PARQUET_DIR, WAREHOUSE_PATH, URL postventa, zona, corte
│   ├── common/               contratos, ejecución de lotes, tipos, identidad, eventos web, stream Bronze, BD
│   ├── ingest/               un módulo por fuente
│   ├── gold/                 comercial, conciliación, cierre de checkout, stock
│   └── apps/                 receptor Flask, productor, consultas y dashboards Dash
├── sql/                      schema*.sql (DDL y vistas) y consultas_*.sql (consultas de prueba)
├── tests/                    pruebas de aceptación por fase (+ servidor de postventa en proceso)
└── data/
    ├── *                     entradas (TNR_EP5_datos_de_entrada)
    ├── bronze/               lotes y recepciones originales     (generado, no versionado)
    └── parquet/              exportaciones de Silver y Gold     (generado, no versionado)
warehouse.sqlite              Silver + Gold + Serving + control  (generado, no versionado)
```

## 7. Requisitos e instalación

```bash
uv venv && source .venv/bin/activate   # o: python -m venv .venv
uv pip install -e ".[dev]"             # o: pip install -e ".[dev]"; versiones probadas en requirements.txt
python -m pytest                        # 46 pruebas de aceptación
```

Variables de entorno opcionales: `TNR_INPUT_DIR`, `TNR_BRONZE_DIR`, `TNR_PARQUET_DIR`, `TNR_WAREHOUSE_PATH`, `TNR_POSTVENTA_URL` (por defecto `http://127.0.0.1:8766/ordenes`), `TNR_WEB_URL` (por defecto `http://127.0.0.1:8765/eventos`), `TNR_TZ`, `TNR_CORTE`, `TNR_SIM_INICIO` y `TNR_SIM_FIN`.

## 8. Ejecución

```bash
python -m tnr init

# --- carril batch (cada ingesta es independiente; reejecutar con la misma huella no republica)
python -m tnr ingest ventas --input sql      # --reproceso reconstruye desde Bronze
python -m tnr ingest productos
python -m tnr ingest clientes
python -m tnr ingest despachos
python -m tnr ingest notas_credito
python -m tnr ingest postventa               # requiere la API (p. ej. python data/postventa_mock.py)
python -m tnr ingest web_historico
python -m tnr ingest inventario
python -m tnr publish comercial --corte 2025-12-31

# --- carril speed: checkout web
python -m tnr serve web                                                  # terminal aparte
python -m tnr replay web --file eventos_web_stream.jsonl --hasta 77      # provisional 5/20
python -m tnr close web --minuto 2025-12-26T13:00:00-03:00               # cierre v1 5/20
python -m tnr replay web --file eventos_web_stream.jsonl --desde 78      # SIM-021-R tardío → reproceso
python -m tnr close web --minuto 2025-12-26T13:00:00-03:00               # cierre v2 5/21
python -m tnr serving web --desde 2025-12-26T13:00:00-03:00 --hasta 2025-12-26T13:01:00-03:00

# --- carril speed: inventario
python -m tnr replay movimientos --file movimientos_inventario.jsonl --hasta 6
python -m tnr serving inventario --corte 2025-12-26T12:05:00-03:00       # P001 = 2
python -m tnr replay movimientos --file movimientos_inventario.jsonl --desde 7
python -m tnr close inventario --corte 2025-12-26T12:05:00-03:00         # P001 = 5

# --- dashboards
python -m tnr serve dashboard            # http://127.0.0.1:8050/comercial y /operaciones
```

Todas las tareas están en `tnr.tasks` como funciones que devuelven un `dict` serializable, así que un orquestador como Airflow (con `PythonOperator` o `BashOperator`) puede invocarlas sin cambios.

## 9. Etapas del pipeline

### 9.1 Contratos comunes

```text
extract(config) -> Iterable[RawRecord]
normalize(record) -> SilverRecord | Rejection
persist(records, run_id) -> LoadResult
```

`common/batch.py` ejecuta cada lote en este orden: adquirir → SHA-256 → control de idempotencia → archivar Bronze → extraer → deduplicar → normalizar. Después publica Silver, rechazos, exclusiones, revisiones, alertas y el estado del lote **en una sola transacción**, y al final exporta el Parquet.

### 9.2 Ingesta comercial

| Fuente | Reglas principales |
|--------|-------------------|
| Ventas | Copia estable de `ventas_crudas` ordenada por `fila_origen`. Deduplica `id_venta`: si solo difieren las observaciones, conserva la menor `fila_origen`, y si difieren campos de negocio el grupo va a revisión. Fecha de 2025, precio > 0, cantidad ≠ 999, estados `confirmada/pendiente/cancelada`. El RUT normalizado va solo a `silver_ventas_identidad` (restringida). |
| Productos | Mapa canónico de seis categorías; `precio_lista` con separadores CLP y nulos con alerta. |
| Clientes CRM | RUT módulo 11. Sin RUT, correo **y** teléfono deben apuntar al mismo cliente; si no, va a revisión sin fusionar. Consentimiento más restrictivo (`no > sin_registro > si`), se prefiere el correo verificado y el mapa `id_crm → customer_id` es estable. |
| Despachos / notas | Validación de ID, fechas ISO y montos. La validez de cada nota se evalúa en la publicación en orden de fecha e ID: no puede ser anterior a la venta ni superar su bruto. |
| Postventa | Paginación por `siguiente` dentro del mismo origen, reintentos ante 429/5xx/timeout, coherencia con `total` antes de publicar y Bronze por página con metadatos HTTP. |

### 9.3 Eventos web y carril speed

- **Lote histórico:** aplana `evento/usuario/contexto` y aplica la zona `America/Santiago` a timestamps sin offset. Una hora ambigua toma la primera ocurrencia y una inexistente avanza +1 h; ambas quedan registradas en `etl_incidencias_tz`.
- **Receptor `POST /eventos`:** escribe en Bronze antes de responder (`202 {id_recepcion, ts_ingesta, estado}`, o `400` si el cuerpo no es JSON), con un único escritor por archivo activo. Inserta en Silver por `id_evento`: un reenvío idéntico queda como `duplicada` y un payload incompatible va a `revision`. `stream_recepciones` confirma cada recepción, e `ingest web_stream` recupera las que no se confirmaron.
- **Cierre:** relee Bronze hasta el corte de ingesta, resuelve `id_evento` en todas las recepciones, filtra el minuto `[inicio, fin)` y reemplaza la ventana en `gold_mart_checkout_minuto` con versión y corte. Un evento válido que llega después marca la ventana como `reproceso_pendiente`.

### 9.4 Inventario

Para cada SKU × ubicación, el saldo toma el último snapshot válido al corte y le suma los movimientos con `corte_snapshot < ts_evento ≤ corte`, una vez cada uno. Un movimiento sin snapshot queda pendiente de integración (`vw_movimientos_sin_snapshot`). Las reglas de signo son: una venta resta, una reposición suma y un ajuste admite ambos signos. Cuando llega un movimiento tardío, el cierre afectado pasa a `reproceso_pendiente`.

### 9.5 Publicación Gold

`publish comercial` valida las dependencias (ventas y productos son obligatorias), registra los `run_id` leídos, reemplaza la publicación del mismo corte y comprueba el grano después de los cruces. También audita que bruto − notas = neto y que Cliente 360 más las ventas anónimas den la venta neta. Las referencias sin contraparte quedan en `ctl_referencias_pendientes`.

## 10. Controles de calidad de datos

| Dimensión | Control aplicado | Resultado (corte 2025-12-31) |
|-----------|------------------|-----------|
| **Integridad** | Recuento por etapa en `gold_mart_conciliacion_ventas` | 37.971 recibidas → 36.988 únicas → 36.643 válidas → 36.641 confirmadas |
| **Unicidad** | Deduplicación por llave con exclusión trazable a la fila conservada | 983 duplicados de ventas (950 exactos y 33 que solo difieren en observaciones), 1 reenvío web, 1 reenvío de movimiento |
| **Validez** | Rechazos con motivo y referencia a Bronze (`etl_rechazos`) | 295 fechas inválidas, 32 precios no positivos y 18 cantidades `999`; coincide con la pauta docente |
| **Completitud** | Alertas de calidad para nulos o valores no interpretables (`etl_alertas_calidad`) | 14 alertas en productos (precio "consultar", marca o costo ausentes y categorías normalizadas) |
| **Consistencia** | Revisión de identidades contradictorias y payloads incompatibles (`etl_revision`) | `CRM-D0003` en revisión con evidencia; 0 conflictos de negocio en ventas |
| **Trazabilidad** | `etl_runs` (fuente, SHA-256, versión, recuentos, Bronze y Parquet) y `gold_publicaciones` (run_ids leídos) | Cada cifra se puede rastrear de la publicación al lote y al archivo de Bronze |

## 11. Salida funcional para el negocio

- **Dashboard comercial** (`/comercial`): KPIs de venta neta, notas y ventas sin cliente; cascada de conciliación con la referencia docente por separado; venta neta por canal y modalidad; perfil Cliente 360 por `customer_id`.
- **Dashboard de operaciones** (`/operaciones`, se actualiza cada 3 s): intentos de checkout, tasa de error 5xx (sumando errores e intentos antes de dividir), sesiones afectadas con `COUNT(DISTINCT id_sesion)`, latencia, estado y versión de cada ventana, y stock por SKU × ubicación con alerta de reposición.
- **Consultas SQL:** `sql/consultas_comercial.sql` y `sql/consultas_web.sql`.
- **Parquet:** `data/parquet/silver/<tabla>/<run_id>.parquet` y `data/parquet/gold/…`.

## 12. Evaluación de resultados

| Prueba | Resultado esperado | Obtenido |
|--------|--------------------|----------|
| Ventas y CRM | 37.971 recepciones y 36.988 ID antes del filtro de calidad; la reejecución conserva claves y montos; identidades contradictorias trazadas | ✔ |
| Integración | V025409 una vez con canal Online y retiro por T07; dos notas aplicadas a V023142 (neto 651.760); NC-0047 huérfana | ✔ |
| Checkout | 25% provisional (5/20); 23,81% al cierre (5/21) | ✔ |
| Stock | P001 en CD01: 6 → 2, el reenvío mantiene 2, el cierre con MOV-0035 da 5 | ✔ |

- **Venta neta 2025 (escenario ampliado):** CLP 48.055.496.260, que resulta del bruto confirmado de 48.069.794.070 menos notas por 14.297.810.
- **Referencia docente:** la ficha declara CLP 48.500.012.180. Las mismas reglas del pipeline aplicadas al XLSX docente dan 48.071.953.970, y la brecha de −428 M corresponde a las 345 filas rechazadas por calidad.
- **Clientes:** 2.501 en Cliente 360. 1.849 ventas no traen RUT y se mantienen en el total comercial sin cliente vinculado.

## 13. Limitaciones

- SQLite comparte la base entre el receptor y la UI gracias a WAL, pero no está pensado para alta concurrencia de escritura.
- El cierre de ventanas y de stock se ejecuta a pedido (`close`); no hay un planificador ni una marca de agua automática.
- El productor de eventos web y el servidor de postventa reales viven en otro proyecto; este repositorio incluye un productor mínimo (`replay`) y pruebas con un servidor en proceso.
- La resolución de identidad en revisión es manual: las fichas quedan en `etl_revision` con un `customer_id` provisional.
- Los dashboards están diseñados solo para modo claro.

## 14. Recomendaciones de mejora

| Mejora | Beneficio |
|--------|-----------|
| DAG de Airflow sobre `tnr.tasks` (ingestas en paralelo → publicación → cierres programados) | Automatización y reintentos |
| Cierre automático por marca de agua y latencia permitida | Ventanas cerradas sin intervención |
| Migrar Silver y Gold a PostgreSQL o DuckDB, y Bronze a almacenamiento de objetos | Concurrencia y volumen |
| Flujo de resolución para `etl_revision` | Cerrar los casos de identidad pendientes |
| Monitoreo de `etl_runs` y alertas de calidad | Detección temprana de fallas |

## 15. Equipo

| Integrante | Rol / responsabilidad |
|------------|-----------------------|
| _[Nombre 1]_ | _[Ingesta y perfilado]_ |
| _[Nombre 2]_ | _[Limpieza, transformación e integración]_ |
| _[Nombre 3]_ | _[Persistencia, visualización y documentación]_ |

**Docente:** _[Nombre]_
**Sección:** _[Sección]_

---

*Proyecto desarrollado con fines académicos para la asignatura DAY1102 – Administración y Almacenamiento de Datos.*
