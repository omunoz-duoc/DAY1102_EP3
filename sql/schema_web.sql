-- Eventos web: Silver compartido (histórico + stream), control de recepciones, cierre de ventanas y Serving.

-- escritor: tnr.common.eventos_web (único módulo que escribe; lo invocan el lote histórico y el receptor)
CREATE TABLE IF NOT EXISTS silver_eventos_web (
    id_evento       TEXT PRIMARY KEY,
    origen          TEXT NOT NULL CHECK (origen IN ('historico', 'stream')),
    ts_evento       TEXT NOT NULL,               -- UTC 'YYYY-MM-DDTHH:MM:SS.mmmZ' (hora de negocio)
    tipo            TEXT NOT NULL,
    pagina          TEXT,
    duracion_seg    REAL,
    codigo_http     INTEGER,
    latencia_ms     INTEGER,
    id_venta        TEXT,
    id_sesion       TEXT,
    dispositivo     TEXT,
    navegador       TEXT,
    tienda_asociada TEXT,
    campania        TEXT,
    payload_sha256  TEXT NOT NULL,
    id_recepcion    TEXT,                        -- stream: recepción que publicó el hecho
    ts_ingesta      TEXT NOT NULL,               -- UTC, reloj del servidor
    run_id          TEXT NOT NULL REFERENCES etl_runs (run_id)
);
CREATE INDEX IF NOT EXISTS ix_eventos_tipo_ts ON silver_eventos_web (tipo, ts_evento);
CREATE INDEX IF NOT EXISTS ix_eventos_origen ON silver_eventos_web (origen);

-- RELACIÓN RESTRINGIDA
CREATE TABLE IF NOT EXISTS silver_eventos_web_identidad (
    id_evento       TEXT PRIMARY KEY REFERENCES silver_eventos_web (id_evento) ON DELETE CASCADE,
    rut_normalizado TEXT NOT NULL
);

-- Confirmación de cada recepción HTTP procesada hacia Silver (punto de reproceso)
CREATE TABLE IF NOT EXISTS web_recepciones (
    id_recepcion TEXT PRIMARY KEY,
    archivo      TEXT NOT NULL,
    ts_ingesta   TEXT NOT NULL,
    id_evento    TEXT,
    estado       TEXT NOT NULL CHECK (estado IN ('insertada', 'duplicada', 'revision', 'rechazada')),
    detalle      TEXT,
    run_id       TEXT NOT NULL REFERENCES etl_runs (run_id)
);

-- Incidencias de zona horaria (horas ambiguas o inexistentes resueltas)
CREATE TABLE IF NOT EXISTS etl_incidencias_tz (
    id_evento   TEXT NOT NULL,
    run_id      TEXT NOT NULL REFERENCES etl_runs (run_id),
    valor_local TEXT NOT NULL,
    tipo        TEXT NOT NULL CHECK (tipo IN ('ambigua', 'inexistente')),
    regla       TEXT NOT NULL,
    ts_resuelto TEXT NOT NULL,
    PRIMARY KEY (id_evento, run_id)
);

-- ------------------------------------------------------------------ provisional (speed)
CREATE VIEW IF NOT EXISTS vw_checkout_provisional AS
SELECT substr(ts_evento, 1, 16) || ':00Z'                             AS minuto_utc,
       dispositivo,
       COUNT(*)                                                       AS intentos,
       SUM(codigo_http BETWEEN 500 AND 599)                           AS errores_5xx,
       COUNT(DISTINCT CASE WHEN codigo_http BETWEEN 500 AND 599 THEN id_sesion END) AS sesiones_afectadas,
       SUM(latencia_ms)                                               AS latencia_suma_ms,
       COUNT(latencia_ms)                                             AS latencia_n,
       MAX(ts_ingesta)                                                AS ultima_actualizacion
FROM silver_eventos_web
WHERE tipo = 'checkout_request' AND codigo_http BETWEEN 100 AND 599
GROUP BY 1, 2;

-- ------------------------------------------------------------------ cierre (batch)
CREATE TABLE IF NOT EXISTS ctl_ventanas_web (
    minuto_utc             TEXT PRIMARY KEY,
    estado                 TEXT NOT NULL CHECK (estado IN ('cerrada', 'reproceso_pendiente')),
    version                INTEGER NOT NULL,
    corte_ingesta          TEXT NOT NULL,
    ts_cierre              TEXT NOT NULL,
    run_id                 TEXT NOT NULL REFERENCES etl_runs (run_id),
    n_recepciones_leidas   INTEGER NOT NULL,
    ts_ultimo_evento_tardio TEXT
);

CREATE TABLE IF NOT EXISTS gold_mart_checkout_minuto (
    minuto_utc         TEXT NOT NULL,
    dispositivo        TEXT NOT NULL,
    intentos           INTEGER NOT NULL,
    errores_5xx        INTEGER NOT NULL,
    sesiones_afectadas INTEGER NOT NULL,
    latencia_suma_ms   INTEGER,
    latencia_n         INTEGER NOT NULL,
    version            INTEGER NOT NULL,
    corte_ingesta      TEXT NOT NULL,
    run_id             TEXT NOT NULL REFERENCES etl_runs (run_id),
    PRIMARY KEY (minuto_utc, dispositivo)
);

-- ------------------------------------------------------------------ serving: Gold cerrado > Silver abierto
CREATE VIEW IF NOT EXISTS serving_checkout_minuto AS
SELECT g.minuto_utc, g.dispositivo, g.intentos, g.errores_5xx,
       ROUND(1.0 * g.errores_5xx / g.intentos, 6) AS tasa_error,
       g.sesiones_afectadas, g.latencia_suma_ms, g.latencia_n,
       w.estado AS estado_ventana, w.version, w.ts_cierre AS ultima_actualizacion, 'gold' AS fuente
FROM gold_mart_checkout_minuto g
JOIN ctl_ventanas_web w ON w.minuto_utc = g.minuto_utc
UNION ALL
SELECT p.minuto_utc, p.dispositivo, p.intentos, p.errores_5xx,
       ROUND(1.0 * p.errores_5xx / p.intentos, 6),
       p.sesiones_afectadas, p.latencia_suma_ms, p.latencia_n,
       'provisional', NULL, p.ultima_actualizacion, 'silver'
FROM vw_checkout_provisional p
WHERE NOT EXISTS (SELECT 1 FROM ctl_ventanas_web w WHERE w.minuto_utc = p.minuto_utc);

-- ------------------------------------------------------------------ actividad histórica y cruce con ventas
CREATE VIEW IF NOT EXISTS vw_actividad_web_dia AS
SELECT substr(ts_evento, 1, 10) AS dia_utc, origen, tipo, dispositivo, COUNT(*) AS eventos
FROM silver_eventos_web
GROUP BY 1, 2, 3, 4;

CREATE VIEW IF NOT EXISTS vw_purchase_web_venta AS
SELECT e.id_evento, e.id_venta, e.ts_evento,
       CASE WHEN v.id_venta IS NULL THEN 'pendiente' ELSE 'vinculada' END AS estado_cruce
FROM silver_eventos_web e
LEFT JOIN silver_ventas v ON v.id_venta = e.id_venta
WHERE e.tipo = 'purchase' AND e.id_venta IS NOT NULL;
