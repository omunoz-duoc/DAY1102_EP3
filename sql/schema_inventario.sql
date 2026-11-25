-- Inventario: snapshot por lote + movimientos continuos. Grano del mart: SKU x ubicación x corte.

-- escritor: tnr.ingest.inventario (UPSERT: conserva los cortes anteriores)
CREATE TABLE IF NOT EXISTS silver_inventario (
    id_producto       TEXT NOT NULL,
    id_ubicacion      TEXT NOT NULL,
    fecha_corte       TEXT NOT NULL,          -- UTC
    stock_fisico      INTEGER NOT NULL,
    stock_reservado   INTEGER NOT NULL,
    umbral_reposicion INTEGER NOT NULL CHECK (umbral_reposicion >= 0),
    run_id            TEXT NOT NULL REFERENCES etl_runs (run_id),
    CHECK (stock_fisico >= stock_reservado AND stock_reservado >= 0),
    PRIMARY KEY (id_producto, id_ubicacion, fecha_corte)
);

-- escritor: tnr.ingest.movimientos_inventario
CREATE TABLE IF NOT EXISTS silver_movimientos_inventario (
    id_evento      TEXT PRIMARY KEY,
    ts_evento      TEXT NOT NULL,             -- UTC
    id_producto    TEXT NOT NULL,
    id_ubicacion   TEXT NOT NULL,
    tipo           TEXT NOT NULL CHECK (tipo IN ('venta_confirmada', 'reposicion', 'ajuste')),
    delta_unidades INTEGER NOT NULL CHECK (delta_unidades <> 0),
    payload_sha256 TEXT NOT NULL,
    id_recepcion   TEXT NOT NULL,
    ts_ingesta     TEXT NOT NULL,
    run_id         TEXT NOT NULL REFERENCES etl_runs (run_id)
);
CREATE INDEX IF NOT EXISTS ix_mov_sku_ubic_ts ON silver_movimientos_inventario (id_producto, id_ubicacion, ts_evento);

-- Saldo provisional "al último dato": último snapshot + movimientos posteriores recibidos
CREATE VIEW IF NOT EXISTS vw_stock_provisional AS
WITH snap AS (
    SELECT s.*, ROW_NUMBER() OVER (PARTITION BY id_producto, id_ubicacion ORDER BY fecha_corte DESC) AS rn
    FROM silver_inventario s
), base AS (
    SELECT * FROM snap WHERE rn = 1
), mov AS (
    SELECT b.id_producto, b.id_ubicacion, SUM(m.delta_unidades) AS delta, COUNT(m.id_evento) AS n,
           MAX(m.ts_ingesta) AS ultima_ingesta
    FROM base b
    LEFT JOIN silver_movimientos_inventario m
           ON m.id_producto = b.id_producto AND m.id_ubicacion = b.id_ubicacion AND m.ts_evento > b.fecha_corte
    GROUP BY b.id_producto, b.id_ubicacion
)
SELECT b.id_producto, b.id_ubicacion, b.fecha_corte AS corte_snapshot,
       b.stock_fisico - b.stock_reservado AS disponible_inicial,
       COALESCE(m.delta, 0) AS delta_movimientos,
       m.n AS n_movimientos,
       b.stock_fisico - b.stock_reservado + COALESCE(m.delta, 0) AS saldo_disponible,
       b.umbral_reposicion,
       (b.stock_fisico - b.stock_reservado + COALESCE(m.delta, 0)) <= b.umbral_reposicion AS alerta_reposicion,
       (b.stock_fisico - b.stock_reservado + COALESCE(m.delta, 0)) < 0 AS incidencia_saldo_negativo,
       m.ultima_ingesta,
       p.id_producto IS NULL AS sku_sin_catalogo
FROM base b
LEFT JOIN mov m ON m.id_producto = b.id_producto AND m.id_ubicacion = b.id_ubicacion
LEFT JOIN silver_productos p ON p.id_producto = b.id_producto;

-- Movimientos pendientes de integración: no existe snapshot anterior para su SKU x ubicación
CREATE VIEW IF NOT EXISTS vw_movimientos_sin_snapshot AS
SELECT m.*
FROM silver_movimientos_inventario m
WHERE NOT EXISTS (SELECT 1 FROM silver_inventario s WHERE s.id_producto = m.id_producto
                  AND s.id_ubicacion = m.id_ubicacion AND s.fecha_corte < m.ts_evento);

CREATE TABLE IF NOT EXISTS ctl_cierres_stock (
    fecha_corte             TEXT PRIMARY KEY,   -- UTC
    estado                  TEXT NOT NULL CHECK (estado IN ('cerrada', 'reproceso_pendiente')),
    version                 INTEGER NOT NULL,
    corte_ingesta           TEXT NOT NULL,
    ts_cierre               TEXT NOT NULL,
    run_id                  TEXT NOT NULL REFERENCES etl_runs (run_id),
    n_pendientes            INTEGER NOT NULL,
    ts_ultimo_evento_tardio TEXT
);

CREATE TABLE IF NOT EXISTS gold_mart_stock_ubicacion (
    id_producto                TEXT NOT NULL,
    id_ubicacion               TEXT NOT NULL,
    fecha_corte                TEXT NOT NULL,    -- corte consultado (UTC)
    corte_snapshot             TEXT NOT NULL,
    disponible_inicial         INTEGER NOT NULL,
    delta_movimientos          INTEGER NOT NULL,
    n_movimientos              INTEGER NOT NULL,
    saldo_disponible           INTEGER NOT NULL,
    umbral_reposicion          INTEGER NOT NULL,
    alerta_reposicion          INTEGER NOT NULL,
    incidencia_saldo_negativo  INTEGER NOT NULL,
    ultima_ingesta_incorporada TEXT,
    corte_ingesta              TEXT NOT NULL,
    version                    INTEGER NOT NULL,
    run_id                     TEXT NOT NULL REFERENCES etl_runs (run_id),
    PRIMARY KEY (id_producto, id_ubicacion, fecha_corte)
);
