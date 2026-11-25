-- TechNova Retail 360 · warehouse.sqlite
-- Control, Silver y Gold. Cada relación Silver tiene un único módulo escritor (indicado en el comentario).

-- ============================== CONTROL ==============================
CREATE TABLE IF NOT EXISTS etl_runs (
    run_id          TEXT PRIMARY KEY,
    fuente          TEXT NOT NULL,
    ruta            TEXT,
    sha256          TEXT,
    version_esquema TEXT NOT NULL,
    modo            TEXT NOT NULL DEFAULT 'normal' CHECK (modo IN ('normal', 'forzado', 'reproceso')),
    ts_ingesta      TEXT NOT NULL,              -- UTC ISO
    ts_fin          TEXT,
    estado          TEXT NOT NULL CHECK (estado IN ('en_proceso', 'exitoso', 'fallido', 'omitido', 'reemplazado')),
    n_recibidos     INTEGER,
    n_excluidos     INTEGER,
    n_revision      INTEGER,
    n_rechazados    INTEGER,
    n_publicados    INTEGER,
    n_alertas       INTEGER,
    bronze_path     TEXT,
    parquet_path    TEXT,
    detalle         TEXT
);
-- Un run exitoso por fuente + huella + versión de esquema
CREATE UNIQUE INDEX IF NOT EXISTS ux_runs_exitoso
    ON etl_runs (fuente, sha256, version_esquema) WHERE estado = 'exitoso';

CREATE TABLE IF NOT EXISTS etl_rechazos (
    id_rechazo  INTEGER PRIMARY KEY,
    run_id      TEXT NOT NULL REFERENCES etl_runs (run_id),
    fuente      TEXT NOT NULL,
    ref_bronze  TEXT NOT NULL,
    id_registro TEXT,
    motivo      TEXT NOT NULL,
    detalle     TEXT
);
CREATE INDEX IF NOT EXISTS ix_rechazos_run ON etl_rechazos (run_id);

CREATE TABLE IF NOT EXISTS etl_exclusiones (
    id_exclusion   INTEGER PRIMARY KEY,
    run_id         TEXT NOT NULL REFERENCES etl_runs (run_id),
    fuente         TEXT NOT NULL,
    ref_bronze     TEXT NOT NULL,
    id_registro    TEXT NOT NULL,
    motivo         TEXT NOT NULL,
    ref_conservada TEXT
);
CREATE INDEX IF NOT EXISTS ix_exclusiones_run ON etl_exclusiones (run_id);

CREATE TABLE IF NOT EXISTS etl_revision (
    id_revision INTEGER PRIMARY KEY,
    run_id      TEXT NOT NULL REFERENCES etl_runs (run_id),
    fuente      TEXT NOT NULL,
    id_registro TEXT NOT NULL,
    motivo      TEXT NOT NULL,
    evidencia   TEXT,
    estado      TEXT NOT NULL DEFAULT 'pendiente'
);

CREATE TABLE IF NOT EXISTS etl_alertas_calidad (
    id_alerta      INTEGER PRIMARY KEY,
    run_id         TEXT NOT NULL REFERENCES etl_runs (run_id),
    fuente         TEXT NOT NULL,
    ref_bronze     TEXT NOT NULL,
    id_registro    TEXT,
    campo          TEXT NOT NULL,
    codigo         TEXT NOT NULL,
    valor_original TEXT
);
CREATE INDEX IF NOT EXISTS ix_alertas_run ON etl_alertas_calidad (run_id);

-- Confirmación de cada recepción continua procesada hacia Silver (punto de reproceso)
CREATE TABLE IF NOT EXISTS stream_recepciones (
    id_recepcion TEXT PRIMARY KEY,
    fuente       TEXT NOT NULL,
    archivo      TEXT NOT NULL,
    ts_ingesta   TEXT NOT NULL,
    id_evento    TEXT,
    estado       TEXT NOT NULL CHECK (estado IN ('insertada', 'duplicada', 'revision', 'rechazada')),
    detalle      TEXT,
    run_id       TEXT NOT NULL REFERENCES etl_runs (run_id)
);
CREATE INDEX IF NOT EXISTS ix_stream_recepciones_fuente ON stream_recepciones (fuente, estado);

-- ============================== SILVER ==============================
-- escritor: tnr.ingest.ventas
CREATE TABLE IF NOT EXISTS silver_ventas (
    id_venta               TEXT PRIMARY KEY,
    fecha                  TEXT NOT NULL,       -- ISO YYYY-MM-DD
    cantidad               INTEGER NOT NULL CHECK (cantidad > 0),
    precio_unitario_clp    INTEGER NOT NULL CHECK (precio_unitario_clp > 0),
    costo_unitario_clp     INTEGER,
    canal                  TEXT NOT NULL,
    estado_venta           TEXT NOT NULL CHECK (estado_venta IN ('confirmada', 'pendiente', 'cancelada')),
    id_producto            TEXT NOT NULL,
    id_tienda              TEXT,
    ciudad_tienda          TEXT,
    id_pedido_omnicanal    TEXT,
    modalidad_entrega      TEXT,
    id_ubicacion_despacho  TEXT,
    observaciones          TEXT,
    estado_vinculo_cliente TEXT NOT NULL CHECK (estado_vinculo_cliente IN ('rut_disponible', 'pendiente')),
    fila_origen            INTEGER NOT NULL,
    run_id                 TEXT NOT NULL REFERENCES etl_runs (run_id)
);
CREATE INDEX IF NOT EXISTS ix_ventas_pedido ON silver_ventas (id_pedido_omnicanal);

-- escritor: tnr.ingest.ventas · RELACIÓN RESTRINGIDA (no se exporta a Parquet ni a marts)
CREATE TABLE IF NOT EXISTS silver_ventas_identidad (
    id_venta        TEXT PRIMARY KEY REFERENCES silver_ventas (id_venta) ON DELETE CASCADE,
    rut_normalizado TEXT NOT NULL,
    run_id          TEXT NOT NULL REFERENCES etl_runs (run_id)
);
CREATE INDEX IF NOT EXISTS ix_ventas_identidad_rut ON silver_ventas_identidad (rut_normalizado);

-- escritor: tnr.ingest.productos
CREATE TABLE IF NOT EXISTS silver_productos (
    id_producto          TEXT PRIMARY KEY,
    nombre_producto      TEXT,
    categoria            TEXT,
    subcategoria         TEXT,
    marca                TEXT,
    precio_lista_clp     INTEGER,
    costo_referencia_clp INTEGER CHECK (costo_referencia_clp IS NULL OR costo_referencia_clp >= 0),
    run_id               TEXT NOT NULL REFERENCES etl_runs (run_id)
);

-- escritor: tnr.ingest.despachos (fase 2)
CREATE TABLE IF NOT EXISTS silver_despachos (
    id_despacho           TEXT PRIMARY KEY,
    id_pedido_omnicanal   TEXT NOT NULL,
    id_ubicacion_despacho TEXT NOT NULL,
    fecha_despacho        TEXT NOT NULL,
    run_id                TEXT NOT NULL REFERENCES etl_runs (run_id)
);
CREATE INDEX IF NOT EXISTS ix_despachos_pedido ON silver_despachos (id_pedido_omnicanal);

-- escritor: tnr.ingest.notas_credito (fase 2)
CREATE TABLE IF NOT EXISTS silver_notas_credito (
    id_nota         TEXT PRIMARY KEY,
    id_venta_origen TEXT NOT NULL,
    fecha_emision   TEXT NOT NULL,
    monto_clp       INTEGER NOT NULL CHECK (monto_clp > 0),
    run_id          TEXT NOT NULL REFERENCES etl_runs (run_id)
);

-- escritor: tnr.ingest.clientes
CREATE TABLE IF NOT EXISTS silver_clientes (
    customer_id               TEXT PRIMARY KEY,
    comuna                    TEXT,
    canal_alta                TEXT,
    fecha_alta                TEXT,
    consentimiento_publicidad TEXT NOT NULL CHECK (consentimiento_publicidad IN ('si', 'no', 'sin_registro')),
    estado_identidad          TEXT NOT NULL CHECK (estado_identidad IN ('resuelto', 'revision')),
    n_fichas_crm              INTEGER NOT NULL,
    run_id                    TEXT NOT NULL REFERENCES etl_runs (run_id)
);

-- escritor: tnr.ingest.clientes · mapa ESTABLE (no se borra entre corridas)
CREATE TABLE IF NOT EXISTS ident_crm_customer (
    id_crm         TEXT PRIMARY KEY,
    customer_id    TEXT NOT NULL,
    metodo_vinculo TEXT NOT NULL,       -- rut | correo_y_telefono | nuevo | revision
    run_id         TEXT NOT NULL REFERENCES etl_runs (run_id)
);

-- escritor: tnr.ingest.clientes · RELACIÓN RESTRINGIDA
CREATE TABLE IF NOT EXISTS ident_rut_customer (
    rut_normalizado TEXT PRIMARY KEY,
    customer_id     TEXT NOT NULL,
    run_id          TEXT NOT NULL REFERENCES etl_runs (run_id)
);

-- escritor: tnr.ingest.clientes · RELACIÓN RESTRINGIDA (contacto)
CREATE TABLE IF NOT EXISTS ident_contacto_cliente (
    customer_id      TEXT PRIMARY KEY REFERENCES silver_clientes (customer_id) ON DELETE CASCADE,
    email_contacto   TEXT,
    email_verificado INTEGER,
    telefono         TEXT,
    run_id           TEXT NOT NULL REFERENCES etl_runs (run_id)
);

-- escritor: tnr.ingest.postventa
CREATE TABLE IF NOT EXISTS silver_postventa (
    id_orden       TEXT PRIMARY KEY,
    id_venta       TEXT NOT NULL,
    estado         TEXT NOT NULL CHECK (estado IN ('recibida', 'en_reparacion', 'cerrada')),
    fecha_apertura TEXT NOT NULL,
    fecha_cierre   TEXT,
    en_garantia    INTEGER NOT NULL CHECK (en_garantia IN (0, 1)),
    pagina         INTEGER NOT NULL,
    run_id         TEXT NOT NULL REFERENCES etl_runs (run_id)
);

-- ============================== GOLD ==============================
CREATE TABLE IF NOT EXISTS gold_publicaciones (
    publish_id     TEXT PRIMARY KEY,
    mart           TEXT NOT NULL,       -- dominio publicado (comercial)
    fecha_corte    TEXT NOT NULL,
    version_reglas TEXT NOT NULL,
    run_ids        TEXT NOT NULL,       -- JSON {tabla_silver: [run_id, ...]}
    ts_publicacion TEXT NOT NULL,
    n_filas        INTEGER NOT NULL,
    parquet_path   TEXT,
    detalle        TEXT
);

-- Estado de cada nota de crédito evaluada en la publicación comercial
CREATE TABLE IF NOT EXISTS ctl_notas_evaluacion (
    fecha_corte     TEXT NOT NULL,
    id_nota         TEXT NOT NULL,
    id_venta_origen TEXT NOT NULL,
    fecha_emision   TEXT NOT NULL,
    monto_clp       INTEGER NOT NULL,
    estado          TEXT NOT NULL,      -- aplicada | huerfana | rechazada_* | pendiente_*
    publish_id      TEXT NOT NULL,
    PRIMARY KEY (fecha_corte, id_nota)
);

CREATE TABLE IF NOT EXISTS gold_mart_ventas (
    fecha_corte            TEXT NOT NULL,
    id_venta               TEXT NOT NULL,
    fecha                  TEXT NOT NULL,      -- reconocimiento = fecha de venta (PoC)
    customer_id            TEXT,               -- NULL = venta sin cliente vinculado
    id_producto            TEXT NOT NULL,
    categoria              TEXT,
    subcategoria           TEXT,
    canal                  TEXT NOT NULL,
    modalidad_entrega      TEXT,
    id_tienda              TEXT,
    id_ubicacion_venta     TEXT,
    id_ubicacion_operativa TEXT,               -- primer despacho por fecha e ID, o la de la venta
    n_despachos            INTEGER NOT NULL,
    cantidad               INTEGER NOT NULL,
    precio_unitario_clp    INTEGER NOT NULL,
    costo_unitario_clp     INTEGER,
    monto_bruto_clp        INTEGER NOT NULL,
    n_notas                INTEGER NOT NULL,
    monto_notas_clp        INTEGER NOT NULL,
    monto_neto_clp         INTEGER NOT NULL,
    publish_id             TEXT NOT NULL REFERENCES gold_publicaciones (publish_id),
    PRIMARY KEY (fecha_corte, id_venta)
);

CREATE TABLE IF NOT EXISTS gold_mart_cliente_360 (
    fecha_corte                TEXT NOT NULL,
    customer_id                TEXT NOT NULL,
    comuna                     TEXT,
    canal_alta                 TEXT,
    fecha_alta                 TEXT,
    consentimiento_publicidad  TEXT NOT NULL,
    estado_identidad           TEXT NOT NULL,
    frecuencia_compras         INTEGER NOT NULL,
    fecha_ultima_compra        TEXT,
    recencia_dias              INTEGER,
    monto_bruto_clp            INTEGER NOT NULL,
    monto_neto_clp             INTEGER NOT NULL,
    canal_preferido            TEXT,
    n_ordenes_postventa        INTEGER NOT NULL,
    n_ordenes_abiertas         INTEGER NOT NULL,
    n_ordenes_garantia         INTEGER NOT NULL,
    dias_resolucion_promedio   REAL,
    publish_id                 TEXT NOT NULL REFERENCES gold_publicaciones (publish_id),
    PRIMARY KEY (fecha_corte, customer_id)
);

CREATE TABLE IF NOT EXISTS gold_mart_conciliacion_ventas (
    fecha_corte  TEXT NOT NULL,
    publish_id   TEXT NOT NULL REFERENCES gold_publicaciones (publish_id),
    escenario    TEXT NOT NULL,          -- ampliado | referencia_docente
    orden        INTEGER NOT NULL,
    etapa        TEXT NOT NULL,
    run_id       TEXT,
    filas        INTEGER,
    monto_clp    INTEGER,
    descripcion  TEXT,
    PRIMARY KEY (publish_id, escenario, orden)
);

-- Auditoría de referencias sin contraparte detectadas en la integración
CREATE TABLE IF NOT EXISTS ctl_referencias_pendientes (
    fecha_corte TEXT NOT NULL,
    publish_id  TEXT NOT NULL,
    tipo        TEXT NOT NULL,           -- despacho_sin_venta | postventa_sin_venta | venta_rut_sin_cliente | ...
    id_registro TEXT NOT NULL,
    referencia  TEXT,
    PRIMARY KEY (fecha_corte, tipo, id_registro)
);
