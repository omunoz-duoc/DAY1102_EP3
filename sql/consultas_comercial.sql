-- Consultas de prueba sobre gold_mart_ventas (fase 1). Ejecutar: sqlite3 warehouse.sqlite < sql/consultas_comercial.sql

-- 1. Ventas netas por canal y modalidad al corte
SELECT canal, modalidad_entrega, COUNT(*) AS ventas, SUM(monto_bruto_clp) AS bruto_clp, SUM(monto_neto_clp) AS neto_clp
FROM gold_mart_ventas
WHERE fecha_corte = '2025-12-31'
GROUP BY canal, modalidad_entrega
ORDER BY neto_clp DESC;

-- 2. Venta neta por categoría
SELECT categoria, SUM(monto_neto_clp) AS neto_clp
FROM gold_mart_ventas
WHERE fecha_corte = '2025-12-31'
GROUP BY categoria
ORDER BY neto_clp DESC;

-- 3. Trazabilidad: publicación -> lotes Silver -> Bronze
SELECT p.publish_id, p.version_reglas, r.fuente, r.run_id, r.sha256, r.bronze_path
FROM gold_publicaciones p
JOIN etl_runs r ON instr(p.run_ids, r.run_id) > 0
WHERE p.fecha_corte = '2025-12-31'
ORDER BY p.ts_publicacion DESC, r.fuente;

-- 4. Cascada de conciliación (escenario ampliado y referencia docente por separado)
SELECT escenario, orden, etapa, filas, monto_clp, descripcion
FROM gold_mart_conciliacion_ventas
WHERE fecha_corte = '2025-12-31'
ORDER BY escenario, orden;

-- 5. Perfil Cliente 360 de los casos verificables
SELECT m.id_crm, g.*
FROM ident_crm_customer m
JOIN gold_mart_cliente_360 g ON g.customer_id = m.customer_id AND g.fecha_corte = '2025-12-31'
WHERE m.id_crm IN ('CRM-D0001', 'CRM-D0002', 'CRM-D0003', 'CRM-D0005');

-- 6. Integración: retiro omnicanal y notas de crédito
SELECT id_venta, canal, modalidad_entrega, id_ubicacion_operativa, n_despachos,
       monto_bruto_clp, n_notas, monto_notas_clp, monto_neto_clp
FROM gold_mart_ventas
WHERE fecha_corte = '2025-12-31' AND id_venta IN ('V025409', 'V023142');

-- 7. Referencias sin contraparte (para reintentar tras nuevas cargas)
SELECT tipo, COUNT(*) AS n FROM ctl_referencias_pendientes WHERE fecha_corte = '2025-12-31' GROUP BY tipo;
