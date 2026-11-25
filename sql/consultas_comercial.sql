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
