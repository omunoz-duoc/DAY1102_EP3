-- Consultas de checkout web. Ejecutar: sqlite3 warehouse.sqlite < sql/consultas_web.sql
-- Los minutos se guardan en UTC: 13:00 -03:00 = 16:00Z.

-- 1. Serving del minuto 13:00 del 26-dic (Gold si está cerrado, Silver si está abierto)
SELECT minuto_utc, dispositivo, intentos, errores_5xx, tasa_error, estado_ventana, version, fuente
FROM serving_checkout_minuto
WHERE minuto_utc = '2025-12-26T16:00:00Z';

-- 2. Tasa global del minuto: sumar errores e intentos antes de dividir
SELECT SUM(errores_5xx) AS errores, SUM(intentos) AS intentos,
       ROUND(100.0 * SUM(errores_5xx) / SUM(intentos), 2) AS tasa_pct
FROM serving_checkout_minuto
WHERE minuto_utc = '2025-12-26T16:00:00Z';

-- 3. Estado y trazabilidad de los cierres
SELECT w.*, r.ruta, r.n_recibidos AS recepciones_leidas
FROM ctl_ventanas_web w JOIN etl_runs r USING (run_id);

-- 4. Recepciones por estado (cada reenvío queda en Bronze, no se duplica en Silver)
SELECT estado, COUNT(*) FROM web_recepciones GROUP BY estado;

-- 5. Cruce de eventos purchase con ventas (puede quedar pendiente)
SELECT estado_cruce, COUNT(*) FROM vw_purchase_web_venta GROUP BY estado_cruce;
