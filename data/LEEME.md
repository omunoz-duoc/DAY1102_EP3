# TechNova Retail 360 · Fuentes de entrada para EP5

Simulación sobre los datos docentes de 2025. Los atributos y fuentes nuevas son sintéticos. Se conservaron las anomalías del CSV y del catálogo de origen para el perfilado. Los CSV ampliados y nuevos usan UTF-8 y `;`. Los montos nuevos son enteros en CLP.

| Archivo | Entrada | Tamaño |
|---|---|---:|
| `ventas_db.sql` | Crea y carga `ventas_crudas`. Probado en SQLite. | 37.971 filas |
| `ventas_2025_tnr_ampliadas.csv` | Exportación del mismo conjunto de ventas. Selecciona SQL o CSV como fuente en una corrida. | 37.971 filas |
| `productos_tnr_ampliados.xlsx` | Catálogo extendido, hoja `productos`. | 45 SKU |
| `clientes_crm.csv` | CRM con contactos sintéticos y registros repetidos o ambiguos. | 2.530 filas |
| `despachos.csv` | Extracto de operaciones con `id_pedido_omnicanal`. | 60 eventos |
| `notas_credito.csv` | Notas vinculadas por `id_venta_origen`. | 47 filas |
| `inventario_inicial.csv` | Corte inicial de `CD01` por SKU. | 10 filas |
| `movimientos_inventario.jsonl` | Un movimiento por línea, en orden simulado de llegada. | 36 recepciones |
| `logs_web_tnr.json` | Histórico original como arreglo JSON anidado. | 5.000 eventos |
| `eventos_web_stream.jsonl` | Un objeto JSON por línea para enviar como cuerpo HTTP, en orden simulado de llegada. | 78 recepciones |
| `postventa_pagina_1.json` a `postventa_pagina_3.json` | Páginas de la API con `pagina`, `siguiente`, `total` y `ordenes`. | 30 órdenes |
| `referencia/dataset_tnr_original.xlsx` | Excel docente para contrastar perfilado y cifras. | Referencia |

## Carga y reproducción

- **Ventas.** Ejecuta `sqlite3 tnr.db ".read ventas_db.sql"` desde la carpeta descomprimida, en una base nueva. También puedes ejecutar el archivo con `sqlite3.Connection.executescript` en Python. `fila_origen` distingue las filas crudas que comparten `id_venta`. Los campos originales `fecha` y `precio_unitario` siguen como texto para conservar valores anómalos. La cifra contable docente corresponde a la base original. Las cancelaciones, los pendientes, los costos y las notas de crédito son casos sintéticos que pueden cambiar las métricas del escenario ampliado.
- **Logs.** Lee el JSON histórico como lote. Para la demo en vivo, reenvía las líneas del JSONL en orden y conserva `timestamp` como hora del evento. Tu receptor agrega `ts_ingesta`. Acelera o pausa el envío a conveniencia. Para filtrar la demo, usa el reloj simulado del 26 de diciembre de 2025, zona `-03:00`.
- **Postventa.** Ejecuta `python postventa_mock.py` y consulta `http://127.0.0.1:8766/ordenes?page=1`. Sigue `siguiente` hasta que sea nulo. Las páginas son respuestas estáticas de una API local.
- **Identidad.** En CRM, los correos usan `example.invalid` y los teléfonos son simulados. Usa un identificador interno generado por el pipeline para la capa analítica.

## Casos verificables

| Caso | Datos y resultado esperado |
|---|---|
| Cliente 360 | `CRM-D0001` contradice el consentimiento de su ficha base. `CRM-D0002` coincide por correo y teléfono después de normalizar, con RUT vacío. `CRM-D0003` tiene señales de dos fichas y requiere revisión. `CRM-D0005` permite preferir el correo verificado de la ficha base. |
| Retiro omnicanal | `V025409` tiene pedido `PED-V025409` y despacho `DSP-0001`. La venta es `Online` y el retiro lo prepara `T07`. El pedido aporta el ingreso una vez. |
| Venta neta | `NC-0001` y `NC-0046` son dos abonos parciales de `V023142`. `NC-0047` referencia `V999999`, ausente de ventas. |
| Stock | `P001` en `CD01` empieza con 8 unidades físicas y 2 reservadas, 6 disponibles. `MOV-0001` baja el disponible a 2 y su reenvío conserva ese saldo. El evento tardío `MOV-0035` lleva el saldo a 5. |
| Checkout | En la ventana `13:00–13:01` llegan 20 resultados iniciales, 5 con HTTP 500, todos en móvil. El reenvío de `SIM-001-R` conserva **5/20 = 25%** tras deduplicar. Después de un evento de las `13:01`, llega `SIM-021-R`, un éxito tardío de la ventana anterior. Al reprocesar el minuto quedan **5/21 ≈ 23,81%**. Hay 15 eventos `purchase` con `id_venta` válido. |
