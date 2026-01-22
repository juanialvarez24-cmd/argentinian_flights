/*
==========================================================
IMPORTANT DATA QUALITY NOTE:
All queries are filtered by 'tipo_de_movimiento = Despegue' 
to avoid double-counting domestic flights (departure/arrival).
==========================================================
*/

-- Query 1. Total flight operations (Dec 2021 - Jan 2022)
-- Business Goal: Analyze flight volume during the peak summer season
SELECT
    COUNT(*) AS total_flights
FROM
    aeropuerto_tabla
WHERE 
	fecha >= '2021-12-01' AND fecha <= '2022-01-31'
	AND tipo_de_movimiento = 'Despegue';

-- Query 2: Total passengers for Aerolíneas Argentinas
-- Business Goal: Measure the market reach of the national carrier.
SELECT
    SUM(pasajeros) AS total_passengers_aerolineas_arg
FROM
    aeropuerto_tabla
WHERE
	aerolinea_nombre = 'AEROLINEAS ARGENTINAS SA'
	AND fecha >= '2021-01-01'
	AND fecha <= '2022-06-30'
	AND tipo_de_movimiento = 'Despegue';

-- Query 3: Detailed flight log with Origin/Destination cities
-- Business Goal: Enriched report linking flight codes to human-readable locations.
SELECT
    t1.fecha,
    t1.horautc,
    t1.origen_destino  AS cod_salida,
    d_salida.ref  AS ciudad_salida,
    t1.aeropuerto AS cod_arribo,
    d_arribo.ref AS ciudad_arribo,
    t1.pasajeros
FROM
    aereo.aeropuerto_tabla t1
LEFT JOIN
    aereo.aeropuerto_detalles_tabla d_salida
ON
    t1.origen_destino  = d_salida.aeropuerto
LEFT JOIN
    aereo.aeropuerto_detalles_tabla d_arribo
ON
    t1.aeropuerto = d_arribo.aeropuerto
WHERE
    t1.fecha >= '2022-01-01'
    AND t1.fecha <= '2022-06-30'
    AND tipo_de_movimiento = 'Despegue'
ORDER BY
    t1.fecha DESC;

-- Query 4: Top 10 Airlines by passenger volume
-- Business Goal: Identify market leaders while filtering out invalid records.
SELECT
    t1.aerolinea_nombre,
    SUM(t1.pasajeros) AS total_pasajeros
FROM
    aereo.aeropuerto_tabla t1
WHERE
    t1.tipo_de_movimiento = 'Despegue' 
    AND t1.aerolinea_nombre IS NOT NULL
    AND TRIM(t1.aerolinea_nombre) <> ''
    AND t1.aerolinea_nombre <> '0'
    AND t1.pasajeros > 0
GROUP BY
    t1.aerolinea_nombre
ORDER BY
    total_pasajeros DESC
LIMIT 10;

-- Query 5: Top 10 Aircraft models departing from Buenos Aires region
-- Business Goal: Analyze the most common fleet used in the country's main hub.
SELECT
    t1.aeronave,
    COUNT(*) AS usos
FROM
    aereo.aeropuerto_tabla t1
LEFT JOIN
    aereo.aeropuerto_detalles_tabla d_salida
ON
    t1.origen_destino = d_salida.aeropuerto
WHERE
    t1.tipo_de_movimiento = 'Despegue' 
    AND (d_salida.provincia) IN ('CIUDAD AUTÓNOMA DE BUENOS AIRES', 'BUENOS AIRES')
    AND t1.aeronave IS NOT NULL
    AND TRIM(t1.aeronave) <> ''
GROUP BY
    t1.aeronave
ORDER BY
    usos DESC
LIMIT 10;

