-- Script SQL de diagnostic pour les données de véhicules
-- À exécuter manuellement dans votre client MySQL

-- 1. Vérifier si la table kpi_charges_mac existe
SHOW TABLES LIKE 'kpi_charges_mac';

-- 2. Si la table existe, vérifier sa structure
DESCRIBE kpi_charges_mac;

-- 3. Compter le nombre total de lignes
SELECT COUNT(*) as total_rows FROM kpi_charges_mac;

-- 4. Compter les lignes avec Vehicle non NULL
SELECT
    COUNT(*) as total,
    SUM(CASE WHEN Vehicle IS NULL THEN 1 ELSE 0 END) as null_count,
    SUM(CASE WHEN Vehicle IS NOT NULL AND TRIM(Vehicle) != '' THEN 1 ELSE 0 END) as valid_count
FROM kpi_charges_mac;

-- 5. Voir la distribution des véhicules
SELECT Vehicle, COUNT(*) as count
FROM kpi_charges_mac
WHERE Vehicle IS NOT NULL AND TRIM(Vehicle) != ''
GROUP BY Vehicle
ORDER BY count DESC
LIMIT 20;

-- 6. Vérifier le JOIN entre kpi_sessions et kpi_charges_mac
SELECT
    COUNT(DISTINCT k.`MAC Address`, k.`Datetime start`) as sessions_count,
    COUNT(DISTINCT c.`MAC Address`, c.`Datetime start`) as mac_count,
    SUM(CASE WHEN c.Vehicle IS NOT NULL THEN 1 ELSE 0 END) as joined_with_vehicle
FROM kpi_sessions k
LEFT JOIN kpi_charges_mac c
    ON k.`MAC Address` = c.`MAC Address`
    AND k.`Datetime start` = c.`Datetime start`
LIMIT 10;

-- 7. Exemples de lignes non jointes
SELECT
    k.`MAC Address` as session_mac,
    k.`Datetime start` as session_datetime,
    c.`MAC Address` as mac_mac,
    c.`Datetime start` as mac_datetime,
    c.Vehicle
FROM kpi_sessions k
LEFT JOIN kpi_charges_mac c
    ON k.`MAC Address` = c.`MAC Address`
    AND k.`Datetime start` = c.`Datetime start`
WHERE c.Vehicle IS NULL
LIMIT 10;

-- 8. Vérifier s'il y a un problème de format de datetime
SELECT
    k.`Datetime start` as session_dt,
    c.`Datetime start` as mac_dt,
    k.`MAC Address`,
    c.Vehicle
FROM kpi_sessions k
LEFT JOIN kpi_charges_mac c
    ON k.`MAC Address` = c.`MAC Address`
WHERE c.Vehicle IS NOT NULL
LIMIT 10;
