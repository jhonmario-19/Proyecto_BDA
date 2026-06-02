-- gold_dim_vendor.sql
-- Dimension de proveedores de taxi (tabla estatica: 5 filas)
-- Fuente: documentacion oficial del TLC de NYC
-- vendor_id -1 captura registros donde vendor_id fue nulo en Silver
-- y fue reemplazado con COALESCE(vendor_id, -1)

{{
    config(
        materialized='table',
        schema='gold',
        file_format='delta',
        tags=['gold', 'dimension', 'static']
    )
}}

SELECT
    vendor_id,
    vendor_name,
    vendor_description
FROM (
    VALUES
        (-1, 'Desconocido',                   'Proveedor no registrado o nulo en origen'),
        ( 1, 'Creative Mobile Technologies',  'CMT — proveedor autorizado TLC NYC'),
        ( 2, 'VeriFone Inc.',                 'VTS — proveedor autorizado TLC NYC'),
        ( 3, 'Otro proveedor A',              'Proveedor menor autorizado TLC NYC'),
        ( 4, 'Otro proveedor B',              'Proveedor menor autorizado TLC NYC')
) AS t(vendor_id, vendor_name, vendor_description)
