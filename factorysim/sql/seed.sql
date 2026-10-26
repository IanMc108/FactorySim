-- Reference data for the default factory. Opening stock is written to
-- inventory_movements by the simulator so the movement ledger is complete.

INSERT INTO raw_materials
    (material_id, material_name, unit_cost, current_stock, lead_time_days, reorder_point, order_quantity)
VALUES
    (1, 'Silicon Wafers',      50.00, 2000, 5, 2400, 1500),
    (2, 'Copper Anodes',       12.00, 2000, 3,  200, 1875),
    (3, 'Photoresist Polymer', 25.00, 2000, 4, 1200, 1200);

INSERT INTO products
    (product_id, sku, product_name, product_family, quality_grade, unit_price, base_yield)
VALUES
    (1, 'MPU-XL-A',     'Microprocessor Die XL (Grade A)', 'Microprocessor XL', 'A',        1000.00, 0.70),
    (2, 'MPU-XL-B',     'Microprocessor Die XL (Grade B)', 'Microprocessor XL', 'B',         650.00, 0.20),
    (3, 'MPU-XL-SCRAP', 'Microprocessor Die XL (Scrap)',   'Microprocessor XL', 'Scrap',       0.00, 0.10),
    (4, 'MESH-AIM',     'Anodized Interconnect Mesh',      'Mesh',              'Standard',  400.00, 0.90),
    (5, 'MESH-SCRAP',   'Interconnect Mesh (Scrap)',       'Mesh',              'Scrap',       0.00, 0.10),
    (6, 'SUB-PE',       'Patterned Etch Substrate',        'Etch Substrates',   'Standard',  250.00, 0.90),
    (7, 'SUB-SCRAP',    'Etch Substrate (Scrap)',          'Etch Substrates',   'Scrap',       0.00, 0.10);

INSERT INTO production_stations
    (station_id, station_name, input_material_id, daily_input_units, product_family)
VALUES
    (1, 'Lithography Line A',    1, 150, 'Microprocessor XL'),
    (2, 'Lithography Line B',    1, 150, 'Microprocessor XL'),
    (3, 'Electroplating Cell A', 2,  20, 'Mesh'),
    (4, 'Electroplating Cell B', 2,  20, 'Mesh'),
    (5, 'Chemical Track A',      3, 100, 'Etch Substrates'),
    (6, 'Chemical Track B',      3, 100, 'Etch Substrates');
