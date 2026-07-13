-- 1. CLEAN UP EXISTING STRUCTURES
DROP TABLE IF EXISTS sales_ledger;
DROP TABLE IF EXISTS financial_ledger;
DROP TABLE IF EXISTS daily_production_ledger;
DROP TABLE IF EXISTS production_stations;
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS raw_materials;

-- 2. CREATE MASTER TABLES
CREATE TABLE raw_materials (
    material_id INTEGER PRIMARY KEY AUTOINCREMENT,
    material_name TEXT NOT NULL,
    unit_cost REAL NOT NULL,
    current_stock INTEGER NOT NULL,
    lead_time INTEGER DEFAULT 5
);

CREATE TABLE products (
    product_id INTEGER PRIMARY KEY AUTOINCREMENT,
    product_name TEXT NOT NULL,
    product_family TEXT NOT NULL,
    quality_grade TEXT NOT NULL,
    market_value REAL NOT NULL
);

CREATE TABLE production_stations (
    station_id INTEGER PRIMARY KEY AUTOINCREMENT,
    station_name TEXT NOT NULL,
    input_material_id INTEGER,
    daily_consumption_rate INTEGER,
    target_product_family TEXT NOT NULL,
    FOREIGN KEY (input_material_id) REFERENCES raw_materials(material_id)
);

-- 3. CREATE TRACKING LEDGERS
CREATE TABLE daily_production_ledger (
    entry_id INTEGER PRIMARY KEY AUTOINCREMENT,
    simulation_day INTEGER NOT NULL,
    station_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    units_produced INTEGER NOT NULL,
    materials_consumed INTEGER NOT NULL,
    FOREIGN KEY (station_id) REFERENCES production_stations(station_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);

CREATE TABLE financial_ledger (
    transaction_id INTEGER PRIMARY KEY AUTOINCREMENT,
    simulation_day INTEGER NOT NULL,
    transaction_type TEXT NOT NULL,
    amount REAL NOT NULL,
    current_balance REAL NOT NULL
);

CREATE TABLE sales_ledger (
    sale_id INTEGER PRIMARY KEY AUTOINCREMENT,
    simulation_day INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    units_sold INTEGER NOT NULL,
    revenue_generated REAL NOT NULL,
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);

-- 4. POPULATE INITIAL FACTORY DATA
INSERT INTO raw_materials (material_name, unit_cost, current_stock, lead_time) VALUES 
('Silicon Wafers', 50.00, 1000, 5),
('Copper Anodes', 12.00, 500, 3),
('Photoresist Polymer', 25.00, 800, 4);

INSERT INTO products (product_name, product_family, quality_grade, market_value) VALUES 
('Microprocessor Die XL (Grade A)', 'Microprocessor XL', 'A', 1000.00),
('Microprocessor Die XL (Grade B)', 'Microprocessor XL', 'B', 650.00),
('Microprocessor Die XL (Scrap)',   'Microprocessor XL', 'Scrap', 0.00),
('Anodized Interconnect Mesh',       'Mesh',              'Standard', 950.00),
('Patterned Etch Substrates',        'Etch Substrates',   'Standard', 220.00);

INSERT INTO production_stations (station_name, input_material_id, daily_consumption_rate, target_product_family) VALUES 
('Lithography Line A',   1, 150, 'Microprocessor XL'),
('Lithography Line B',   1, 150, 'Microprocessor XL'),
('Electroplating Cell A', 2, 20,  'Mesh'),
('Chemical Track A',     3, 100, 'Etch Substrates'),
('Electroplating Cell B', 2, 20,  'Mesh'),
('Chemical Track B',     3, 100, 'Etch Substrates');