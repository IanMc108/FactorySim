-- FactorySim source-system schema (SQLite).
-- Reference tables are populated by seed.sql; ledger tables are written by the simulator.

PRAGMA foreign_keys = ON;

-- ---------------------------------------------------------------- reference data

CREATE TABLE raw_materials (
    material_id     INTEGER PRIMARY KEY,
    material_name   TEXT    NOT NULL UNIQUE,
    unit_cost       REAL    NOT NULL CHECK (unit_cost >= 0),
    current_stock   INTEGER NOT NULL CHECK (current_stock >= 0),
    lead_time_days  INTEGER NOT NULL CHECK (lead_time_days >= 0),
    reorder_point   INTEGER NOT NULL CHECK (reorder_point >= 0),
    order_quantity  INTEGER NOT NULL CHECK (order_quantity > 0)
);

-- Every input unit becomes exactly one output unit of some grade, so the
-- base_yield values within a product_family must sum to 1.
CREATE TABLE products (
    product_id      INTEGER PRIMARY KEY,
    sku             TEXT    NOT NULL UNIQUE,
    product_name    TEXT    NOT NULL,
    product_family  TEXT    NOT NULL,
    quality_grade   TEXT    NOT NULL CHECK (quality_grade IN ('A', 'B', 'Standard', 'Scrap')),
    unit_price      REAL    NOT NULL CHECK (unit_price >= 0),
    base_yield      REAL    NOT NULL CHECK (base_yield BETWEEN 0 AND 1)
);

CREATE TABLE production_stations (
    station_id        INTEGER PRIMARY KEY,
    station_name      TEXT    NOT NULL UNIQUE,
    input_material_id INTEGER NOT NULL REFERENCES raw_materials (material_id),
    daily_input_units INTEGER NOT NULL CHECK (daily_input_units > 0),
    product_family    TEXT    NOT NULL
);

-- ---------------------------------------------------------------- ledgers

-- One row per station per day that the station actually ran.
CREATE TABLE production_runs (
    run_id          INTEGER PRIMARY KEY,
    sim_day         INTEGER NOT NULL CHECK (sim_day >= 1),
    station_id      INTEGER NOT NULL REFERENCES production_stations (station_id),
    material_id     INTEGER NOT NULL REFERENCES raw_materials (material_id),
    units_consumed  INTEGER NOT NULL CHECK (units_consumed > 0),
    yield_decay_pp  REAL    NOT NULL DEFAULT 0 CHECK (yield_decay_pp >= 0),
    UNIQUE (sim_day, station_id)
);

-- Graded output of a run. SUM(units) per run equals production_runs.units_consumed.
CREATE TABLE production_output (
    run_id      INTEGER NOT NULL REFERENCES production_runs (run_id),
    product_id  INTEGER NOT NULL REFERENCES products (product_id),
    units       INTEGER NOT NULL CHECK (units >= 0),
    PRIMARY KEY (run_id, product_id)
);

-- Days a station could not run because its input material was short.
CREATE TABLE station_stoppages (
    sim_day     INTEGER NOT NULL,
    station_id  INTEGER NOT NULL REFERENCES production_stations (station_id),
    reason      TEXT    NOT NULL,
    PRIMARY KEY (sim_day, station_id)
);

CREATE TABLE purchase_orders (
    po_id         INTEGER PRIMARY KEY,
    material_id   INTEGER NOT NULL REFERENCES raw_materials (material_id),
    quantity      INTEGER NOT NULL CHECK (quantity > 0),
    unit_cost     REAL    NOT NULL CHECK (unit_cost >= 0),
    order_day     INTEGER NOT NULL,
    due_day       INTEGER NOT NULL,
    received_day  INTEGER,
    CHECK (due_day >= order_day),
    CHECK (received_day IS NULL OR received_day >= due_day)
);

-- Signed stock movements. Opening + SUM(quantity) equals raw_materials.current_stock.
CREATE TABLE inventory_movements (
    movement_id    INTEGER PRIMARY KEY,
    sim_day        INTEGER NOT NULL,
    material_id    INTEGER NOT NULL REFERENCES raw_materials (material_id),
    movement_type  TEXT    NOT NULL CHECK (movement_type IN ('Opening', 'Receipt', 'Consumption')),
    quantity       INTEGER NOT NULL,
    po_id          INTEGER REFERENCES purchase_orders (po_id),
    run_id         INTEGER REFERENCES production_runs (run_id)
);

-- Good output is assumed to sell on the day it is produced, at list price.
CREATE TABLE sales_ledger (
    sale_id     INTEGER PRIMARY KEY,
    sim_day     INTEGER NOT NULL,
    product_id  INTEGER NOT NULL REFERENCES products (product_id),
    units_sold  INTEGER NOT NULL CHECK (units_sold > 0),
    unit_price  REAL    NOT NULL CHECK (unit_price > 0),
    revenue     REAL    NOT NULL,
    UNIQUE (sim_day, product_id)
);

CREATE TABLE maintenance_events (
    event_id  INTEGER PRIMARY KEY,
    sim_day   INTEGER NOT NULL UNIQUE,
    scope     TEXT    NOT NULL,
    cost      REAL    NOT NULL CHECK (cost >= 0)
);

-- Cash ledger with signed amounts (inflows positive, outflows negative).
-- balance_after of each row equals the previous row's balance_after plus amount.
CREATE TABLE financial_ledger (
    transaction_id    INTEGER PRIMARY KEY,
    sim_day           INTEGER NOT NULL,
    transaction_type  TEXT    NOT NULL
        CHECK (transaction_type IN ('Opening Balance', 'Sales Revenue', 'Material Purchase', 'Maintenance')),
    amount            REAL    NOT NULL,
    balance_after     REAL    NOT NULL,
    reference         TEXT
);

CREATE INDEX idx_runs_day ON production_runs (sim_day);
CREATE INDEX idx_sales_day ON sales_ledger (sim_day);
CREATE INDEX idx_fin_day ON financial_ledger (sim_day);
