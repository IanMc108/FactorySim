BEGIN TRANSACTION;
DROP TABLE IF EXISTS "daily_production_ledger";
CREATE TABLE "daily_production_ledger" (
	"entry_id"	INTEGER,
	"simulation_day"	INTEGER NOT NULL,
	"station_id"	INTEGER NOT NULL,
	"product_id"	INTEGER NOT NULL,
	"units_produced"	INTEGER NOT NULL,
	"materials_consumed"	INTEGER NOT NULL,
	PRIMARY KEY("entry_id" AUTOINCREMENT),
	FOREIGN KEY("product_id") REFERENCES "products"("product_id"),
	FOREIGN KEY("station_id") REFERENCES "production_stations"("station_id")
);
DROP TABLE IF EXISTS "financial_ledger";
CREATE TABLE "financial_ledger" (
	"transaction_id"	INTEGER,
	"simulation_day"	INTEGER NOT NULL,
	"transaction_type"	TEXT NOT NULL,
	"amount"	REAL NOT NULL,
	"current_balance"	REAL NOT NULL,
	PRIMARY KEY("transaction_id" AUTOINCREMENT)
);
DROP TABLE IF EXISTS "production_stations";
CREATE TABLE "production_stations" (
	"station_id"	INTEGER,
	"station_name"	TEXT NOT NULL,
	"input_material_id"	INTEGER,
	"daily_consumption_rate"	INTEGER,
	"target_product_family"	TEXT NOT NULL,
	PRIMARY KEY("station_id" AUTOINCREMENT),
	FOREIGN KEY("input_material_id") REFERENCES "raw_materials"("material_id")
);
DROP TABLE IF EXISTS "products";
CREATE TABLE "products" (
	"product_id"	INTEGER,
	"product_name"	TEXT NOT NULL,
	"product_family"	TEXT NOT NULL,
	"quality_grade"	TEXT NOT NULL,
	"market_value"	REAL NOT NULL,
	PRIMARY KEY("product_id" AUTOINCREMENT)
);
DROP TABLE IF EXISTS "raw_materials";
CREATE TABLE "raw_materials" (
	"material_id"	INTEGER,
	"material_name"	TEXT NOT NULL,
	"unit_cost"	REAL NOT NULL,
	"current_stock"	INTEGER NOT NULL,
	"lead_time"	INTEGER DEFAULT 5,
	PRIMARY KEY("material_id" AUTOINCREMENT)
);
DROP TABLE IF EXISTS "sales_ledger";
CREATE TABLE "sales_ledger" (
	"sale_id"	INTEGER,
	"simulation_day"	INTEGER NOT NULL,
	"product_id"	INTEGER NOT NULL,
	"units_sold"	INTEGER NOT NULL,
	"revenue_generated"	REAL NOT NULL,
	PRIMARY KEY("sale_id" AUTOINCREMENT),
	FOREIGN KEY("product_id") REFERENCES "products"("product_id")
);
COMMIT;
