import sqlite3
import time
import random

def execute_daily_production(cursor, current_day):
    print(f"   [Dynamic Production Engine] Querying active floor lines...")
    
    # 1. Fetch station_id along with the other line details!
    cursor.execute("""
        SELECT station_id, station_name, input_material_id, daily_consumption_rate, target_product_family 
        FROM production_stations;
    """)
    active_stations = cursor.fetchall()
    
    # This dictionary dynamically compiles the cumulative daily outputs for financials
    daily_yields = {
        "Microprocessors": {"Grade-A": 0, "Grade-B": 0, "Scrap": 0},
        "Mesh": 0,
        "Substrates": 0
    }
    
    # Mapping table IDs to clean product IDs for standard tracks
    product_id_map = {
        "Mesh": 4,             # Anodized Interconnect Mesh
        "Etch Substrates": 5   # Patterned Etch Substrates
    }
    
    for station_id, station_name, material_id, consumption_rate, product_family in active_stations:
        
        # 2. Check stock using the input_material_id integer directly
        cursor.execute(
            "SELECT current_stock, material_name FROM raw_materials WHERE material_id = ?;", 
            (material_id,)
        )
        mat_result = cursor.fetchone()
        
        if mat_result is None or mat_result[0] < consumption_rate:
            print(f"      [ALERT] {station_name} starved! Insufficient material ID {material_id}.")
            continue
            
        current_stock = mat_result[0]
        material_name = mat_result[1]
        new_stock = current_stock - consumption_rate
        
        # 3. Deduct stock from inventory
        cursor.execute(
            "UPDATE raw_materials SET current_stock = ? WHERE material_id = ?;", 
            (new_stock, material_id)
        )
        
        # 4. Route processing rules based on target_product_family
        if product_family == "Microprocessor XL":
            grade_a = 0
            grade_b = 0
            scrap = 0
            
            # Roll quality bins for every unit processed by this specific line
            for _ in range(consumption_rate):
                roll = random.random()
                if roll < 0.70:
                    grade_a += 1
                elif roll < 0.90:
                    grade_b += 1
                else:
                    scrap += 1
            
            # Insert the three separate split records into your ledger, including station_id
            production_runs = [(1, grade_a), (2, grade_b), (3, scrap)]
            for prod_id, qty in production_runs:
                if qty > 0:
                    cursor.execute(
                        """INSERT INTO daily_production_ledger 
                           (simulation_day, station_id, product_id, units_produced, materials_consumed) 
                           VALUES (?, ?, ?, ?, ?);""",
                        (current_day, station_id, prod_id, qty, consumption_rate)
                    )
            
            print(f"      -> {station_name}: Processed {consumption_rate} {material_name}. "
                  f"Yield: [{grade_a} A | {grade_b} B | {scrap} Scrap]")
            
            # Accumulate into global ledger dictionary for the financial step
            daily_yields["Microprocessors"]["Grade-A"] += grade_a
            daily_yields["Microprocessors"]["Grade-B"] += grade_b
            daily_yields["Microprocessors"]["Scrap"] += scrap
            
        else:
            # Handle standard tracks ("Mesh" or "Etch Substrates") dynamically
            successful_units = 0
            for _ in range(consumption_rate):
                if random.random() < 0.90: # 90% yield rule
                    successful_units += 1
                    
            target_prod_id = product_id_map.get(product_family)
            
            # Insert standard run, including station_id
            if successful_units > 0 and target_prod_id:
                cursor.execute(
                    """INSERT INTO daily_production_ledger 
                       (simulation_day, station_id, product_id, units_produced, materials_consumed) 
                       VALUES (?, ?, ?, ?, ?);""",
                    (current_day, station_id, target_prod_id, successful_units, consumption_rate)
                )
                
            print(f"      -> {station_name}: Processed {consumption_rate} {material_name}. "
                  f"Yield: {successful_units} units")
            
            # Accumulate totals for the financial step
            if product_family == "Mesh":
                daily_yields["Mesh"] += successful_units
            elif product_family == "Etch Substrates":
                daily_yields["Substrates"] += successful_units
                
    return daily_yields


def execute_daily_financials(cursor, current_day, daily_yields):
    print(f"   [Financial Ledger] Reconciling accounts for Day {current_day}...")
    total_revenue_today = 0.0
    
    chips = daily_yields.get("Microprocessors", {"Grade-A": 0, "Grade-B": 0, "Scrap": 0})
    mesh_qty = daily_yields.get("Mesh", 0)
    substrate_qty = daily_yields.get("Substrates", 0)
    
    sales_manifest = [
        (1, chips.get("Grade-A", 0), 1000.00),
        (2, chips.get("Grade-B", 0), 650.00),
        (3, chips.get("Scrap", 0), 0.00),
        (4, mesh_qty, 400.00),
        (5, substrate_qty, 250.00)
    ]
    
    for product_id, qty, unit_price in sales_manifest:
        if qty > 0:
            line_item_total = qty * unit_price
            total_revenue_today += line_item_total
            
            cursor.execute(
                """INSERT INTO sales_ledger 
                   (simulation_day, product_id, units_sold, revenue_generated) 
                   VALUES (?, ?, ?, ?);""",
                (current_day, product_id, qty, line_item_total)
            )
            
    print(f"      -> Financial Summary: Generated ${total_revenue_today:,.2f} in gross revenue today.")
    return total_revenue_today


def run_factory_simulation(total_days):
    print("--- INITIALIZING FACTORY SIMULATION ENGINE ---")
    
    try:
        # Open connection at the start of the simulation
        conn = sqlite3.connect("factory_logistics.db")
        cursor = conn.cursor()
        
        # --- AUTOMATIC SEED RESET FOR TESTING ---
        print("[Reset] Seeding fresh inventory for test run...")
        cursor.execute("UPDATE raw_materials SET current_stock = 2000 WHERE material_name = 'Silicon Wafers';")
        cursor.execute("UPDATE raw_materials SET current_stock = 2000 WHERE material_name = 'Copper Anodes';")
        cursor.execute("UPDATE raw_materials SET current_stock = 2000 WHERE material_name = 'Photoresist Polymer';")
    
        # Wipe the ledgers from previous test runs
        cursor.execute("DELETE FROM daily_production_ledger;")
        cursor.execute("DELETE FROM sales_ledger;")
        cursor.execute("DELETE FROM financial_ledger;") # Clear out old financial histories
    
        # Insert Day 0 starting balance row
        cursor.execute("""
            INSERT INTO financial_ledger (simulation_day, transaction_type, amount, current_balance)
            VALUES (0, 'Initial Seed', 100000.00, 100000.00);
        """)
        conn.commit()
        print("[Reset] Database primed with $100,000.00 capital. Starting simulation loop.\n")

        # This is our main timeline loop
        for current_day in range(1, total_days + 1):
            print(f"\n================ SYSTEM CLOCK: DAY {current_day} ================")
            
            # 0. FETCH PREVIOUS DAY'S ENDING BALANCE
            # We sort by transaction_id descending to get the absolute latest balance row in the table
            cursor.execute("""
                SELECT current_balance FROM financial_ledger 
                ORDER BY transaction_id DESC LIMIT 1;
            """)
            starting_cash = cursor.fetchone()[0]
            
            # 1. RUN PRODUCTION
            print(f"[Log] Step 1: Processing materials at production stations...")
            daily_output = execute_daily_production(cursor, current_day)
            
            # 2. RECONCILE FINANCIALS
            print(f"[Log] Step 2: Simulating daily sales and updating ledgers...")
            day_revenue = execute_daily_financials(cursor, current_day, daily_output)
            
            # Update running cash with today's sales revenue
            running_cash = starting_cash + day_revenue
            
            # Log the Revenue transaction if money was made
            if day_revenue > 0:
                cursor.execute("""
                    INSERT INTO financial_ledger (simulation_day, transaction_type, amount, current_balance)
                    VALUES (?, 'Revenue', ?, ?);
                """, (current_day, day_revenue, running_cash))
            
            # (Expenses transaction placeholder - currently 0.0)
            day_expenses = 0.0
            if day_expenses > 0:
                running_cash -= day_expenses
                cursor.execute("""
                    INSERT INTO financial_ledger (simulation_day, transaction_type, amount, current_balance)
                    VALUES (?, 'Expense', ?, ?);
                """, (current_day, day_expenses, running_cash))
                
            print(f"   [Financial Ledger] Day {current_day} Closed. Vault Balance: ${running_cash:,.2f}")
            
            # 3. CONSIDER CAPITAL REINVESTMENT
            print(f"[Log] Step 3: Considering capital reinvestment, adding new production lines...")                         
            # A tiny pause just so it feels like a live machine running in your terminal
            time.sleep(2.5) 
            
        print("\n==================================================")
        print("--- SIMULATION COMPLETE: ALL DAYS PROCESSED ---")
        
        # Safely save changes and close when the whole simulation is done
        conn.commit()
        conn.close()
        
    except sqlite3.Error as error:
        print(f"\n[CRITICAL ERROR] Database failure: {error}")

# Trigger a 5-day test run
run_factory_simulation(total_days=5)