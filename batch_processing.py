from pyspark.sql.functions import col, current_timestamp, unix_timestamp
import psycopg2
from psycopg2.extras import execute_values
from psycopg2 import sql
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
from pyspark.sql.functions import (
    from_json, col, window, avg, count, current_timestamp,
    expr, lit, explode, min
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, TimestampType, ArrayType,IntegerType
)
from threading import Thread
import time
import json
import shutil, os
from pyspark.sql.functions import explode

from config import PATHS

def create_spark_session():
    """Create Spark session"""
    return SparkSession.builder \
        .appName("Global Mart Backend") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
        .getOrCreate()
def get_or_create_database(
    dbname="globalmart_dw",
    user="postgres",
    password="1",
    host="localhost",
    port="5432"
):
    try:
        conn = psycopg2.connect(
            dbname=dbname, user=user, password=password, host=host, port=port
        )
        conn.autocommit = True
        print(f"Connected to existing database: {dbname}")
        return conn

    except psycopg2.OperationalError as e:
        if "does not exist" in str(e):
            print(f"Database '{dbname}' not found. Creating it...")
            conn = psycopg2.connect(
                dbname="postgres", user=user, password=password, host=host, port=port
            )
            conn.autocommit = True
            cur = conn.cursor()
            cur.execute(sql.SQL("CREATE DATABASE {}").format(
                sql.Identifier(dbname)
            ))
            print(f" Created database: {dbname}")

            cur.close()
            conn.close()
            conn = psycopg2.connect(
                dbname=dbname, user=user, password=password, host=host, port=port
            )
            conn.autocommit = True
            return conn
        else:
            raise e
def create_tables(cur):
    cur.execute("""
    CREATE TABLE IF NOT EXISTS Dim_User (
        Customer_Key SERIAL PRIMARY KEY,
        User_id VARCHAR(50) NOT NULL UNIQUE,
        Email VARCHAR(255),
        Age INT,
        Country VARCHAR(50),
        Registration_Date TIMESTAMP
        );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS Dim_UserPref (
        Preference_Key SERIAL PRIMARY KEY,
        Preference_Name VARCHAR(100) UNIQUE
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS User_Pref_Bridge (
            Customer_Key INT REFERENCES Dim_User(Customer_Key),
            Preference_Key INT REFERENCES Dim_UserPref(Preference_Key),
            PRIMARY KEY (Customer_Key, Preference_Key)
        );
    """)
    # cur.execute("""
    #     SELECT conname, contype, conkey
    # FROM pg_constraint
    # WHERE conrelid = 'dim_product'::regclass;
    # """)
    # constraints = cur.fetchall()
    # # Print each row
    # for row in constraints:
    #     print(row)
    # cur.execute("ALTER TABLE dim_product DROP CONSTRAINT dim_product_product_id_key;")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS Dim_Product (
        Product_Key SERIAL PRIMARY KEY,
        Product_id VARCHAR(50) NOT NULL,
        Category VARCHAR(50),
        Price DOUBLE PRECISION,
        Inventory INT,
        valid_from TIMESTAMP NOT NULL,
        valid_to TIMESTAMP,
        is_current BOOLEAN NOT NULL DEFAULT TRUE,
        UNIQUE (Product_id, valid_from)
        );
    """)
    # cur.execute("""
    #     CREATE UNIQUE INDEX IF NOT EXISTS u_product_current
    #     ON Dim_Product (Product_id)
    #     WHERE is_current = TRUE;
    # """)
    # cur.execute("""
    # ALTER TABLE Dim_Product
    # ALTER COLUMN Product_id TYPE VARCHAR(50);
    # """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS Dim_Date (
        Date_Key INT PRIMARY KEY,
        Year INT NOT NULL,
        Month INT NOT NULL,
        Day INT NOT NULL
        );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS Dim_Time (
        Time_Key INT PRIMARY KEY,
        Hour INT NOT NULL,
        Minute INT NOT NULL,
        Second INT NOT NULL
        );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS Fact_ProductView (
        View_Key SERIAL PRIMARY KEY,
        Event_id VARCHAR(50) NOT NULL UNIQUE,
        Customer_Key INT NOT NULL REFERENCES Dim_User(Customer_Key),
        User_id VARCHAR(50) NOT NULL,
        Product_Key INT NOT NULL REFERENCES Dim_Product(Product_Key),
        Product_id VARCHAR(50) NOT NULL,
        Date_Key INT NOT NULL REFERENCES Dim_Date(Date_Key),
        Time_Key INT NOT NULL REFERENCES Dim_Time(Time_Key),
        Timestamp TIMESTAMP NOT NULL
        );
    """)
    # cur.execute("""
    # ALTER TABLE Fact_ProductView
    # ALTER COLUMN Event_id TYPE VARCHAR(50);       
    # """)
    # cur.execute("""
    # ALTER TABLE Fact_ProductView
    # ALTER COLUMN User_id TYPE VARCHAR(50);       
    # """)
    # cur.execute("""
    # ALTER TABLE Fact_ProductView
    # ALTER COLUMN Product_id TYPE VARCHAR(50);       
    # """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS Fact_TransactionEvent (
        Transaction_Key SERIAL PRIMARY KEY,
        Transaction_id VARCHAR(50) NOT NULL,
        Customer_Key INT NOT NULL REFERENCES Dim_User(Customer_Key),
        Product_Key INT NOT NULL REFERENCES Dim_Product(Product_Key),
        Date_Key INT NOT NULL REFERENCES Dim_Date(Date_Key),
        Time_Key INT NOT NULL REFERENCES Dim_Time(Time_Key),
        Timestamp TIMESTAMP NOT NULL,
        Quantity INT,
        Line_amount DOUBLE PRECISION,
        Payment_method VARCHAR(50),
        Price DOUBLE PRECISION,
        UNIQUE (transaction_id, product_key)
        );
    """)
    
    # cur.execute("""
    # DELETE FROM fact_transactionevent a
    # USING fact_transactionevent b
    # WHERE a.transaction_id = b.transaction_id
    # AND a.product_key = b.product_key
    # AND a.ctid < b.ctid;
    # ALTER TABLE fact_transactionevent
    # ADD CONSTRAINT uniq_transaction_product UNIQUE (transaction_id, product_key);       
    # """)
    # cur.execute("""
    # ALTER TABLE Fact_TransactionEvent
    # ADD CONSTRAINT uniq_transaction_product UNIQUE (transaction_id, product_key);       
    # """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS Fact_CartEvent (
        CartEvent_Key SERIAL PRIMARY KEY,
        Cart_id VARCHAR(50) NOT NULL,
        Customer_Key INT NOT NULL REFERENCES Dim_User(Customer_Key),
        Product_Key INT NOT NULL REFERENCES Dim_Product(Product_Key),
        Date_Key INT NOT NULL REFERENCES Dim_Date(Date_Key),
        Time_Key INT NOT NULL REFERENCES Dim_Time(Time_Key),
        Timestamp TIMESTAMP NOT NULL,
        Quantity INT,
        Price DOUBLE PRECISION,
        UNIQUE (cart_id, product_key)
        );
    """)
    # cur.execute("""
    # DELETE FROM Fact_CartEvent a
    # USING Fact_CartEvent b
    # WHERE a.cart_id = b.cart_id
    # AND a.product_key = b.product_key
    # AND a.ctid < b.ctid;
    # ALTER TABLE Fact_CartEvent
    # ADD CONSTRAINT uniq_cart_product UNIQUE (cart_id, product_key);          
    # """)
    # cur.execute("""
    # ALTER TABLE Fact_CartEvent
    # ALTER COLUMN Cart_id TYPE VARCHAR(50);       
    # """)
def print_row_count(cur, table_name):
    cur.execute(f"SELECT COUNT(*) FROM {table_name};")
    count = cur.fetchone()[0]
    print(f"\nTable: {table_name} Row count: {count}")
def print_table_metadata(cur):
    cur.execute("""
        SELECT
            t.table_name,
            c.column_name,
            c.data_type,
            c.is_nullable,
            tc.constraint_name,
            tc.constraint_type,
            kcu.column_name AS constraint_column,
            ccu.table_name AS foreign_table,
            ccu.column_name AS foreign_column
        FROM information_schema.tables t
        LEFT JOIN information_schema.columns c
            ON t.table_name = c.table_name AND t.table_schema = c.table_schema
        LEFT JOIN information_schema.table_constraints tc
            ON t.table_name = tc.table_name AND t.table_schema = tc.table_schema
        LEFT JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
        LEFT JOIN information_schema.constraint_column_usage ccu
            ON tc.constraint_name = ccu.constraint_name AND tc.table_schema = ccu.table_schema
        WHERE t.table_schema = 'public' AND t.table_type = 'BASE TABLE'
        ORDER BY t.table_name, c.ordinal_position;
    """)
    rows = cur.fetchall()
    from collections import defaultdict
    tables = defaultdict(lambda: {"columns": [], "constraints": []})
    for row in rows:
        table_name, col_name, col_type, is_null, cons_name, cons_type, cons_col, f_table, f_col = row
        if col_name:
            tables[table_name]["columns"].append((col_name, col_type, is_null))
        if cons_name:
            tables[table_name]["constraints"].append((cons_name, cons_type, cons_col, f_table, f_col))
    for table, info in tables.items():
        print("\n" + "="*60)
        print(f"Table: {table}")
        print("\nColumns:")
        for col_name, col_type, is_null in info["columns"]:
            print(f"  {col_name} | {col_type} | nullable: {is_null}")
        if info["constraints"]:
            print("\nConstraints:")
            for cons_name, cons_type, cons_col, f_table, f_col in info["constraints"]:
                if cons_type in ["FOREIGN KEY"]:
                    print(f"  {cons_type} ({cons_col}) -> {f_table}.{f_col}")
                else:
                    print(f"  {cons_type}: {cons_col}")
        else:
            print("\nConstraints: None")
def safe_parse_date(date_value):
    try:
        if isinstance(date_value, datetime):
            return date_value
        else:
            return datetime.fromisoformat(date_value)
    except (ValueError, TypeError):
        return None
def products_scd(cur, products_df):
    rows = products_df.collect()
    row_count = 0
    conflicted_count = 0
    for row in rows:
        cur.execute("""
            INSERT INTO dim_product (product_id, category, price, inventory, valid_from, valid_to, is_current)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (product_id, valid_from) DO NOTHING
            RETURNING Product_Key;
        """, (
            row.product_id,
            row.category,
            row.price,
            row.inventory,
            safe_parse_date(row.valid_from),
            None,
            True
        ))
        result = cur.fetchone()
        if result:
            row_count += 1
        else:
            conflicted_count += 1
    print(f" Conflicted {conflicted_count} product records")
    print(f" Inserted {row_count} product records")
def update_product_inventory_from_transactions(cur, transactions_df):
    rows = transactions_df.collect()
    updated = 0
    skipped = 0
    for trx in rows:
        trx_time = safe_parse_date(trx.timestamp)
        for product in trx.products:  
            cur.execute("""
                SELECT product_key, inventory, category, price, valid_from
                FROM dim_product
                WHERE product_id = %s AND is_current = TRUE;
            """, (product.product_id,))
            current = cur.fetchone()
            if not current:
                skipped += 1
                continue
            product_key, current_inv, category, price, current_valid_from = current
            new_inv = current_inv - product.quantity
            if new_inv == current_inv or trx_time <= current_valid_from:
                skipped += 1
                continue
            cur.execute("""
                UPDATE dim_product
                SET valid_to = %s, is_current = FALSE
                WHERE product_id = %s AND is_current = TRUE;
            """, (trx_time - timedelta(seconds=1), product.product_id))
            cur.execute("""
                INSERT INTO dim_product (product_id, category, price, inventory, valid_from, valid_to, is_current)
                VALUES (%s, %s, %s, %s, %s, NULL, TRUE);
            """, (product.product_id, category, price, new_inv, trx_time))

            updated += 1

    print(f"SCD2 Updates: Updated={updated}, Skipped={skipped}")

def users_insert(cur, users_df):
    rows = users_df.collect()
    row_count = 0
    conflicted_count = 0
    for row in rows:
        cur.execute("""
            INSERT INTO dim_user (user_id, email, age, country, registration_date)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (user_id) DO NOTHING
            RETURNING Customer_Key;
        """, (
            row.user_id,
            row.email,
            row.age,
            row.country,
            safe_parse_date(row.registration_date)
        ))
        result = cur.fetchone()
        if result:
            row_count += 1
        else:
            conflicted_count += 1
    print(f" Conflicted {conflicted_count} user records.")
    print(f" Inserted {row_count} user records.")
def user_prefs_insert(cur, users_df):
    all_prefs = set()
    for row in users_df.collect():
        for pref in row.preferences:
            all_prefs.add(pref)
    pref_count = 0
    conflict_count = 0
    for pref in all_prefs:
        cur.execute("""
            INSERT INTO dim_userpref (preference_name)
            VALUES (%s)
            ON CONFLICT (preference_name) DO NOTHING
                    RETURNING Preference_Key;
        """, (pref,))
        result = cur.fetchone()
        if result:
            pref_count += 1
        else:
            conflict_count += 1
    print(f" Conflicted {conflict_count} user preference records.")
    print(f" Inserted {pref_count} user preference records.")
def user_pref_bridge_insert(cur, users_df):
    cur.execute("SELECT user_id, Customer_Key FROM Dim_User;")
    user_map = {u: k for u, k in cur.fetchall()}
    cur.execute("SELECT preference_name, Preference_Key FROM Dim_UserPref;")
    pref_map = {p: k for p, k in cur.fetchall()}
    inserted_count = 0
    conflict_count = 0
    for row in users_df.collect():
        ckey = user_map.get(row.user_id)
        if ckey:
            for pref in row.preferences:
                pkey = pref_map.get(pref)
                if pkey:
                    cur.execute("""
                        INSERT INTO User_Pref_Bridge (Customer_Key, Preference_Key)
                        VALUES (%s, %s)
                        ON CONFLICT (Customer_Key, Preference_Key) DO NOTHING
                        RETURNING Customer_Key;
                    """, (ckey, pkey))
                    result = cur.fetchone()
                    if result:
                        inserted_count += 1
                    else:
                        conflict_count += 1
    print(f" Conflicted {conflict_count} user-preference bridge records.")
    print(f" Inserted {inserted_count} user-preference bridge records.")
def date_dim_insert(cur, timestamp):
    date_key = int(timestamp.strftime("%Y%m%d"))
    year = timestamp.year
    month = timestamp.month
    day = timestamp.day
    cur.execute("""
        INSERT INTO Dim_Date (Date_Key, Year, Month, Day)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (Date_Key) DO NOTHING
        RETURNING Date_Key;
    """, (date_key, year, month, day))
    result = cur.fetchone()
    # if result:
    #     print(f"Inserted new date dimension record for Date_Key: {date_key}")
    # else:
    #     print(f"Date dimension record for Date_Key: {date_key} already exists.")
    return date_key
def time_dim_insert(cur, timestamp):
    time_key = int(timestamp.strftime("%H%M%S"))
    hour = timestamp.hour
    minute = timestamp.minute
    second = timestamp.second
    cur.execute("""
        INSERT INTO Dim_Time (Time_Key, Hour, Minute, Second)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (Time_Key) DO NOTHING
        RETURNING Time_Key;
    """, (time_key, hour, minute, second))
    result = cur.fetchone()
    # if result:
    #     print(f"Inserted new time dimension record for Time_Key: {time_key}")
    # else:
    #     print(f"Time dimension record for Time_Key: {time_key} already exists.")
    
    return time_key
def fact_product_view_insert(cur, views_df):
    rows = views_df.collect()
    inserted_count = 0
    skipped_count = 0
    conflict_count = 0
    for row in rows:
        date_key = date_dim_insert(cur, safe_parse_date(row.timestamp))
        time_key = time_dim_insert(cur, safe_parse_date(row.timestamp))
        cur.execute("SELECT customer_key FROM dim_user WHERE user_id = %s", (row.user_id,))
        customer_row = cur.fetchone()
        customer_key = customer_row[0] if customer_row else None

        # Lookup product_key from dim_product
        cur.execute("SELECT product_key FROM dim_product WHERE product_id = %s", (row.product_id,))
        product_row = cur.fetchone()
        product_key = product_row[0] if product_row else None

        # Skip if keys are missing (optional)
        if customer_key is None or product_key is None or safe_parse_date(row.timestamp) is None:
            skipped_count += 1
            continue

        cur.execute("""
            INSERT INTO Fact_ProductView (event_id, customer_key, user_id, product_key, product_id, date_key, time_key, timestamp)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (event_id) DO NOTHING
            RETURNING event_id;
        """, (
            row.event_id,
            customer_key,
            row.user_id,
            product_key,
            row.product_id,
            date_key,
            time_key,
            safe_parse_date(row.timestamp)
        ))
        result = cur.fetchone()
        if result:
            inserted_count += 1
        else:
            conflict_count += 1
    print(f"Inserted {inserted_count} product view event fact records.")
    print(f"Conflicted {conflict_count} product view event fact records.")
    print(f"Skipped {skipped_count} product view event fact records.")
def fact_transaction_event_insert(cur, transactions_df):
    rows = transactions_df.collect()
    inserted_count = 0
    skipped_count = 0
    conflict_count = 0
    for row in rows:
        date_key = date_dim_insert(cur, safe_parse_date(row.timestamp))
        time_key = time_dim_insert(cur, safe_parse_date(row.timestamp))
        cur.execute("SELECT customer_key FROM dim_user WHERE user_id = %s", (row.user_id,))
        customer_row = cur.fetchone()
        customer_key = customer_row[0] if customer_row else None

        # Lookup product_key from dim_product
        for product in row.products:
            cur.execute("SELECT product_key FROM dim_product WHERE product_id = %s", (product.product_id,))
            product_row = cur.fetchone()
            product_key = product_row[0] if product_row else None

            # Skip if keys are missing (optional)
            if customer_key is None or product_key is None or safe_parse_date(row.timestamp) is None:
                skipped_count += 1
                continue
            line_amount = product.price * product.quantity
            cur.execute("""
                INSERT INTO Fact_TransactionEvent (transaction_id, customer_key, product_key, date_key, time_key, timestamp, quantity, line_amount, payment_method, price)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (transaction_id, product_key) DO NOTHING
                RETURNING transaction_id;
            """, (
                row.transaction_id,
                customer_key,
                product_key,
                date_key,
                time_key,
                safe_parse_date(row.timestamp),
                product.quantity,
                line_amount,
                row.payment_method,
                product.price
            ))
            result = cur.fetchone()
            if result:
                inserted_count += 1
            else:
                conflict_count += 1
    print(f"Inserted {inserted_count} transaction event fact records.")
    print(f"Conflicted {conflict_count} transaction event fact records.")
    print(f"Skipped {skipped_count} transaction event fact records.")
def fact_cart_event_insert(cur, carts_df):
    rows = carts_df.collect()
    inserted_count = 0
    skipped_count = 0
    conflict_count = 0
    for row in rows:
        date_key = date_dim_insert(cur, safe_parse_date(row.timestamp))
        time_key = time_dim_insert(cur, safe_parse_date(row.timestamp))
        cur.execute("SELECT customer_key FROM dim_user WHERE user_id = %s", (row.user_id,))
        customer_row = cur.fetchone()
        customer_key = customer_row[0] if customer_row else None
        for product in row.products:
            cur.execute("SELECT product_key FROM dim_product WHERE product_id = %s", (product.product_id,))
            product_row = cur.fetchone()
            product_key = product_row[0] if product_row else None
            # Skip if keys are missing (optional)
            if customer_key is None or product_key is None or safe_parse_date(row.timestamp) is None:
                skipped_count += 1
                continue
            cur.execute("""
                INSERT INTO Fact_CartEvent (cart_id, customer_key, product_key, date_key, time_key, timestamp, quantity, price)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (cart_id, product_key) DO NOTHING
                RETURNING cart_id;
            """, (
                row.cart_id,
                customer_key,
                product_key,
                date_key,
                time_key,
                safe_parse_date(row.timestamp),
                product.quantity,
                product.price
            ))
            result = cur.fetchone()
            if result:
                inserted_count += 1
            else:
                conflict_count += 1
    print(f"Inserted {inserted_count} cart event fact records.")
    print(f"Conflicted {conflict_count} cart event fact records.")
    print(f"Skipped {skipped_count} cart event fact records.")
def batch_job(cur):
    rfm_query = """
    WITH customer_rfm AS (
        SELECT
            u.customer_key,
            MAX(f.timestamp) AS last_purchase_date,
            COUNT(f.transaction_id) AS frequency,
            SUM(f.line_amount) AS monetary
        FROM Fact_TransactionEvent f
        JOIN Dim_User u ON f.customer_key = u.customer_key
        JOIN Dim_Date d ON f.date_key = d.date_key
        GROUP BY u.customer_key
    )
    SELECT
        customer_key,
        DATE_PART('day', CURRENT_DATE - last_purchase_date) AS recency_days,
        frequency,
        monetary,
        CASE
            WHEN DATE_PART('day', CURRENT_DATE - last_purchase_date) <= 30 THEN 'Active'
            WHEN DATE_PART('day', CURRENT_DATE - last_purchase_date) <= 90 THEN 'Dormant'
            ELSE 'Churned'
        END AS customer_segment
    FROM customer_rfm;
    """
    cur.execute(rfm_query)
    rfm_results = cur.fetchall()
    print("=== Customer RFM Segmentation ===")
    print("Customer_Key | Recency_Days | Frequency | Monetary | Customer_Segment")
    for row in rfm_results:
        print(row)

    product_perf_query = """
        SELECT
        p.product_id,
        p.category,
        COUNT(f.transaction_id) AS total_transactions,
        SUM(f.quantity) AS total_quantity_sold,
        SUM(f.line_amount) AS total_revenue
    FROM Fact_TransactionEvent f
    JOIN Dim_Product p ON f.product_key = p.product_key
    WHERE f.timestamp::date BETWEEN CURRENT_DATE - INTERVAL '30 days' AND CURRENT_DATE
    GROUP BY p.product_id, p.category
    ORDER BY total_revenue DESC
    LIMIT 20;
    """
    cur.execute(product_perf_query)
    product_perf_results = cur.fetchall()
    print("\n=== Top Product Performance (Last 30 Days) ===")
    print("\nProduct_ID | Category | Total_Transactions | Total_Quantity_Sold | Total_Revenue")
    for row in product_perf_results:
        print(row)
    sales_trend_query = """
    SELECT
        DATE(f.timestamp) as sale_date,
        SUM(f.line_amount) AS daily_sales,
        COUNT(DISTINCT f.transaction_id) AS daily_transactions
    FROM Fact_TransactionEvent f
    JOIN Dim_Date d ON f.date_key = d.date_key
    GROUP BY f.timestamp
    ORDER BY f.timestamp ASC;
    """
    cur.execute(sales_trend_query)
    sales_trend_results = cur.fetchall()
    print("\n=== Daily Sales Trend ===")
    print("\nSale_Date | Daily_Sales | Daily_Transactions")
    for row in sales_trend_results:
        print(row)
    return product_perf_results, sales_trend_results, rfm_results
def ETL_batch():
    print("\n" + "="*60)
    print("Starting ETL Batch Processing Job")
    print("="*60)
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    # Read all parquet files written by your streaming job
    users_df = spark.read.parquet(PATHS['clean_users'])
    views_df = spark.read.parquet(PATHS['clean_views'])
    transactions_df = spark.read.parquet(PATHS['clean_transactions'])
    carts_df = spark.read.parquet(PATHS['clean_carts'])
    products_df = spark.read.parquet(PATHS['clean_products'])
    # Now you can do normal batch ETL
    users_df.printSchema()
    users_df.show(1,truncate=False)
    views_df.printSchema()
    views_df.show(1,truncate=False)
    transactions_df.printSchema()
    transactions_df.show(1,truncate=False)
    carts_df.printSchema()
    carts_df.show(1,truncate=False)
    products_df.printSchema()
    products_df.show(1,truncate=False)
    product_first_seen_df = views_df.groupBy("product_id") \
    .agg(min(col("timestamp")).alias("valid_from"))
    products_df = products_df.join(product_first_seen_df, on="product_id", how="left") \
    .withColumn("valid_to", lit(None).cast("timestamp")) \
    .withColumn("is_current", lit(True))
    products_df = products_df \
    .withColumn("valid_from", col("valid_from").cast("timestamp")) \
    .withColumn("valid_to", col("valid_to").cast("timestamp"))
    conn=get_or_create_database()
    cur=conn.cursor()
    create_tables(cur)
    print_table_metadata(cur)

    products_scd(cur, products_df)
    update_product_inventory_from_transactions(cur, transactions_df)

    users_insert(cur, users_df)
    user_prefs_insert(cur, users_df)
    user_pref_bridge_insert(cur, users_df)
    fact_product_view_insert(cur, views_df)
    fact_transaction_event_insert(cur, transactions_df)
    fact_cart_event_insert(cur, carts_df)
    conn.commit()
    print("\n" + "="*60)
    print("Final Table Row Counts")
    print("="*60)
    tables = [
        "Dim_User",
        "Dim_UserPref",
        "User_Pref_Bridge",
        "Dim_Product",
        "Dim_Date",
        "Dim_Time",
        "Fact_ProductView",
        "Fact_TransactionEvent",
        "Fact_CartEvent"
    ]
    for table in tables:
        print_row_count(cur, table)
    print("\n" + "="*60)
    print("Running Batch Analytics Job")
    print("="*60)
    product_perf_results, sales_trend_results, rfm_results=batch_job(cur)
    #print_table_metadata(cur)
    conn.close()
    cur.close()
if __name__ == "__main__":
    try:
        ETL_batch()
    except KeyboardInterrupt:
        print("\n\nShutting down dashboard...")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
