import psycopg2

# Connect to your database
conn = psycopg2.connect(
    host="localhost",
    database="globalmart_dw",
    user="postgres",
    password="1"
)
cur = conn.cursor()

# Drop all tables in the public schema
cur.execute("""
DO $$ DECLARE
    r RECORD;
BEGIN
    FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = 'public') LOOP
        EXECUTE 'DROP TABLE IF EXISTS public.' || quote_ident(r.tablename) || ' CASCADE';
    END LOOP;
END $$;
""")

conn.commit()
cur.close()
conn.close()

print("✓ All tables in the database have been dropped.")
