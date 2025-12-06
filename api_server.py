from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
import psycopg2
from datetime import datetime, timedelta
from typing import Optional
import os

app = FastAPI(title="GlobalMart Analytics API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Database connection
DB_CONFIG = {
    "host": "localhost",
    "database": "globalmart_dw",
    "user": "postgres",
    "password": "1"
}


def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)


@app.get("/")
async def read_root():
    html_path = os.path.join(os.path.dirname(__file__), "data_explorer.html")
    return FileResponse(html_path)


@app.get("/api/health")
async def health_check():
    try:
        conn = get_db_connection()
        conn.close()
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/metrics/sales/summary")
async def get_sales_summary():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        query = """
        SELECT 
            COUNT(DISTINCT transaction_id) as total_transactions,
            SUM(line_amount)::numeric(12,2) as total_revenue,
            (Select COUNT(*) FROM dim_user) as total_customers,
            AVG(order_total)::numeric(12,2) as average_order_value
        FROM (
            SELECT 
                transaction_id,
                customer_key,
                line_amount,
                SUM(line_amount) OVER (PARTITION BY transaction_id) as order_total
            FROM fact_transactionevent
        ) subquery;
        """

        cursor.execute(query)
        result = cursor.fetchone()
        cursor.close()
        conn.close()

        return {
            "total_transactions": result[0],
            "total_revenue": float(result[1]) if result[1] else 0,
            "total_customers": result[2],
            "average_order_value": float(result[3]) if result[3] else 0
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/metrics/products/top")
async def get_top_products(limit: int = 10, days: Optional[int] = None):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        date_filter = ""
        if days:
            date_filter = f"WHERE f.timestamp >= CURRENT_DATE - INTERVAL '{days} days'"

        query = f"""
        SELECT 
            p.product_id,
            p.category,
            SUM(f.line_amount)::numeric(12,2) as revenue,
            SUM(f.quantity) as units_sold,
            COUNT(DISTINCT f.transaction_id) as times_purchased
        FROM fact_transactionevent f
        JOIN dim_product p ON f.product_key = p.product_key
        {date_filter}
        GROUP BY p.product_id, p.category
        ORDER BY revenue DESC
        LIMIT %s;
        """

        cursor.execute(query, (limit,))
        results = cursor.fetchall()

        cursor.close()
        conn.close()

        products = []
        for row in results:
            products.append({
                "product_id": row[0],
                "category": row[1],
                "revenue": float(row[2]),
                "units_sold": row[3],
                "times_purchased": row[4]
            })

        return products
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/metrics/geographic/distribution")
async def get_geographic_distribution():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        query = """
        SELECT 
            u.country,
            COUNT(DISTINCT f.transaction_id) as orders,
            SUM(f.line_amount)::numeric(12,2) as revenue,
            COUNT(DISTINCT f.customer_key) as customers,
            ROUND((SUM(f.line_amount) * 100.0 / SUM(SUM(f.line_amount)) OVER())::numeric, 2) as market_share
        FROM fact_transactionevent f
        JOIN dim_user u ON f.customer_key = u.customer_key
        GROUP BY u.country
        ORDER BY revenue DESC
        LIMIT 5;
        """

        cursor.execute(query)
        results = cursor.fetchall()

        cursor.close()
        conn.close()

        distribution = []
        for row in results:
            distribution.append({
                "country": row[0],
                "orders": row[1],
                "revenue": float(row[2]),
                "customers": row[3],
                "market_share": float(row[4])
            })

        return distribution
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/metrics/categories/performance")
async def get_categories_performance(limit: int = 15):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        query = """
        SELECT 
            p.category,
            SUM(f.line_amount)::numeric(12,2) as revenue,
            COUNT(DISTINCT f.transaction_id) as transactions,
            SUM(f.quantity) as units_sold
        FROM fact_transactionevent f
        JOIN dim_product p ON f.product_key = p.product_key
        GROUP BY p.category
        ORDER BY revenue DESC
        LIMIT %s;
        """

        cursor.execute(query, (limit,))
        results = cursor.fetchall()

        cursor.close()
        conn.close()

        categories = []
        for row in results:
            categories.append({
                "category": row[0],
                "revenue": float(row[1]),
                "transactions": row[2],
                "units_sold": row[3]
            })

        return categories
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/metrics/sales/daily")
async def get_daily_sales():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        query = """
        SELECT 
            DATE(timestamp) as date,
            SUM(line_amount)::numeric(12,2) as revenue,
            COUNT(DISTINCT transaction_id) as transactions
        FROM fact_transactionevent
        GROUP BY DATE(timestamp)
        ORDER BY date;
        """

        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()
        conn.close()

        daily_sales = []
        for row in results:
            daily_sales.append({
                "date": row[0].strftime("%Y-%m-%d"),
                "revenue": float(row[1]),
                "transactions": row[2]
            })

        return daily_sales

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)