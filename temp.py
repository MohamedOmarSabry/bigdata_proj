    # # ==================== SALES METRICS AGGREGATION ====================

    # def aggregate_sales_by_hour(self, df: DataFrame) -> DataFrame:
    #     """
    #     Aggregate sales metrics by hour
    #     Returns: Total sales, transaction count, avg transaction value per hour
    #     """
    #     hourly_sales = df.withColumn("hour", hour(col("timestamp"))) \
    #         .groupBy("hour") \
    #         .agg(
    #             spark_sum("total_amount").alias("total_sales"),
    #             count("transaction_id").alias("transaction_count"),
    #             avg("total_amount").alias("avg_transaction_value")
    #         )

    #     return hourly_sales

    # def aggregate_sales_by_category(self, df: DataFrame) -> DataFrame:
    #     """
    #     Aggregate sales metrics by product category
    #     NOTE: Requires exploding products array and joining with product catalog
    #     """
    #     # Explode products array to get individual products
    #     exploded_df = df.select(
    #         "transaction_id",
    #         "user_id",
    #         "total_amount",
    #         "timestamp",
    #         explode("products").alias("product")
    #     )

    #     # Extract product details
    #     exploded_df = exploded_df.select(
    #         "transaction_id",
    #         "user_id",
    #         "timestamp",
    #         col("product.product_id").alias("product_id"),
    #         col("product.quantity").alias("quantity"),
    #         col("product.price").alias("price")
    #     )

    #     # Calculate revenue per product
    #     exploded_df = exploded_df.withColumn(
    #         "product_revenue",
    #         col("quantity") * col("price")
    #     )

    #     # Note: To aggregate by category, we'd need to join with product catalog
    #     # For now, return product-level aggregation
    #     return exploded_df

    # def aggregate_sales_by_country(self, df: DataFrame) -> DataFrame:
    #     """
    #     Aggregate sales metrics by country (region)
    #     NOTE: Requires joining transactions with users to get country
    #     """
    #     # This will be implemented in the streaming pipeline with joins
    #     # For now, return the dataframe as-is
    #     return df

