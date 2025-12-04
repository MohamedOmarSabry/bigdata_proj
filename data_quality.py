"""
Data Quality and Error Handling Module
Validates, cleans, and quarantines data based on business rules
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, when, lit, trim, regexp_replace,
    current_timestamp, concat_ws, coalesce,
    length, lower, upper, to_timestamp,
    array_contains, size
)
from pyspark.sql.types import IntegerType, DoubleType, StringType, TimestampType
from config import DATA_QUALITY
import re


class DataQualityValidator:
    """
    Main Class that is made to validate and clean data according to the defined rules, used by functions below
    """

    # initialize the parameters for validation using the set configurations
    def __init__(self):
        self.config = DATA_QUALITY
        self.valid_countries = self.config['valid_domains']['countries']
        self.valid_payment_methods = self.config['valid_domains']['payment_methods']
        self.age_range = self.config['valid_domains']['age']
        self.price_range = self.config['valid_domains']['price']
        self.amount_range = self.config['valid_domains']['total_amount']
        self.inventory_range = self.config['valid_domains']['inventory']

    # Helper functions

    def add_quality_metadata(self, df: DataFrame) -> DataFrame:
        """Add data quality metadata columns to track validation status"""
        return df \
            .withColumn("processing_timestamp", current_timestamp()) \
            .withColumn("error_reasons", lit("").cast(StringType())) \
            .withColumn("is_valid", lit(True))

    def mark_error(self, df: DataFrame, condition, error_message: str) -> DataFrame:
        """
        Mark records with errors and accumulate error reasons
        The default is that records are valid (is_valid=True). If condition is met, set is_valid=False and append error reason.
        """
        return df \
            .withColumn("is_valid", when(condition, False).otherwise(col("is_valid"))) \
            .withColumn("error_reasons",
                when(condition,
                     when(col("error_reasons") == "", lit(error_message))
                     .otherwise(concat_ws("; ", col("error_reasons"), lit(error_message)))
                ).otherwise(col("error_reasons"))
            )

    def convert_timestamp_fields(self, df: DataFrame, timestamp_fields: list) -> DataFrame:
        """Convert timestamp string fields to TimestampType for clean data"""
        for field in timestamp_fields:
            if field in df.columns:
                df = df.withColumn(field, to_timestamp(col(field)))
        return df

    def split_clean_quarantine(self, df: DataFrame, timestamp_fields: list = None):
        """
        Split the df into clean and quarantine by checking the is_valid attribute
        Clean data will have timestamps converted to TimestampType
        Quarantine data keeps original string format for debugging
        """
        clean_df = df.filter(col("is_valid") == True).drop("is_valid", "error_reasons")
        quarantine_df = df.filter(col("is_valid") == False).drop("is_valid")

        # Convert timestamp fields for clean data only
        if timestamp_fields:
            clean_df = self.convert_timestamp_fields(clean_df, timestamp_fields)

        return clean_df, quarantine_df

    # Fixable error corrections - implementation of the corrections

    def apply_fixable_corrections(self, df: DataFrame, field_name: str, target_type: str) -> DataFrame:
        """
        Apply corrections such as type conversion, trimming whitespace, formatting dates, etc.
        """
        # Trim whitespace that is leading/trailing
        if target_type == "string":
            df = df.withColumn(field_name, trim(col(field_name)))

        # Replace "<corrupted>" with NULL
        df = df.withColumn(
            field_name,
            when(col(field_name) == "<corrupted>", None)
            .otherwise(col(field_name))
        )

        # Attempt type conversion in particular for integer and double types
        if target_type == "integer":
            df = df.withColumn(
                field_name,
                when(col(field_name).cast(IntegerType()).isNotNull(),
                     col(field_name).cast(IntegerType()))
                .otherwise(col(field_name))
            )
        elif target_type == "double":
            df = df.withColumn(
                field_name,
                when(col(field_name).cast(DoubleType()).isNotNull(),
                     col(field_name).cast(DoubleType()))
                .otherwise(col(field_name))
            )
        elif target_type == "timestamp":
            # Convert ISO format string to timestamp
            # Keep as string for validation, will convert after validation passes
            pass

        return df

    # USER DATA VALIDATION

    def clean_user_data(self, df: DataFrame):
        """
        Validate and clean user data
        """
        # Add quality metadata via helper func
        df = self.add_quality_metadata(df)

        # apply corrections that were defined using the helper function
        df = self.apply_fixable_corrections(df, "email", "string")
        df = self.apply_fixable_corrections(df, "age", "integer")
        df = self.apply_fixable_corrections(df, "country", "string")
        df = self.apply_fixable_corrections(df, "registration_date", "timestamp")

        # check required fields to see if there are missing fields (in that case it should be flagged as error)
        required_fields = self.config['required_fields']['user']
        for field in required_fields:
            df = self.mark_error(
                df,
                col(field).isNull(),
                f"missing_required_field: {field}"
            )

        # Validate email format (must follow username@domain format)
        df = self.mark_error(
            df,
            ~(col("email").rlike(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")),
            "invalid_email_format"
        )

        # Validate age range (must be within configured range)
        df = self.mark_error(
            df,
            (col("age") < self.age_range[0]) | (col("age") > self.age_range[1]),
            f"invalid_age: must be between {self.age_range[0]} and {self.age_range[1]}"
        )

        # Check for negative age
        df = self.mark_error(
            df,
            col("age") < 0,
            "negative_value: age cannot be negative"
        )

        # Validate country (must be one of the 5 valid countries)
        df = self.mark_error(
            df,
            ~col("country").isin(self.valid_countries),
            f"invalid_country: must be one of {self.valid_countries}"
        )

        # Validate registration date format (ISO format)
        df = self.mark_error(
            df,
            to_timestamp(col("registration_date")).isNull() & col("registration_date").isNotNull(),
            "invalid_date_format: registration_date"
        )

        # We will then split the clean data and the data that needs to be quarantined due to validation errors
        return self.split_clean_quarantine(df, timestamp_fields=["registration_date"])
    
    # PRODUCT DATA VALIDATION

    def clean_product_data(self, df: DataFrame):
        """
        Validate and clean product catalog data
        """
        # Add quality metadata via helper func
        df = self.add_quality_metadata(df)

        # apply corrections that were defined using the helper function
        df = self.apply_fixable_corrections(df, "product_id", "string")
        df = self.apply_fixable_corrections(df, "price", "double")
        df = self.apply_fixable_corrections(df, "inventory", "integer")
        df = self.apply_fixable_corrections(df, "category", "string")

        # check required fields to see if there are missing fields (in that case it should be flagged as error)
        required_fields = self.config['required_fields']['product']
        for field in required_fields:
            df = self.mark_error(
                df,
                col(field).isNull(),
                f"missing_required_field: {field}"
            )

        # Validate price range
        df = self.mark_error(
            df,
            (col("price") < self.price_range[0]) | (col("price") > self.price_range[1]),
            f"invalid_price: must be between {self.price_range[0]} and {self.price_range[1]}"
        )

        # check for negative price
        df = self.mark_error(
            df,
            col("price") < 0,
            "negative_value: price cannot be negative"
        )

        # Validate inventory range
        df = self.mark_error(
            df,
            (col("inventory") < self.inventory_range[0]) | (col("inventory") > self.inventory_range[1]),
            f"invalid_inventory: must be between {self.inventory_range[0]} and {self.inventory_range[1]}"
        )

        # check for negative inventory
        df = self.mark_error(
            df,
            col("inventory") < 0,
            "negative_value: inventory cannot be negative"
        )

        # Split into clean and quarantine
        return self.split_clean_quarantine(df)


    # PRODUCT VIEW DATA VALIDATION

    def clean_view_data(self, df: DataFrame):
        """
        Validate and clean product view event data
        """
        # Add quality metadata via helper func
        df = self.add_quality_metadata(df)

        # apply corrections that were defined using the helper function
        df = self.apply_fixable_corrections(df, "event_id", "string")
        df = self.apply_fixable_corrections(df, "product_id", "string")
        df = self.apply_fixable_corrections(df, "user_id", "string")
        df = self.apply_fixable_corrections(df, "timestamp", "timestamp")

        # check required fields to see if there are missing fields (in that case it should be flagged as error)
        required_fields = self.config['required_fields']['view']
        for field in required_fields:
            df = self.mark_error(
                df,
                col(field).isNull(),
                f"missing_required_field: {field}"
            )

        # Validate timestamp format
        df = self.mark_error(
            df,
            to_timestamp(col("timestamp")).isNull() & col("timestamp").isNotNull(),
            "invalid_date_format: timestamp"
        )

        # Split into clean and quarantine, converting timestamps for clean data
        return self.split_clean_quarantine(df, timestamp_fields=["timestamp"])

    # CART DATA VALIDATION

    def clean_cart_data(self, df: DataFrame):
        """
        Validate and clean cart event data
        """
        # Add quality metadata via helper func
        df = self.add_quality_metadata(df)

        # apply corrections that were defined using the helper function
        df = self.apply_fixable_corrections(df, "cart_id", "string")
        df = self.apply_fixable_corrections(df, "user_id", "string")
        df = self.apply_fixable_corrections(df, "timestamp", "timestamp")

        # check required fields
        required_fields = self.config['required_fields']['cart']
        for field in required_fields:
            df = self.mark_error(
                df,
                col(field).isNull(),
                f"missing_required_field: {field}"
            )

        # Validate timestamp format
        df = self.mark_error(
            df,
            to_timestamp(col("timestamp")).isNull() & col("timestamp").isNotNull(),
            "invalid_date_format: timestamp"
        )

        # Validate products array is not empty
        df = self.mark_error(
            df,
            (size(col("products")) == 0) | col("products").isNull(),
            "invalid_cart: products array is empty"
        )

        # Split into clean and quarantine, converting timestamps for clean data
        return self.split_clean_quarantine(df, timestamp_fields=["timestamp"])
    
        # TRANSACTION DATA VALIDATION

    def clean_transaction_data(self, df: DataFrame):
        """
        Validate and clean transaction event data
        """
        # Add quality metadata via helper func
        df = self.add_quality_metadata(df)

        # apply corrections that were defined using the helper function
        df = self.apply_fixable_corrections(df, "transaction_id", "string")
        df = self.apply_fixable_corrections(df, "user_id", "string")
        df = self.apply_fixable_corrections(df, "timestamp", "timestamp")
        df = self.apply_fixable_corrections(df, "total_amount", "double")
        df = self.apply_fixable_corrections(df, "payment_method", "string")

        # check required fields
        required_fields = self.config['required_fields']['transaction']
        for field in required_fields:
            df = self.mark_error(
                df,
                col(field).isNull(),
                f"missing_required_field: {field}"
            )

        # Validate timestamp format
        df = self.mark_error(
            df,
            to_timestamp(col("timestamp")).isNull() & col("timestamp").isNotNull(),
            "invalid_date_format: timestamp"
        )

        # Validate total_amount range
        df = self.mark_error(
            df,
            (col("total_amount") < self.amount_range[0]) | (col("total_amount") > self.amount_range[1]),
            f"invalid_total_amount: must be between {self.amount_range[0]} and {self.amount_range[1]}"
        )

        # Check for negative total_amount
        df = self.mark_error(
            df,
            col("total_amount") < 0,
            "negative_value: total_amount cannot be negative"
        )

        # Validate payment method
        df = self.mark_error(
            df,
            ~col("payment_method").isin(self.valid_payment_methods),
            f"invalid_payment_method: must be one of {self.valid_payment_methods}"
        )

        # Validate products array is not empty
        df = self.mark_error(
            df,
            (size(col("products")) == 0) | col("products").isNull(),
            "invalid_transaction: products array is empty"
        )

        # Split into clean and quarantine, converting timestamps for clean data
        return self.split_clean_quarantine(df, timestamp_fields=["timestamp"])


# Abstracted functions for readability and ease of use

def validate_users(df: DataFrame):
    validator = DataQualityValidator()
    return validator.clean_user_data(df)

def validate_products(df: DataFrame):
    validator = DataQualityValidator()
    return validator.clean_product_data(df)

def validate_views(df: DataFrame):
    validator = DataQualityValidator()
    return validator.clean_view_data(df)

def validate_carts(df: DataFrame):
    validator = DataQualityValidator()
    return validator.clean_cart_data(df)

def validate_transactions(df: DataFrame):
    validator = DataQualityValidator()
    return validator.clean_transaction_data(df)
