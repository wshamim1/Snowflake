# Snowflake Python Worksheet: Sales Analysis
# This script uses Snowpark to perform data analysis on the SALES_DEMO table
# Can be run directly in Snowflake or locally with Python environment

from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, sum as sf_sum, count as sf_count, avg as sf_avg
import os

# Only import dotenv when running locally (not in Snowflake)
try:
    from dotenv import load_dotenv
    DOTENV_AVAILABLE = True
except ImportError:
    DOTENV_AVAILABLE = False


def get_snowpark_session():
    """
    Create a Snowflake Snowpark session.
    Supports both local (.env) and Snowflake-hosted environments.
    """
    try:
        # Try to get active session in Snowflake environment (first priority)
        from snowflake.snowpark.context import get_active_session
        print("📌 Using Snowflake native session")
        return get_active_session()
    except Exception:
        # Fall back to local .env credentials
        if not DOTENV_AVAILABLE:
            raise ImportError(
                "❌ Running in Snowflake: dotenv not available (not needed).\n"
                "   The script should use get_active_session() automatically.\n"
                "   If this error persists, ensure the worksheet has Snowflake context."
            )
        
        print("📌 Using local .env credentials")
        load_dotenv()
        
        connection_params = {
            "account": os.getenv("SNOWFLAKE_ACCOUNT"),
            "user": os.getenv("SNOWFLAKE_USER"),
            "password": os.getenv("SNOWFLAKE_PASSWORD"),
            "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
            "database": os.getenv("SNOWFLAKE_DATABASE"),
            "schema": os.getenv("SNOWFLAKE_SCHEMA"),
        }
        
        return Session.builder.configs(connection_params).create()


def analyze_sales(session):
    """Perform comprehensive sales analysis."""
    print("=" * 70)
    print("SNOWFLAKE SALES ANALYSIS")
    print("=" * 70)
    
    # Read the SALES_DEMO table
    df = session.table("SALES_DEMO")
    
    # 1. Total Sales Overview
    print("\n1. TOTAL SALES OVERVIEW")
    print("-" * 70)
    overview = df.select(
        sf_sum("SALES_AMOUNT").alias("TOTAL_SALES"),
        sf_sum("QUANTITY").alias("TOTAL_QUANTITY"),
        sf_count("ORDER_ID").alias("TOTAL_ORDERS"),
        sf_avg("SALES_AMOUNT").alias("AVG_ORDER_VALUE")
    )
    overview.show()
    
    # 2. Sales by Region
    print("\n2. SALES BY REGION")
    print("-" * 70)
    by_region = df.group_by("REGION").agg(
        sf_sum("SALES_AMOUNT").alias("TOTAL_SALES"),
        sf_count("ORDER_ID").alias("NUM_ORDERS"),
        sf_avg("SALES_AMOUNT").alias("AVG_SALES")
    ).sort("TOTAL_SALES", ascending=False)
    by_region.show()
    
    # 3. Sales by Category
    print("\n3. SALES BY CATEGORY")
    print("-" * 70)
    by_category = df.group_by("CATEGORY").agg(
        sf_sum("SALES_AMOUNT").alias("TOTAL_SALES"),
        sf_sum("QUANTITY").alias("TOTAL_QUANTITY"),
        sf_count("ORDER_ID").alias("NUM_ORDERS")
    ).sort("TOTAL_SALES", ascending=False)
    by_category.show()
    
    # 4. Top Products
    print("\n4. TOP 5 PRODUCTS BY SALES")
    print("-" * 70)
    top_products = df.group_by("PRODUCT").agg(
        sf_sum("SALES_AMOUNT").alias("TOTAL_SALES"),
        sf_sum("QUANTITY").alias("TOTAL_QUANTITY"),
        sf_count("ORDER_ID").alias("NUM_ORDERS")
    ).sort("TOTAL_SALES", ascending=False).limit(5)
    top_products.show()
    
    # 5. Regional Category Performance
    print("\n5. SALES BY REGION AND CATEGORY")
    print("-" * 70)
    regional_category = df.group_by("REGION", "CATEGORY").agg(
        sf_sum("SALES_AMOUNT").alias("TOTAL_SALES"),
        sf_count("ORDER_ID").alias("NUM_ORDERS")
    ).sort("TOTAL_SALES", ascending=False)
    regional_category.show()
    
    # 6. Orders by Date
    print("\n6. ORDERS BY DATE")
    print("-" * 70)
    by_date = df.group_by("ORDER_DATE").agg(
        sf_sum("SALES_AMOUNT").alias("DAILY_SALES"),
        sf_count("ORDER_ID").alias("NUM_ORDERS")
    ).sort("ORDER_DATE")
    by_date.show()
    
    # 7. High-Value Orders (> $1500)
    print("\n7. HIGH-VALUE ORDERS (> $1,500)")
    print("-" * 70)
    high_value = df.filter(col("SALES_AMOUNT") > 1500).select(
        "ORDER_ID", "ORDER_DATE", "PRODUCT", "REGION", "SALES_AMOUNT"
    ).sort("SALES_AMOUNT", ascending=False)
    high_value.show()
    
    # 8. Quantity Analysis
    print("\n8. QUANTITY ANALYSIS")
    print("-" * 70)
    qty_analysis = df.group_by("CATEGORY").agg(
        sf_sum("QUANTITY").alias("TOTAL_UNITS"),
        sf_avg("QUANTITY").alias("AVG_UNITS_PER_ORDER"),
        sf_count("ORDER_ID").alias("NUM_ORDERS")
    ).sort("TOTAL_UNITS", ascending=False)
    qty_analysis.show()
    
    # 9. Regional Summary
    print("\n9. COMPREHENSIVE REGIONAL SUMMARY")
    print("-" * 70)
    regional_summary = df.group_by("REGION").agg(
        sf_count("ORDER_ID").alias("ORDERS"),
        sf_sum("QUANTITY").alias("UNITS_SOLD"),
        sf_sum("SALES_AMOUNT").alias("REVENUE"),
        sf_avg("SALES_AMOUNT").alias("AVG_ORDER_VALUE")
    ).sort("REVENUE", ascending=False)
    regional_summary.show()
    
    print("\n" + "=" * 70)
    print("ANALYSIS COMPLETE")
    print("=" * 70)


def filter_and_analyze(session, region: str = None, category: str = None):
    """
    Flexible analysis with optional filters.
    
    Args:
        session: Snowpark session
        region: Filter by region (e.g., 'North', 'South', 'East', 'West')
        category: Filter by category (e.g., 'Electronics', 'Furniture', 'Stationery')
    """
    df = session.table("SALES_DEMO")
    
    # Apply filters
    if region:
        df = df.filter(col("REGION") == region.upper())
    if category:
        df = df.filter(col("CATEGORY") == category.upper())
    
    print(f"\nFiltered Analysis (Region: {region}, Category: {category})")
    print("-" * 70)
    
    summary = df.agg(
        sf_count("ORDER_ID").alias("ORDERS"),
        sf_sum("SALES_AMOUNT").alias("TOTAL_SALES"),
        sf_avg("SALES_AMOUNT").alias("AVG_SALES"),
        sf_sum("QUANTITY").alias("TOTAL_QUANTITY")
    )
    summary.show()
    
    # Show detailed records
    print("\nDetailed Records:")
    df.select("ORDER_ID", "ORDER_DATE", "PRODUCT", "REGION", "CATEGORY", "QUANTITY", "SALES_AMOUNT").show()


def export_to_csv(session, output_path: str = "sales_report.csv"):
    """Export sales data to CSV file."""
    df = session.table("SALES_DEMO")
    
    # Convert to Pandas for easier CSV export
    pandas_df = df.to_pandas()
    pandas_df.to_csv(output_path, index=False)
    
    print(f"✅ Data exported to {output_path}")
    print(f"   Total rows: {len(pandas_df)}")


def main(session):
    """
    Main handler function for Snowflake Python Worksheets.
    
    Args:
        session: Snowpark session provided by Snowflake
    """
    print("📌 Using Snowflake native session")
    
    # Run the comprehensive analysis
    analyze_sales(session)
    
    # Optional: Run filtered analysis
    # filter_and_analyze(session, region="North", category="Electronics")
    # filter_and_analyze(session, region="South")
    
    # Optional: Export to CSV
    # export_to_csv(session, "sales_report.csv")
    
    print("\n✅ Worksheet execution complete!")


# =============================================
# LOCAL EXECUTION (when run as script)
# =============================================

if __name__ == "__main__":
    session_local = get_snowpark_session()
    analyze_sales(session_local)
