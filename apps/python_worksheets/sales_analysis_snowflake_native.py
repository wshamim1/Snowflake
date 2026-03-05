# Snowflake Python Worksheet: Sales Analysis (Native)
# Optimized for running directly in Snowflake Python Worksheets
# This version has no external dependencies

from snowflake.snowpark.functions import col, sum as sf_sum, count as sf_count, avg as sf_avg


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


def top_customers_by_spending(session):
    """Analyze top spending patterns."""
    df = session.table("SALES_DEMO")
    
    print("\n10. TOP ORDERS BY SPENDING")
    print("-" * 70)
    top_orders = df.select(
        "ORDER_ID", "ORDER_DATE", "REGION", "PRODUCT", "CATEGORY", "SALES_AMOUNT"
    ).sort("SALES_AMOUNT", ascending=False).limit(10)
    top_orders.show()


def main(session):
    """
    Main handler function for Snowflake Python Worksheets.
    Must return a Snowpark DataFrame.
    
    Args:
        session: Snowpark session provided by Snowflake
    
    Returns:
        DataFrame: Sales summary or analysis results
    """
    # Run the comprehensive analysis
    analyze_sales(session)
    
    # Return the SALES_DEMO table as the result
    # (Snowflake Python Worksheets require a DataFrame return)
    result_df = session.table("SALES_DEMO").select(
        "ORDER_ID", "ORDER_DATE", "REGION", "PRODUCT", "CATEGORY", "QUANTITY", "SALES_AMOUNT"
    ).sort("SALES_AMOUNT", ascending=False)
    
    # Optional: Uncomment to run additional analyses
    # filter_and_analyze(session, region="North")
    # filter_and_analyze(session, category="Electronics")
    # top_customers_by_spending(session)
    
    print("\n✅ Worksheet execution complete!")
    return result_df
