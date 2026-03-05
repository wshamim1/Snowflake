# Snowflake Python Worksheets

Python worksheets for data analysis and reporting using Snowflake's Snowpark API.

## Files

- **sales_analysis.py** - Comprehensive analysis script (local or Snowflake, with .env support)
- **sales_analysis_snowflake_native.py** - Optimized for Snowflake Python Worksheets (no external dependencies)

## Features

### sales_analysis.py

This worksheet provides multiple analysis functions:

1. **Total Sales Overview** - Overall sales metrics and statistics
2. **Sales by Region** - Regional performance analysis
3. **Sales by Category** - Category-wise breakdown
4. **Top 5 Products** - Best-performing products by sales
5. **Regional Category Performance** - Cross-tabulation of region and category
6. **Orders by Date** - Daily sales trends
7. **High-Value Orders** - Filter for orders over $1,500
8. **Quantity Analysis** - Unit sales and volume metrics
9. **Comprehensive Regional Summary** - Full regional performance overview

## Usage

### Option 1: Run in Snowflake (Recommended for Snowflake Worksheets)

**Use `sales_analysis_snowflake_native.py`** - No external dependencies needed!

1. Open Snowflake web interface
2. Create a new Python Worksheet
3. Copy the entire content of `sales_analysis_snowflake_native.py`
4. Click **Run** to execute
5. View results in the worksheet output panel

✅ This version works immediately - no setup required!

### Option 2: Run Locally (with .env)

```bash
# Ensure your .env file is set up with Snowflake credentials
python apps/python_worksheets/sales_analysis.py
```

### Option 3: Use Filtered Analysis

In Snowflake worksheet, uncomment the filter examples:

```python
# Analyze specific region
filter_and_analyze(region="North")

# Analyze specific category
filter_and_analyze(category="Electronics")

# Combine filters
filter_and_analyze(region="North", category="Electronics")

# See top orders
top_customers_by_spending()
```

### Option 4: Run Locally with Custom Filters

Edit `sales_analysis.py` and uncomment examples in the `if __name__ == "__main__":` block.

## Output

Running the script produces:

- 9 different analytical views of sales data
- Sorted summaries by revenue, orders, units sold
- Detailed record listings
- Summary statistics (totals, averages, counts)

## Requirements

- `snowflake-snowpark-python` (included in requirements.txt)
- `python-dotenv` (for local .env file support)
- Pandas (for CSV export functionality)

## Snowflake Session Setup

The script automatically:

1. **In Snowflake**: Uses the active Snowflake worksheet session
2. **Locally**: Reads credentials from `.env` file and creates a session

## Functions

### `get_snowpark_session()`
Returns a Snowpark session, automatically detecting environment (Snowflake or local).

### `analyze_sales()`
Runs comprehensive 9-part sales analysis (main function).

### `filter_and_analyze(region=None, category=None)`
Performs analysis with optional regional or categorical filtering.

### `export_to_csv(output_path="sales_report.csv")`
Exports all sales data to a CSV file for use in other tools.

## Tips

- All analyses are read-only; no data is modified
- Aggregations are optimized by Snowflake for performance
- Use filters to focus analysis on specific regions or categories
- For local CSV export, use `sales_analysis.py` (has pandas support)
- For Snowflake worksheets, use `sales_analysis_snowflake_native.py` (no dependencies)

## Quick Comparison

| Feature | sales_analysis.py | sales_analysis_snowflake_native.py |
|---------|---|---|
| Run locally with .env | ✅ | ❌ |
| Run in Snowflake worksheet | ✅ | ✅ (Recommended) |
| Export to CSV | ✅ | ❌ |
| No external dependencies | ❌ | ✅ |
| Custom filtering | ✅ | ✅ |

## Next Steps

- Customize filters for your business questions
- Combine multiple analyses for comprehensive reports
- Schedule as a Snowflake task for automated reporting
- Add more complex transformations or ML-based predictions
