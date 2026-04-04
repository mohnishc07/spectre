
from edgar import *
set_identity("mohnishc77@gmail.com")

# Get a company's balance sheet
balance_sheet = Company("AAPL").get_financials().balance_sheet()

# Browse a company's filings
company = Company("MSFT")

# Parse insider transactions
filings = company.get_filings(form="4")
form4 = filings[0].obj()
# Get Microsoft's income statement - now shows product/service breakdowns
company = Company("MSFT")
xbrl = company.get_filings(form="10-K").latest().xbrl()
income_stmt = xbrl.statements.income_statement()

print(income_stmt)
# Output shows both summary revenue AND detailed breakdowns:
# - Product revenue: $63.9B 
# - Service revenue: $217.8B
# - Business segment details (LinkedIn: $17.8B, Gaming: $23.5B, etc.)
# Default behavior - includes dimensional segment data
df_enhanced = income_stmt.to_dataframe()  # 48 rows for Microsoft
print(f"Enhanced view: {len(df_enhanced)} rows")

# Standard view - face presentation only
df_standard = income_stmt.to_dataframe(view="standard")  # 21 rows
print(f"Standard view: {len(df_standard)} rows")

# Summary view - non-dimensional totals only
df_summary = income_stmt.to_dataframe(view="summary")
print(f"Summary view: {len(df_summary)} rows")