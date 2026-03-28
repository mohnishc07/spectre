# Program for importing the details of the company using EDGAR TOOLS.
from edgar import Company, set_identity, set_rate_limit


set_identity("OSINT_Project_Dev contact@yourproject.io")

set_rate_limit(5) 

class EdgarAnalyzer:
    def __init__(self, target_id: str):
        """
        target_id: Can be a Ticker (AAPL) or a CIK (0000320193)
        """
        self.target_id = target_id.upper()
        try:
            self.company = Company(self.target_id)
        except Exception as e:
            self.company = None
            print(f"Error: Could not resolve company ID '{target_id}': {e}")

    def get_legal_profile(self):
        """Returns the legal name and registration basics."""
        if not self.company: return {"error": "Invalid Target"}
        return {
            "legal_name": self.company.name,
            "cik": self.company.cik,
            "industry": self.company.industry,
            "state_of_incorporation": self.company.state_of_incorporation
        }

    def get_board_and_exec_changes(self):
        """
        Retrieves recent 8-K filings to find leadership shifts.
        Focuses on Item 5.02 (Legal code for personnel changes).
        """
        if not self.company: return []
        
        # Pull the 5 most recent 8-K filings directly from SEC
        filings = self.company.get_filings(form="8-K").latest(5)
        changes = []
        
        for f in filings:
            obj = f.obj()
            # Check if this specific 8-K mentions leadership changes
            if hasattr(obj, 'items') and "5.02" in obj.items:
                changes.append({
                    "date_filed": f.filing_date,
                    "description": "Executive/Director Departure or Appointment",
                    "source_url": f.html_url
                })
        return changes

    def get_leadership_snapshot(self):
        """Extracts CEO and Board info from the latest Proxy Statement (DEF 14A)."""
        if not self.company: return {}
        
        try:
            # DEF 14A contains the most detailed leadership and pay data
            proxy = self.company.get_filings(form="DEF 14A").latest().obj()
            return {
                "ceo_name": getattr(proxy, 'peo_name', 'Unknown'),
                "ceo_compensation": getattr(proxy, 'peo_total_comp', 'Not Listed'),
                "board_members": [d.name for d in getattr(proxy, 'directors', [])]
            }
        except:
            return {"status": "Proxy data unavailable for this entity"}

# --- MODULAR EXECUTION ---
def run_edgar_scan(ticker_or_cik):
    """Entry point for the backend orchestrator."""
    analyzer = EdgarAnalyzer(ticker_or_cik)
    return {
        "metadata": analyzer.get_legal_profile(),
        "board_changes": analyzer.get_board_and_exec_changes(),
        "current_leadership": analyzer.get_leadership_snapshot()
    }