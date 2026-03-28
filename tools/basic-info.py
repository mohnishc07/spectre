import requests
import json

class CompanyScanner:
    def __init__(self, target_name):
        self.target_name = target_name
        self.report = {
            "search_query": target_name,
            "ticker": "-1",
            "corporate_identity": {},
            "leadership": {}
        }

    def fetch_gleif_data(self):
        """
        Script A: Queries the Global Legal Entity Identifier Foundation.
        Uses a 'contains' filter for approximate name matching.
        """
        base_url = "https://api.gleif.org/api/v1/lei-records"
        params = {
            "filter[entity.legalName]": f"contains:{self.target_name}",
            "page[size]": 1
        }
        
        try:
            response = requests.get(base_url, params=params, timeout=10)
            data = response.json()
            
            if data.get('data'):
                attributes = data['data'][0]['attributes']
                entity = attributes['entity']
                
                self.report["corporate_identity"] = {
                    "official_legal_name": entity.get('legalName', {}).get('name'),
                    "lei_code": attributes.get('lei'),
                    "jurisdiction": entity.get('jurisdiction'),
                    "entity_status": entity.get('status'),
                    "legal_address": entity.get('legalAddress', {}).get('addressLines', ["N/A"])[0],
                    "city": entity.get('legalAddress', {}).get('city')
                }
        except Exception as e:
            self.report["corporate_identity"]["error"] = f"GLEIF Error: {str(e)}"

    def fetch_wikidata_data(self):
        """
        Script B: Searches Wikidata for the entity ID, then runs a SPARQL
        query to extract CEO, Founders, and Stock Ticker.
        """
        # 1. Search for the Wikidata QID
        search_url = "https://www.wikidata.org/w/api.php"
        search_params = {
            "action": "wbsearchentities",
            "search": self.target_name,
            "language": "en",
            "format": "json"
        }
        
        try:
            search_res = requests.get(search_url, params=search_params).json()
            if not search_res.get('search'):
                return

            qid = search_res['search'][0]['id']
            
            # 2. SPARQL query for specific properties: 
            # CEO (P169), Founder (P112), Ticker (P249)
            sparql_query = f"""
            SELECT ?ceoLabel ?founderLabel ?ticker WHERE {{
              wd:{qid} wdt:P169 ?ceo .
              OPTIONAL {{ wd:{qid} wdt:P112 ?founder . }}
              OPTIONAL {{ wd:{qid} wdt:P249 ?ticker . }}
              SERVICE wikibase:label {{ bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }}
            }}
            """
            sparql_url = "https://query.wikidata.org/sparql"
            res = requests.get(sparql_url, params={'query': sparql_query, 'format': 'json'}).json()
            
            bindings = res['results']['bindings']
            if bindings:
                # Extract Founders (can be multiple)
                founders = list(set(b['founderLabel']['value'] for b in bindings if 'founderLabel' in b))
                
                self.report["leadership"] = {
                    "ceo": bindings[0].get('ceoLabel', {}).get('value'),
                    "founders": founders if founders else "Not Found"
                }
                
                # Update Ticker if found, otherwise stays -1
                if 'ticker' in bindings[0]:
                    self.report["ticker"] = bindings[0]['ticker']['value']
                    
        except Exception as e:
            self.report["leadership"]["error"] = f"Wikidata Error: {str(e)}"

    def run(self):
        self.fetch_gleif_data()
        self.fetch_wikidata_data()
        return self.report

# --- RUNTIME ---
if __name__ == "__main__":
    company_input = input("Enter company name (approximate): ")
    scanner = CompanyScanner(company_input)
    results = scanner.run()

    print("\n--- OSINT RESULTS ---")
    print(json.dumps(results, indent=2))