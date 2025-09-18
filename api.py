import sqlite3
import json
import requests
import os
from fastapi import FastAPI, HTTPException, Query, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional, Dict
from datetime import date
import logging
from rapidfuzz import process, fuzz  
import json
import logging

# =============================================================
# CONFIGURAZIONE
# =============================================================
DB_FILE = "etf_analysis.db"
BLACKLIST_DIR = "blacklist"
API_KEY_FILE = "apikey.txt"

logging.basicConfig(
    filename='app.log',          # Log file name
    filemode='w',                # 'w' = overwrite, 'a' = append
    level=logging.INFO,          # Minimum level to log
    format='%(asctime)s - %(levelname)s - %(message)s'  # Optional format
)

logging.info("This will go into app.log")

# =============================================================
# ETF JSON LOADING
# =============================================================


with open("etfs.json") as f:
    etfs = json.load(f)
etf_lookup = {
    f"{etf['symbol']} {etf['name']}": etf for etf in etfs
}

symbol_lookup = {
    etf["symbol"].upper(): etf for etf in etfs
}

# Inizializza l'applicazione FastAPI
app = FastAPI(
    title="ETF Screener API",
    description="API per analizzare le holding di ETF, con fetch dinamico se i dati non sono presenti."
    
)


#app.add_middleware(
#    CORSMiddleware,
#    allow_origins="*",
#    allow_credentials=True,
#    allow_methods=["*"],
#    allow_headers=["*"],
#)

# =============================================================
# MODELLI Pydantic
# =============================================================
class BlacklistCompanyDto(BaseModel):
    companyName: str
    description: Optional[str] = None

class AddToBlacklistRequest(BaseModel):
    listName: str
    companies: List[BlacklistCompanyDto]
    
class Holding(BaseModel):
    CompanyName: str
    Weight: float  
    
class EtfSearchResult(BaseModel):
    Ticker: str
    Holdings: List[Holding]
    
class EtfDetailsResponse(BaseModel):
    Ticker: str
    ETFName: Optional[str]
    FossilFuelsPercentage: float
    FossilFuelsHoldings: List[str]
    WeaponsPercentage: float
    WeaponsHoldings: List[str]
    GazaListPercentage: float
    GazaListHoldings: List[str]
    category: Optional[str]
    family: Optional[str]
    summary: Optional[str]
    exchange: Optional[str]
    currency: Optional[str]
    category_group: Optional[str]

# =============================================================
# SERVIZI E LOGICA DI BUSINESS
# =============================================================


def get_api_key():
   # """Legge la chiave API da un file. Solleva un'eccezione se non la trova."""
   # if not os.path.exists(API_KEY_FILE):
   #     raise HTTPException(status_code=500, detail=f"File chiave API '{API_KEY_FILE}' non trovato.")
   # with open(API_KEY_FILE, 'r') as f:
   #     key = f.read().strip()
   # if not key:
   #     raise HTTPException(status_code=500, detail=f"La chiave API nel file '{API_KEY_FILE}' è vuota.")
   # return key
    return ""

gaza_company_descriptions = None

def load_gaza_descriptions():
    global gaza_company_descriptions
    if gaza_company_descriptions is None:
        with open("blacklist/GAZA_LIST.json", "r", encoding="utf-8") as f:
            gaza_company_descriptions = json.load(f).get("companies", [])
    return gaza_company_descriptions      
    
def get_db_connection():
    """Crea e restituisce una connessione al database."""
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn

def fetch_and_analyze_ticker(ticker: str, api_key: str):
    """
    Funzione centrale che orchestra il fetch, l'analisi e il salvataggio
    per un nuovo ticker.
    """
    print(f"Esecuzione analisi on-demand per {ticker}...")
    logging.info(f"Esecuzione analisi on-demand per {ticker}...")
    print("Fetch da Alpha Vantage")
    holdings_data = get_etf_holdings_from_finworld(ticker, api_key)
    if not holdings_data:
        print(f"Fetch senza risultato {ticker}. Salvataggio di un'analisi vuota.")
        save_empty_analysis_to_db(ticker)
        return None

    # 2. Salva i dati grezzi nel DB
    save_raw_holdings_to_db(ticker, holdings_data['profile']['holdings'])
    # 3. Esegui l'analisi e salva i risultati
    analysis_result = run_analysis_for_ticker(ticker, ticker)
    
    return analysis_result

def fetch_holdings(ticker: str, api_key: str):
    """
    Fetch the ETF holdings from Alpha Vantage and save them to the database.
    This function is used for on-demand analysis when the ETF is not found in the local database
    """
    print(f"Esecuzione analisi on-demand per {ticker}...")
    holdings_data = get_etf_holdings_from_finworld(ticker, api_key)
    if not holdings_data:
        return None

    # 2. Salva i dati grezzi nel DB
    save_raw_holdings_to_db(ticker, holdings_data['profile']['holdings'])

def save_empty_analysis_to_db(ticker: str):
    """When we find no Financial Data."""
    conn = get_db_connection()
    cur = conn.cursor()

    db_data = {
        "ticker": ticker, "ETFName": "ETFName", "AnalysisDate": date.today().isoformat(),
        "FossilFuelsPercentage": -1,
        "FossilFuelsHoldings": json.dumps(["no data found"]),
        "WeaponsPercentage": -1,
        "WeaponsHoldings": json.dumps(["no data found"]),
        "GazaListPercentage": -1,
        "GazaListHoldings": json.dumps(["no data found"])
    }
    cur.execute('''
        INSERT INTO AnalysisResults (Ticker, ETFName, AnalysisDate, FossilFuelsPercentage, FossilFuelsHoldings, WeaponsPercentage, WeaponsHoldings, GazaListPercentage, GazaListHoldings)
        VALUES (:ticker, :ETFName, :AnalysisDate, :FossilFuelsPercentage, :FossilFuelsHoldings, :WeaponsPercentage, :WeaponsHoldings, :GazaListPercentage, :GazaListHoldings)
        ON CONFLICT(Ticker) DO UPDATE SET
            ETFName=excluded.ETFName, AnalysisDate=excluded.AnalysisDate, FossilFuelsPercentage=excluded.FossilFuelsPercentage, FossilFuelsHoldings=excluded.FossilFuelsHoldings,
            WeaponsPercentage=excluded.WeaponsPercentage, WeaponsHoldings=excluded.WeaponsHoldings, GazaListPercentage=excluded.GazaListPercentage, GazaListHoldings=excluded.GazaListHoldings
    ''', db_data)
    conn.commit()
    conn.close()
    return db_data
    
def get_etf_holdings_from_api_ALPHA(ticker: str, api_key: str):
    """Recupera le holding di un ETF da Alpha Vantage."""
    url = f"https://www.alphavantage.co/query?function=ETF_PROFILE&symbol={ticker}&apikey={api_key}"
    logging.info(f"Chiamata API per {ticker} a {url}")
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        logging.info(f"Risposta API per {ticker}: {data}")
        if data.get("holdings") is None:
            #print(data.get("holdings"))
            if data.get("profile", {}).get("holdings") is None:
                 logging.info(f"Avviso: Risposta API per {ticker} non contiene la lista 'holdings'.")
                 return None
        
        # Normalizza la risposta per avere sempre 'profile' e 'holdings'
        if "profile" not in data:
            data["profile"] = {}
        if "holdings" not in data["profile"]:
            data["profile"]["holdings"] = data["holdings"]
        if "ETFName" not in data["profile"]:
             data["profile"]["ETFName"] = f"{ticker} Analysis"


        return data
    except Exception as e:
        logging.info(f"Errore durante la chiamata API per {ticker}: {e}")
        return None
    
def get_etf_holdings_from_finworld(ticker: str, api_key: str):
    """Recupera le holding di un ETF dal nuovo API."""
    url = f" https://api.finnworlds.com/api/v1/etfholdings?key={api_key}&ticker={ticker}"
    
    isin = ticker
    logging.info(f"Chiamata API per {ticker} a {url}")
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        logging.info(f"Risposta API per {ticker}: {data}")
        
        # Check if the response has the expected structure
        if "result" not in data or "output" not in data["result"] or not data["result"]["output"]:
            logging.info(f"Avviso: Risposta API per {ticker} non contiene la Results. Proviamo con ISIN")
            url = f" https://api.finnworlds.com/api/v1/etfholdings?key={api_key}&ISIN={isin}"
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            logging.info(f"Risposta API per {ticker}: {data}")
            if "result" not in data or "output" not in data["result"] or not data["result"]["output"]:
                logging.info(f"Avviso: Risposta API - ISIN per {ticker} non contiene la Results. boh")
                return None

            
        # Extract the first output (assuming there's only one)
        output = data["result"]["output"][0]
        holdings_list = output.get("holdings", [])
        fund_name = data["result"].get("basics", {}).get("fund_name") or output.get("attributes", {}).get("series_name")
        
        # If we don't have holdings, return None
        if not holdings_list:
            logging.info(f"Avviso: Risposta API per {ticker} non contiene holdings.")
            return None
            
        # Build the normalized holdings
        normalized_holdings = []
        for holding in holdings_list:
            security = holding.get("investment_security", {})
            description = security.get("title") or security.get("name")
            weight = security.get("percent_value")
            if description and weight is not None:
                normalized_holdings.append({
                    "symbol": "",   # we don't have a stock symbol
                    "description": description,
                    "weight": weight
                })
                
        # Build the response structure that the existing code expects
        normalized_response = {
            "profile": {
                "holdings": normalized_holdings,
                "ETFName": fund_name or f"{ticker} Analysis"
            }
        }
        
        return normalized_response
    except Exception as e:
        logging.info(f"Errore durante la chiamata API per {ticker}: {e}")
        return None
    
    
    
def save_raw_holdings_to_db(ticker: str, holdings_data: List[Dict]):
    """Salva la lista completa di holding nel database cache."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM RawHoldings WHERE EtfTicker = ?", (ticker,))
    
    holdings_to_add = []
    processed_companies = set()
    
    for holding in holdings_data:
        asset_name = holding.get("description")
        weight_str = holding.get("weight")
        if not all([asset_name, weight_str]) or asset_name.lower() in processed_companies:
            continue
        try:
            weight = float(weight_str)
            holdings_to_add.append((ticker, date.today().isoformat(), holding.get("symbol"), asset_name, weight))
            processed_companies.add(asset_name.lower())
        except (ValueError, TypeError):
            continue
            
    cursor.executemany("INSERT INTO RawHoldings (EtfTicker, FetchDate, CompanySymbol, CompanyName, Weight) VALUES (?, ?, ?, ?, ?)", holdings_to_add)
    conn.commit()
    conn.close()

def run_analysis_for_ticker(ticker: str, ETFName: str):
    """Esegue l'analisi per un singolo ticker e salva il risultato."""
    conn = get_db_connection()
    cursor = conn.cursor()

    # Carica le blacklist dai file JSON
    blacklists = {}
   #for filename in os.listdir(BLACKLIST_DIR):
   #    if filename.endswith(".json"):
   #        list_name = filename.upper().replace(".JSON", "")
   #        with open(os.path.join(BLACKLIST_DIR, filename), 'r', encoding='utf-8') as f:
   #            data = json.load(f)
   #            blacklists[list_name] = [
   #                item.get('CompanyName', '').lower()
   #                for item in data.get('companies', [])
   #                if isinstance(item, dict)
   #            ]
    load_gaza_descriptions()  # Make sure this sets the global gaza_company_descriptions

    blacklists["GAZA_LIST"] = [
        item.get('CompanyName', '').lower()
        for item in gaza_company_descriptions
        if isinstance(item, dict)
        ]
                
    cursor.execute("SELECT CompanyName, Weight FROM RawHoldings WHERE EtfTicker = ?", (ticker,))
    holdings = cursor.fetchall()
    if not holdings:
        save_empty_analysis_to_db(ticker)
        logging.info(f"Nessuna holding trovata per {ticker}.")
    else:
        logging.info(f"Trovate {len(holdings)} holding per {ticker}.")
        analysis = {name: {"percentage": 0.0, "list": []} for name in blacklists.keys()}

        for company_name, weight in holdings:
            for list_name, companies in blacklists.items():
                if any(bl_company in company_name.lower() for bl_company in companies):
                    analysis[list_name]["percentage"] += weight
                    analysis[list_name]["list"].append(f"{company_name} ({(weight):.2f}%)")

        db_data = {
            "ticker": ticker, "ETFName": "ETFName", "AnalysisDate": date.today().isoformat(),
            "FossilFuelsPercentage": round(analysis.get("FOSSIL_FUELS", {}).get("percentage", 0.0), 4),
            "FossilFuelsHoldings": json.dumps(analysis.get("FOSSIL_FUELS", {}).get("list", [])),
            "WeaponsPercentage": round(analysis.get("WEAPONS", {}).get("percentage", 0.0), 4),
            "WeaponsHoldings": json.dumps(analysis.get("WEAPONS", {}).get("list", [])),
            "GazaListPercentage": round(analysis.get("GAZA_LIST", {}).get("percentage", 0.0), 4),
            "GazaListHoldings": json.dumps(list(set(analysis.get("GAZA_LIST", {}).get("list", []))))
        }

        cursor.execute('''
            INSERT INTO AnalysisResults (Ticker, ETFName, AnalysisDate, FossilFuelsPercentage, FossilFuelsHoldings, WeaponsPercentage, WeaponsHoldings, GazaListPercentage, GazaListHoldings)
            VALUES (:ticker, :ETFName, :AnalysisDate, :FossilFuelsPercentage, :FossilFuelsHoldings, :WeaponsPercentage, :WeaponsHoldings, :GazaListPercentage, :GazaListHoldings)
            ON CONFLICT(Ticker) DO UPDATE SET
                ETFName=excluded.ETFName, AnalysisDate=excluded.AnalysisDate, FossilFuelsPercentage=excluded.FossilFuelsPercentage, FossilFuelsHoldings=excluded.FossilFuelsHoldings,
                WeaponsPercentage=excluded.WeaponsPercentage, WeaponsHoldings=excluded.WeaponsHoldings, GazaListPercentage=excluded.GazaListPercentage, GazaListHoldings=excluded.GazaListHoldings
        ''', db_data)

        conn.commit()
        conn.close()
        return db_data

def safe_get(row, key, default=""):
    try:
        value = row[key]
        if value is None:
            return default
        return value
    except (KeyError, IndexError):
        return default
    
def populateDb():
    with open("etfs.json", "r", encoding="utf-8") as f:
        etfs = json.load(f)

# Create SQLite DB
    conn = get_db_connection()
    cur = conn.cursor()

# Insert records
    for etf in etfs:
        cur.execute("""
            INSERT INTO etfs (
                symbol, name, summary,
                category_group, category,
                family, exchange, currency
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            etf.get("symbol", ""),
            etf.get("name", ""),
            etf.get("summary", ""),
            etf.get("category_group", ""),
            etf.get("category", ""),
            etf.get("family", ""),
            etf.get("exchange", ""),
            etf.get("currency", "")
        ))

    conn.commit()
    conn.close()
    print(f"✔ SQLite DB created: {DB_FILE}")
# =============================================================
# ENDPOINT DELL'API
# =============================================================



#@app.get("/api/populate", tags=["Frontend"])
#def DISABLED():
    ##populateDb()
#    return {"message": "Database populated with ETF data."}
@app.get("/api/TEST", tags=["Frontend"])
def TEST(q: str = Query(..., min_length=1)):
    return {
        "message": {"I have a test suite, yuhuuuu!": q},
    }

@app.get("/api/count", tags=["Frontend"])
def count_etfs():
    conn = get_db_connection()
    cursor = conn.execute("SELECT COUNT(*) as count FROM etfs")
    return {
        "count": cursor.fetchone()["count"],
        "description": "Total number of ETFs stored in the SQLite FTS5 database."
    }

@app.get("/api/count_providers", tags=["Frontend"])
def count_families():
    conn = get_db_connection()
    cursor = conn.execute("SELECT COUNT(DISTINCT family) AS count FROM etfs")
    return {
        "count": cursor.fetchone()["count"],
        "description": "Total number of Providers stored in the SQLite FTS5 database."
    }


@app.get("/api/count_exchange", tags=["Frontend"])
def count_exchange():
    conn = get_db_connection()
    cursor = conn.execute("SELECT COUNT(DISTINCT exchange) AS count FROM etfs")
    return {
        "count": cursor.fetchone()["count"],
        "description": "Total number of Exchanges stored in the SQLite FTS5 database."
    }


@app.get("/api/families", tags=["Frontend"])
def get_families():
    conn = get_db_connection()
    rows = conn.execute("""
        SELECT DISTINCT family
        FROM etfs
        WHERE family IS NOT NULL AND family != ''
        ORDER BY family COLLATE NOCASE
    """).fetchall()
    return [row["family"] for row in rows]
    

@app.get("/api/exchanges", tags=["Frontend"])
def get_exchanges():
    conn = get_db_connection()
    rows = conn.execute("""
        SELECT DISTINCT exchange
        FROM etfs
        WHERE exchange IS NOT NULL AND exchange != ''
        ORDER BY exchange COLLATE NOCASE
    """).fetchall()
    exchanges = []
    for row in rows:
        code = row["exchange"]
        exchanges.append({
            "code": code,
            "name": exchange_map.get(code, code)  # fallback to code if name unknown
        })

    return exchanges

@app.get("/api/holdings/{Ticker}", response_model=List[EtfSearchResult], tags=["ETF"])
def search_etf(Ticker: str, api_key: str = Depends(get_api_key)):   
    """
    Local Cached search. Look only for ETF in the local database.
    Return the list of holdings for the given ETF ticker.
    """
    logging.info(f"Richiesta Holding per ticker: {Ticker}")
    # Search per ETF nel database
    conn = get_db_connection()
    cursor = conn.cursor()
    search_term = Ticker.upper()
    cursor.execute("SELECT * FROM RawHoldings WHERE EtfTicker LIKE ?", (search_term,))
    rows = cursor.fetchall()
    conn.close()
    
    if not rows:
        fetch_holdings(search_term, api_key)
        # Riprova a cercare dopo il fetch
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM RawHoldings WHERE EtfTicker LIKE ?", (search_term,))
        rows = cursor.fetchall()
        conn.close()
    etf_dict: Dict[str, List[Holding]] = {}

    for row in rows:
        ticker = row["EtfTicker"]
        holding = Holding(
            CompanyName=row["CompanyName"],
            Weight=float(row["Weight"])
        )
        etf_dict.setdefault(ticker, []).append(holding)

    results = [
        EtfSearchResult(Ticker=ticker, Holdings=holdings)
        for ticker, holdings in etf_dict.items()
    ]
    return results

@app.get("/api/search", tags=["Frontend"])
def search_etfs_fts5(
    q: Optional[str] = Query(None),
    limit: int = 50,
    provider: Optional[str] = None,
    exchange: Optional[str] = None,
):
    logging.info(f"Search query: {q}, Limit: {limit}, Provider: {provider}, Exchange: {exchange}")
    load_gaza_descriptions()

    conn = get_db_connection()
    print(f"Search query: {q}, Limit: {limit}, Provider: {provider}, Exchange: {exchange}")
    results = []
    seen_symbols = set()

    # Prepare dynamic filters
    fts_filters = []
    if provider:
        fts_filters.append(f"family:{provider}*")
    if exchange:
        fts_filters.append(f"exchange:{exchange}*")
    filter_str = " AND ".join(fts_filters)

    def build_match_query(base: str):
        return f"{base} AND {filter_str}" if filter_str else base
    if q:    
        q_upper = q.upper()   
        # 1. Exact symbol match
        query_1 = """
            SELECT *, bm25(etfs) AS rank
            FROM etfs
            WHERE symbol = ?
            LIMIT 1
        """
        for row in conn.execute(query_1, (q_upper,)).fetchall():
            results.append(dict(row))
            seen_symbols.add(row["symbol"])
        # 2. Prefix symbol match
        match_2 = build_match_query(f"symbol:{q_upper}*")
        query_2 = """
            SELECT *, bm25(etfs) AS rank
            FROM etfs
            WHERE etfs MATCH ?
            ORDER BY rank
            LIMIT ?
        """
        for row in conn.execute(query_2, (match_2, limit)).fetchall():
            if row["symbol"] not in seen_symbols and row["symbol"] != q_upper:
                results.append(dict(row))
                seen_symbols.add(row["symbol"])
        # 3. Name match
        match_3 = build_match_query(f"name:{q}*")
        for row in conn.execute(query_2, (match_3, limit)).fetchall():
            if row["symbol"] not in seen_symbols:
                results.append(dict(row))
                seen_symbols.add(row["symbol"])

        # 4. Other fields
        match_4 = build_match_query(
            f"summary:{q}* OR category_group:{q}* OR category:{q}* OR family:{q}* OR exchange:{q}* OR currency:{q}*"
        )
        for row in conn.execute(query_2, (match_4, limit)).fetchall():
            if row["symbol"] not in seen_symbols:
                results.append(dict(row))
                seen_symbols.add(row["symbol"])
    if not q:
        print("No query provided, fetching all ETFs with filters.")
        # Build basic SQL WHERE clauses
        filters = []
        params = []

        if provider:
            filters.append("family LIKE ?")
            params.append(f"{provider}%")
        if exchange:
            filters.append("exchange LIKE ?")
            params.append(f"{exchange}%")

        where_clause = " AND ".join(filters) if filters else "1=1"
        query = f"""
            SELECT * FROM etfs
            WHERE {where_clause}
            LIMIT ?
        """
        params.append(limit)

        for row in conn.execute(query, tuple(params)).fetchall():
            results.append(dict(row))
            seen_symbols.add(row["symbol"])
    
    ticker_list = [row["symbol"] for row in results[:limit]]
    if not ticker_list:
        conn.close()
        return []

    # Build the analysis query for all tickers at once
    placeholders = ",".join(["?"] * len(ticker_list))
    analysis_query = f"""
        SELECT 
            ar.Ticker, 
            e.name as ETFName,
            e.Category,
            e.Family,
            e.summary,
            e.exchange,
            e.currency,
            e.category_group,
            ar.FossilFuelsPercentage, 
            ar.FossilFuelsHoldings, 
            ar.WeaponsPercentage, 
            ar.WeaponsHoldings, 
            ar.GazaListPercentage, 
            ar.GazaListHoldings
        FROM 
            AnalysisResults ar
        JOIN 
            etfs e ON ar.Ticker = e.symbol
        WHERE e.symbol IN ({placeholders})
    """
    analysis_rows = conn.execute(analysis_query, tuple(ticker_list)).fetchall()
    analysis_map = {row["Ticker"]: row for row in analysis_rows}

    enriched_results = []
    for row in results[:limit]:
        ticker = row["symbol"]
        analysis_row = analysis_map.get(ticker)
        if analysis_row:
            code = analysis_row["exchange"]
            exchange_str = exchange_map.get(code, code)
            gaza_holdings = json.loads(analysis_row["GazaListHoldings"])
            summary_description = ""
            gaza_descriptions = []
            for company in gaza_holdings:
                company_lower = company.lower()
                for record in gaza_company_descriptions:
                    if record["CompanyName"].lower() in company_lower:
                        gaza_descriptions.append(f"{record['CompanyName']}: {record['Description']}")
                        break
            if gaza_descriptions:
                summary_description = "<ul>"
                for item in gaza_descriptions:
                    if ":" in item:
                        company, desc = item.split(":", 1)
                        summary_description += f"<li><strong>{company.strip()}:</strong>{desc}</li>"
                    else:
                        summary_description += f"<li>{item}</li>"
                summary_description += "</ul>"
            enriched_results.append({
                "Ticker": analysis_row["Ticker"],
                "ETFName": analysis_row["ETFName"],
                "FossilFuelsPercentage": analysis_row["FossilFuelsPercentage"] * 100,
                "FossilFuelsHoldings": json.loads(analysis_row["FossilFuelsHoldings"]),
                "WeaponsPercentage": analysis_row["WeaponsPercentage"] * 100,
                "WeaponsHoldings": json.loads(analysis_row["WeaponsHoldings"]),
                "GazaListPercentage": analysis_row["GazaListPercentage"] * 100,
                "GazaListHoldings": gaza_holdings,
                "category": analysis_row["Category"],
                "family": analysis_row["Family"],
                "summary": summary_description.strip(),
                "exchange": exchange_str,
                "currency": analysis_row["currency"],
                "category_group": analysis_row["category_group"]
            })
        else:
            enriched_results.append(row)
    conn.close()
    return enriched_results

    
@app.get("/api/etf/{ticker}", response_model=EtfDetailsResponse, tags=["ETF"])
def get_etf_details(ticker: str, api_key: str = Depends(get_api_key)):
    """
    Restituisce i dati di analisi per un ticker. Se non è nel DB,
    tenta di scaricarlo, analizzarlo e salvarlo.
    """
    print(f"Richiesta dettagli ETF per ticker: {ticker}")
    logging.info(f"Richiesta dettagli ETF per ticker: {ticker}")

    conn = get_db_connection()
    cursor = conn.cursor()
    #cursor.execute("SELECT * FROM AnalysisResults WHERE Ticker = ?", (ticker.upper(),))
    cursor.execute("""
    SELECT 
        ar.Ticker, 
        e.name AS ETFName,
        e.Category,
        e.Family,
        e.summary,
        e.exchange,
        e.currency,
        e.category_group,
        ar.FossilFuelsPercentage, 
        ar.FossilFuelsHoldings, 
        ar.WeaponsPercentage, 
        ar.WeaponsHoldings, 
        ar.GazaListPercentage, 
        ar.GazaListHoldings
    FROM 
        AnalysisResults ar
    JOIN 
        etfs e ON ar.Ticker = e.symbol
    WHERE 
        ar.Ticker = ?
""", (ticker.upper(),))

    result = cursor.fetchone()
    conn.close()
    
    if result is None:
        # Se non trovato, esegui l'analisi on-demand
        result = fetch_and_analyze_ticker(ticker.upper(), api_key)
        if result is None:
            raise HTTPException(status_code=404, detail=f"ETF with ticker '{ticker}' non trovato nel database locale né su Alpha Vantage.")
    logging.info("result=")
    logging.info(dict(result))
    
    gaza_holdings = json.loads(result["GazaListHoldings"])

    # Build summary by appending company descriptions
    summary_description =  ""
    gaza_descriptions = []
    for company in gaza_holdings:
        company_lower = company.lower()
        for record in gaza_company_descriptions:
            # Check if the blacklist company name is contained in the holding company name
            if record["CompanyName"].lower() in company_lower:
                gaza_descriptions.append(f"{record['CompanyName']}: {record['Description']}")
                break
    if gaza_descriptions:
        summary_description = "<ul>"
        for item in gaza_descriptions:
            if ":" in item:
                company, desc = item.split(":", 1)
                summary_description += f"<li><strong>{company.strip()}:</strong>{desc}</li>"
            else:
                summary_description += f"<li>{item}</li>"
        summary_description += "</ul>"


    print(result)
    
    #Qui si incarta sempre
    response_data = {
"Ticker": result["ticker"] ,
"ETFName": safe_get(result, "ETFName"),
"FossilFuelsPercentage": result["FossilFuelsPercentage"] * 100,
"FossilFuelsHoldings": json.loads(result["FossilFuelsHoldings"]),
"WeaponsPercentage": result["WeaponsPercentage"] * 100,
"WeaponsHoldings": json.loads(result["WeaponsHoldings"]),
"GazaListPercentage": result["GazaListPercentage"] * 100,
"GazaListHoldings": json.loads(result["GazaListHoldings"]),
"category": safe_get(result, "Category"),
"family": safe_get(result, "Family"),
"summary": summary_description,
"exchange": safe_get(result, "exchange"),
"currency": safe_get(result, "currency"),
"category_group": safe_get(result, "category_group")
}


    return response_data


    
@app.get("/api/worst_etfs", response_model=List[EtfDetailsResponse], tags=["ETF"])
def Worst_etf():
    load_gaza_descriptions()
    """
    Shows the 50 ETFs WORST cached in search results, ordered by GazaListPercentage.
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    # Query the AnalysisResults table for the top 50 ETFs by GazaListPercentage
    # We order by GazaListPercentage in descending order to get the "worst"
    # and limit to 50 results.
    cursor.execute("""
    SELECT 
        ar.Ticker, 
        e.name as ETFName,
        e.Category,
        e.Family,
		e.summary,
		e.exchange,
		e.currency,
		e.category_group,
        ar.FossilFuelsPercentage, 
        ar.FossilFuelsHoldings, 
        ar.WeaponsPercentage, 
        ar.WeaponsHoldings, 
        ar.GazaListPercentage, 
        ar.GazaListHoldings
    FROM 
        AnalysisResults ar
    JOIN 
        etfs e ON ar.Ticker = e.symbol
    ORDER BY 
        ar.GazaListPercentage DESC
    LIMIT 50
    """)
    rows = cursor.fetchall()
    conn.close()

    results = []
    for row in rows:
        code = row["exchange"]
        exchange_str = exchange_map.get(code, code)

        gaza_holdings = json.loads(row["GazaListHoldings"])

        # Build summary by appending company descriptions
        summary_description =  ""
        gaza_descriptions = []

        for company in gaza_holdings:
            company_lower = company.lower()
            for record in gaza_company_descriptions:
                # Check if the blacklist company name is contained in the holding company name
                if record["CompanyName"].lower() in company_lower:
                    gaza_descriptions.append(f"{record['CompanyName']}: {record['Description']}")
                    break
        if gaza_descriptions:
            print("TRUE")
            summary_description = "<ul>"
            for item in gaza_descriptions:
                if ":" in item:
                    company, desc = item.split(":", 1)
                    summary_description += f"<li><strong>{company.strip()}:</strong>{desc}</li>"
                else:
                    summary_description += f"<li>{item}</li>"
            summary_description += "</ul>"

        response_data = EtfDetailsResponse(
            Ticker=row["Ticker"],
            ETFName=row["ETFName"],
            FossilFuelsPercentage=row["FossilFuelsPercentage"] * 100,
            FossilFuelsHoldings=json.loads(row["FossilFuelsHoldings"]),
            WeaponsPercentage=row["WeaponsPercentage"] * 100,
            WeaponsHoldings=json.loads(row["WeaponsHoldings"]),
            GazaListPercentage=row["GazaListPercentage"] * 100,
            GazaListHoldings=gaza_holdings,
            category=row["Category"],
            family=row["Family"],
            summary=summary_description.strip(),
            exchange=exchange_str,
            currency=row["currency"],
            category_group=row["category_group"]
        )

        results.append(response_data)
    return results



#________________________________________________
exchange_map = {
    "AMS": "Euronext Amsterdam",
    "AQS": "Alternative Quotation System (OTC)",
    "ASE": "NYSE American",
    "ASX": "Australian Securities Exchange",
    "ATH": "Athens Stock Exchange",
    "BER": "Börse Berlin",
    "BRU": "Euronext Brussels",
    "BTS": "Börse Stuttgart",
    "BUD": "Budapest Stock Exchange",
    "CPH": "Nasdaq Copenhagen",
    "DJI": "Dow Jones Industrial Average (Index)",
    "DOH": "Qatar Stock Exchange",
    "DUS": "Börse Düsseldorf",
    "EBS": "Vienna Exchange (EBS)",
    "FRA": "Deutsche Börse Frankfurt (Xetra)",
    "GER": "Generic Germany (Various)",
    "HAM": "Börse Hamburg",
    "HAN": "Börse Hannover",
    "HEL": "Nasdaq Helsinki",
    "ICE": "Nasdaq Iceland",
    "ISE": "Irish Stock Exchange",
    "IST": "Borsa Istanbul",
    "JPX": "Japan Exchange Group (Tokyo)",
    "KSC": "Korea Stock Exchange (KRX)",
    "LIS": "Euronext Lisbon",
    "LIT": "Nasdaq Vilnius",
    "LSE": "London Stock Exchange",
    "MCE": "Bolsa de Madrid",
    "MCX": "Moscow Exchange",
    "MEX": "Bolsa Mexicana de Valores",
    "MIL": "Borsa Italiana Milan",
    "MUN": "Börse München",
    "NCM": "Nasdaq Capital Market",
    "NEO": "NEO Exchange (Canada)",
    "NGM": "Nordic Growth Market",
    "NIM": "Nasdaq OMX Nordic",
    "NMS": "Nasdaq Global Market",
    "NYQ": "New York Stock Exchange",
    "OSL": "Oslo Børs",
    "PAR": "Euronext Paris",
    "PCX": "NYSE Arca",
    "PNK": "OTC Markets (Pink Sheets)",
    "PRA": "Prague Stock Exchange",
    "SAU": "Tadawul (Saudi Stock Exchange)",
    "SES": "Singapore Exchange",
    "SET": "Stock Exchange of Thailand",
    "STO": "Nasdaq Stockholm",
    "TAI": "Taiwan Stock Exchange",
    "TLV": "Tel Aviv Stock Exchange",
    "TOR": "Toronto Stock Exchange",
    "TWO": "Taipei Exchange",
    "VIE": "Vienna Stock Exchange"
}

def exchange_code_to_name(code: str) -> str:
    """
    Convert exchange code (e.g. 'NYQ') to full exchange name (e.g. 'New York Stock Exchange').
    Falls back to original code if not found.
    """
    return exchange_map.get(code.upper(), code)


def exchange_name_to_code(name: str) -> str:
    """
    Convert full exchange name to exchange code.
    Falls back to original name if not found.
    """
    name_lower = name.strip().lower()
    for code, full in exchange_map.items():
        if full.lower() == name_lower:
            return code
    return name

if __name__ == "__main__":
    import uvicorn
    print("Avvio il server API...")
    load_gaza_descriptions()
    uvicorn.run(app, host="127.0.0.1", port=8000, debug=False, docs_url="/docs")