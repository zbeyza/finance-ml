import argparse # CLI argument parser
import os
from html.parser import HTMLParser # HTML parser for parsing the wikipedia sp500 table
import requests # HTTP requests for wikipedia page fetching 
import pandas as pd


WIKI_SP500_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

class SP500TableParser(HTMLParser):
    # minimal html table parser for the wikipedia sp500 table
    def __init__(self):
        super().__init__() # initialize the base HTMLParser
        self.in_table = False # track whether we are inside the target table
        self.table_depth = 0 # track nested table depth
        self.in_cell = False # track whether we are inside a table cell
        self.current_cell = [] # collect text for current cell
        self.current_row = [] # collect cells for current row
        self.rows = [] # store parsed table rows
        self.headers = None  # store table headers
        self.symbol_col = None # store the Symbol column index

    def handle_starttag(self, tag, attrs):
        if tag == "table": # check table start
            attr_dic = dict(attrs) # map attributes
            if attr_dic.get("id") == "constituents": # target table id
                self.in_table = True # enter target table
                self.table_depth = 1  # initialize depth
            elif self.in_table: # nested table inside target
                self.table_depth += 1 # increment depth
        if not self.in_table: # ignore tags outside target table
            return
        if tag == "tr": # row start (tr: table row)
            self.current_row = [] # reset the row buffer
        if tag in ("th", "td"): # table header/data cell (th: table header, td: table data)
            self.in_cell = True # enter cell
            self.current_cell = [] # reset cell buffer
    
    def handle_endtag(self, tag):
        if not self.in_table: # ingnore tags outside target table 
            return
        if tag in ("th", "td") and self.in_cell: # cell end
            cell_text = " ".join(self.current_cell).strip() # join cell text
            self.current_row.append(cell_text) # add cell to row
            self.in_cell = False # exit cell
            self.current_cell = [] # clear cell buffer
        if tag == "tr" and self.current_row: # row end with content
            if self.headers is None:  # first row is header row
                # Header row: capture all column names and locate Symbol index.
                self.headers = self.current_row  # store header names
                for i, value in enumerate(self.headers):  # scan for Symbol
                    if value.strip().lower() == "symbol":  # match Symbol
                        self.symbol_col = i  # store Symbol column index
                        break
            else:
                self.rows.append(self.current_row)  # store data row
            self.current_row = []  # reset row buffer
        if tag == "table" and self.in_table:  # table end
            self.table_depth -= 1  # decrement depth
            if self.table_depth <= 0:  # left target table
                self.in_table = False  # exit target table

    def handle_data(self, data):
        if self.in_table and self.in_cell:  # only collect data inside cells
            self.current_cell.append(data)  # append cell text


def _parse_table_with_pandas(html_text):
    # Preferred path when pandas has an HTML parser installed.
    tables = pd.read_html(html_text, attrs={"id": "constituents"})  # read tables
    return tables[0]  # return the first matching table


def _parse_table_with_html_parser(html_text):
    # Fallback parser that avoids optional dependencies.
    parser = SP500TableParser()  # create parser
    parser.feed(html_text)  # parse HTML
    if not parser.headers:  # ensure headers found
        raise ValueError("table headers not found in Wikipedia table")
    rows = []  # collect rows for DataFrame
    for row in parser.rows:  # iterate parsed rows
        if len(row) < len(parser.headers):  # pad missing columns
            row = row + [""] * (len(parser.headers) - len(row))
        rows.append(row[: len(parser.headers)])  # ensure correct width
    return pd.DataFrame(rows, columns=parser.headers)  # build DataFrame


def fetch_sp500_table(url=WIKI_SP500_URL):
    # Download Wikipedia page and extract the constituents table.
    headers = {
        "User-Agent": "finance-sp500-fetch/1.0 (+https://example.com)"  # avoid 403
    }
    resp = requests.get(url, headers=headers, timeout=30)  # fetch HTML
    resp.raise_for_status()  # raise for HTTP errors
    html_text = resp.text  # page HTML
    try:
        table = _parse_table_with_pandas(html_text)  # parse with pandas
    except Exception:
        # pandas may fail if optional HTML parsing deps are missing.
        table = _parse_table_with_html_parser(html_text)  # fallback parser
    return table  # return full constituents table


def _symbol_column_name(df):
    for col in df.columns:  # scan columns
        if str(col).strip().lower() == "symbol":  # match Symbol column
            return col  # return column name
    raise ValueError("symbol column not found in Wikipedia table")


def normalize_symbols(symbols, mode="none"):
    if mode == "none":  # no normalization
        return symbols
    if mode == "yahoo":  # Yahoo ticker formatting
        # Yahoo uses dashes instead of dots for tickers like BRK.B.
        return [s.replace(".", "-") for s in symbols]
    raise ValueError(f"unknown normalization mode: {mode}")


def fetch_sp500_symbols(url=WIKI_SP500_URL, normalize="none"):
    table = fetch_sp500_table(url=url)  # get full table
    symbol_col = _symbol_column_name(table)  # find Symbol column
    symbols = table[symbol_col].astype(str).tolist()  # extract symbols
    symbols = [s.strip() for s in symbols if s and s.strip()]  # clean blanks
    # Preserve order while removing duplicates.
    symbols = list(dict.fromkeys(symbols))  # de-dup in order
    return normalize_symbols(symbols, mode=normalize)  # normalize if needed


def write_symbols_csv(symbols, out_path):
    # Emit a one-column CSV of symbols.
    df = pd.DataFrame({"symbol": symbols})  # build symbols DataFrame
    df.to_csv(out_path, index=False)  # write CSV


def write_table_csv(table, out_path):
    table.to_csv(out_path, index=False)  # write full table CSV


def main():
    parser = argparse.ArgumentParser(description="Fetch current S&P 500 constituents.")  # CLI
    parser.add_argument("--symbols-out", default="data/universe_sp500.csv", help="Symbols CSV output path.")
    parser.add_argument("--info-out", default="data/sp500_info.csv", help="Full table CSV output path.")
    parser.add_argument("--normalize", default="none", choices=["none", "yahoo"])  # normalize mode
    args = parser.parse_args()  # parse CLI args

    for path in (args.symbols_out, args.info_out):
        out_dir = os.path.dirname(path)
        if out_dir:
            os.makedirs(out_dir, exist_ok=True)
    table = fetch_sp500_table()  # fetch full table
    symbols = fetch_sp500_symbols(normalize=args.normalize)  # fetch symbols
    write_symbols_csv(symbols, args.symbols_out)  # write symbols CSV
    write_table_csv(table, args.info_out)  # write full table CSV


if __name__ == "__main__":
    main()  # run CLI entrypoint

        
