from typing import List
from config.symbols import blacklist, us, enhanced_symbols


def find_all(p: str, s: str):
    '''Yields all the positions of  the pattern p in the string s.'''
    i = s.find(p)
    while i != -1:
        yield i
        i = s.find(p, i+1)


def extract_tickers(text: str, use_companies: bool = False, use_tickers: bool = True) -> List[str]:
    symbol_data = enhanced_symbols()
    mentioned_symbols = []
    if use_tickers: mentioned_symbols = [word for word in text.split() if word.isupper() and len(word) <= 5 and word not in blacklist and word in us]

    if use_companies:
        mentioned_companies = [(sym, company) for sym, company in symbol_data.companies.items() if company in text]
        for sym, company in mentioned_companies:
            mentions = list(find_all(text, company))
            if any([not text[m_start + len(company)].isalpha() for m_start in mentions]): mentioned_symbols.append(sym)
            
    return set(mentioned_symbols)


def extract_market_segment(text: str) -> List[str]:
    return []

