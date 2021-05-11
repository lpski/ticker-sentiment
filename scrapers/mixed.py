from typing import Any, Callable, Dict, List, OrderedDict, Tuple
import json, requests, datetime as dt, xmltodict, pandas as pd, threading, time, calendar, hashlib, os, asyncio, re, numpy as np
from urllib.parse import quote_plus
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from timeit import default_timer as timer

from scrapers.utils import extract_tickers
from scrapers.fetching import fetch_article
from data.es import add_analyses
from config.symbols import us, extended_blacklist, focus_symbols
from config.models import Analysis, SentimentContext

EXECUTOR = ThreadPoolExecutor(max_workers=6)


# Helpers
def get_state(source: str) -> Dict[str, Any]:
    if not os.path.exists(f'scrapers/fetched/mixed/{source}.json'):
        save_state(source, {})

    with open(f'scrapers/fetched/mixed/{source}.json', 'r') as f:
        try: state = json.loads(f.read())
        except: state = {}
    return state

def save_state(source: str, state: dict):
    with open(f'scrapers/fetched/mixed/{source}.json', 'w') as f:
        try: f.write(json.dumps(state, indent=4))
        except Exception as e: raise Exception(f'Error updating {source} state:', e)





def seeking_alpha(mode: str = 'all'):
    """
    seeking_alpha monthly article feeds: https://seekingalpha.com/article/index.xml
        - Monthly article feeds: https://seekingalpha.com/article/YYYY_M.xml format (e.g. https://seekingalpha.com/article/2021_3.xml)
            - Earliest: 2005_8
        - Monthly news feeds: https://seekingalpha.com/news/YYYY_M.xml format (e.g. https://seekingalpha.com/news/2021_3.xml)
        - blog feeds: https://seekingalpha.com/instablog/2021_3.xml
        - symbol-specific news: https://seekingalpha.com/api/v3/symbols/ual/news?cacheBuster=2021-03-10&filter[until]=1612965520&id=ual&include=author%2CprimaryTickers%2CsecondaryTickers%2Csentiments&isMounting=false&page[size]=20
        - ^^                    https://seekingalpha.com/api/v3/symbols/ual/news?id=ual&include=primaryTickers%2CsecondaryTickers%2Csentiments&isMounting=false&page[size]=300
        - Robots.txt: https://seekingalpha.com/robots.txt
    """
    state = get_state('seeking_alpha')
    if 'fetched' not in state: state['fetched'] = []
 
    # Set up fetch range
    cur_year, cur_month = dt.datetime.now().year, dt.datetime.now().month
    years, months = [y for y in range(cur_year, (2015 if mode == 'all' else cur_year - 1), -1)], [m for m in (range(1, 13) if mode == 'all' else range(cur_month - 1, cur_month + 1))]
    periods = [f'{y}_{m}' for m in months for y in years if y < cur_year or m <= cur_month]

    # Build up urls + timestamps
    vader = SentimentIntensityAnalyzer()
    for period in periods:
        for category in ['article', 'news', 'instablog']:
            analysis = Analysis()
            sitemap_url = f'https://seekingalpha.com/{category}/{period}.xml'

            raw_articles = requests.get(sitemap_url).text
            raw_articles: OrderedDict = xmltodict.parse(raw_articles)['urlset']
            print(f'{period}: {len(raw_articles["url"])} articles')

            # Start extraction
            articles: List[Tuple[str, str, int]] = []
            for url in raw_articles['url']:
                try:
                    article_id: str = url['loc'].split('/')[-1].split('-')[0]
                    if article_id in state['fetched']: continue

                    timestamp = int(dt.datetime.strptime(url['lastmod'], '%Y-%m-%dT%H:%M:%S%z').replace(tzinfo=dt.timezone.utc).timestamp())
                    articles.append((url['loc'], article_id, timestamp))
                except Exception as e:
                    print('Error parsing seeking alpha sitemap article for:', url, e)


            # Start Analysis
            for article in articles:
                url, article_id, timestamp = article
                content: BeautifulSoup = fetch_article(url, 'seeking_alpha')
                if content is None or content == '': continue

                link_texts = [(a['href'], a.text) for a in content.select('div[data-test-id="content-container"] a')]
                symbols = [text for url, text in link_texts if text in us and '/symbol' in url]
                

                text_blocks = [block.text for block in content.find_all('div', attrs={'data-test-id' : 'content-container'})]
                article_content = ''.join(text_blocks)
                
                if len(symbols) > 0:
                    sentiment = vader.polarity_scores(article_content)
                    print(f'seeking alpha | {len(symbols)} symbols: {symbols} | {url} | {sentiment}')
                    for symbol in symbols: analysis.data.append(SentimentContext(symbol, 'seeking_alpha', timestamp, sentiment['compound']))
                    add_analyses([analysis])

                state['fetched'].append(article_id)
                save_state('seeking_alpha', state)



def cnbc(mode: str = 'all'):
    """
    Articles can be fetched on a daily basis:
        - https://www.cnbc.com/site-map/articles/2021/March/10/
    """
    state = get_state('cnbc')
    if 'fetched' not in state: state['fetched'] = []

    cur_year, cur_month, cur_day = dt.datetime.now().year, dt.datetime.now().month, dt.datetime.now().day
    if mode == 'all':
        years, months, days = [y for y in range(cur_year, 2015, -1)], [m for m in range(1, 13)], [d for d in range(1, 32)]
    else:
        from_month = cur_month if cur_month == 1 or cur_day > 3 else cur_month - 1
        to_day = cur_day + 1 if from_month == cur_month else calendar.monthrange(cur_year, from_month)[1] + 1
        from_day = cur_day - 3 if from_month == cur_month else to_day - abs(cur_day - 3) - 1
        years, months, days = [y for y in range(cur_year, cur_year - 1, -1)], [m for m in range(from_month, cur_month + 1)], [d for d in range(from_day, to_day)]
    periods = [f'{y}/{calendar.month_name[m]}/{d}/' for y in years for m in months for d in days if y < cur_year or (y == cur_year and (m < cur_month or (m == cur_month and d <= cur_day)))]

    # Build up urls + timestamps
    vader = SentimentIntensityAnalyzer()
    for period in periods:
        sitemap_url = f'https://www.cnbc.com/site-map/articles/{period}'

        res = requests.get(sitemap_url).text
        soup = BeautifulSoup(res, 'html.parser')
        links: List[str] = [(a['href'], quote_plus(a['href']).encode('utf8')) for a in soup.find_all('a', { 'class': 'SiteMapArticleList-link' })]
        articles: List[Tuple[str, str]] = [(link, hashlib.md5(text).hexdigest()) for link, text in links]

        # print(f'cnbc | {period}: {len(articles)} articles')
        for link, article_id in articles:
            try:
                if article_id in state['fetched']: continue
                else: state['fetched'].append(article_id)
                save_state('cnbc', state)

                analysis = Analysis()
                link_content = requests.get(link).text
                soup = BeautifulSoup(link_content, 'html.parser')
                tags = [t['content'] for t in soup.find_all('meta', {'property': 'article:tag'})]
                timestamp = soup.find('meta', {'property': 'article:published_time'})['content']
                timestamp = int(dt.datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S%z').replace(tzinfo=dt.timezone.utc).timestamp())

                symbols = extract_tickers(' '.join(tags), True)
                symbols = [s for s in symbols if s not in ['NWSA', 'NWS']]
                if len(symbols) > 0:
                    article_content = soup.find('div', { 'class': 'ArticleBody-articleBody'})
                    if article_content is None:
                        # Try alternate parse strategy
                        article_content = soup.find('div', { 'data-module': 'ArticleBody'})
                        if article_content is None: article_content = soup.find('div', { 'class': 'PageBuilder-article' })
                        if article_content is None: # Still unsucceful -> skip
                            print(f'unknown page format: {link}')
                            continue
                            
                    sentiment = vader.polarity_scores(article_content.text)
                    for symbol in symbols: analysis.data.append(SentimentContext(symbol, 'cnbc', timestamp, sentiment['compound']))
                    add_analyses([analysis])

                    print(f'\tcnbc: sentiment {sentiment["compound"]} | symbols: {symbols}')

            except Exception as e:
                print(f'Error parsing cnbc link: {link} | ', e)
                continue


    print('\tcnbc | analysis complete')


def _ap_process_article(article):
    analysis = Analysis()
    vader = SentimentIntensityAnalyzer()
    try:
        url, _ = article
        link_content = requests.get(url).text
        soup = BeautifulSoup(link_content, 'html.parser')
        content: str = soup.find('div', { 'class': 'Article' }).text
        tags = [tag['content'].lower() for tag in soup.find_all('meta', { 'property': 'article:tag' })]
        if any([s in ' '.join(tags) for s in ['ball', 'sport', 'soccer', 'television']]): return

        timestamp = soup.find('meta', {'property': 'article:published_time'})['content']
        timestamp = int(dt.datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=dt.timezone.utc).timestamp())

        # custom ap symbol extraction
        symbols = set([])
        for exchange_prefix in ['NASDAQ', 'NYSE']:
            matches = re.findall(f'\({exchange_prefix} *: *([a-zA-Z]*)', content)
            for match in matches: symbols.add(match)

        if len(symbols) > 0:
            sentiment = vader.polarity_scores(content)
            for symbol in symbols: analysis.data.append(SentimentContext(symbol, 'ap', timestamp, sentiment['compound']))
            add_analyses([analysis])

            print(f'\tap: sentiment {sentiment["compound"]} | symbols: {symbols} | {url}\n')

    except Exception as e:
        print(f'ap:parse article error for link: {url}  |  ', e)
        return

def ap(mode: str = 'all'):
    """
    Includes content from various other producers (e.g. globe_newswire, pr_newswire)
    in addition to first-party content.
        - A bit figity to deal with, sitemap located here: https://apnews.com/sitemap/sitemap_index.xml
        - per day articles available in format:
            - https://apnews.com/sitemap/sitemap_2021-02-11T05:00:00+00:00.xml
            - https://apnews.com/sitemap/sitemap_2021-02-13T05:00:05+00:00.xml
            - As seen in the prev two examples the digit prior to the timezone is varying so check variations

    """
    state = get_state('ap')
    if 'fetched' not in state: state['fetched'] = []
    print(f'ap | starting analysis')
    
    cur_year, cur_month, cur_day = dt.datetime.now().year, dt.datetime.now().month, dt.datetime.now().day
    if mode == 'all':
        years, months, days = [y for y in range(cur_year, 2018, -1)], [m for m in range(1, 13)], [d for d in range(1, 32)]
    else:
        from_month = cur_month if cur_month == 1 or cur_day > 3 else cur_month - 1
        to_day = cur_day + 1 if from_month == cur_month else calendar.monthrange(cur_year, from_month)[1] + 1
        from_day = cur_day - 3 if from_month == cur_month else to_day - abs(cur_day - 3) - 1
        years, months, days = [y for y in range(cur_year, cur_year - 1, -1)], [m for m in range(from_month, cur_month + 1)], [d for d in range(from_day, to_day)]

    # We create period strings up to the varying point (minutes): e.g. 2021-02-11T05:00:
    periods = [f'{y}-{m if m >= 10 else f"0{m}"}-{d if d >= 10 else f"0{d}"}' for y in years for m in months for d in days if y < cur_year or (y == cur_year and m <= cur_month and (m < cur_month or d <= cur_day))]

    # Build up urls + timestamps
    for period in periods:
        articles: List[Tuple[str, str]] = [] # list of [(url, id)]

        # Find a valid sitemap
        link_options = [f'https://apnews.com/sitemap/sitemap_{period}T05:00:{n if n >= 10 else f"0{n}"}+00:00.xml' for n in range(0, 60)]
        link_options += [f'https://apnews.com/sitemap/sitemap_{period}T04:00:{n if n >= 10 else f"0{n}"}+00:00.xml' for n in range(0, 60)]
        for link in link_options:
            res = requests.get(link)
            if res.status_code == 200: # Found valid url

                sitemap = res.text
                urls: OrderedDict = xmltodict.parse(sitemap)
                # if 'urlset' not in urls or 'url' not in urls['urlset']: continue

                # print(f'ap | checking {len(urls["urlset"]["url"])} articles')
                for url in urls['urlset']['url']:
                    try:
                        article_path = url['loc'].split('/')[-1].split('-')
                        if len(article_path) == 1: continue # Links with only id value aren't relevant
                        article_id: str = article_path[-1]
                        # if article_id in state['fetched'] or not any([s in url['loc'] for s in ['/press-release/', '/article/']]): continue
                        if article_id in state['fetched'] or not any([s in url['loc'] for s in ['/press-release/']]): continue
                        if any([s in url['loc'] for s in [
                            'sports', 'basketball', 'lottery', 'sport', 'ball', 'score', 'scores', 'shooting', 'deportes', 'nfl',
                            'nba', 'nhl', 'mlb', 'archive', 'history', 'broadcast'
                        ]]): continue
                        articles.append((url['loc'], article_id))
                    except:
                        print('Error parsing ap link:', url)
                        continue

                break
        
        if len(articles) == 0: continue
        print(f'ap | {period}: {len(articles)} new articles')

        try: loop = asyncio.get_event_loop()
        except: loop = asyncio.new_event_loop()
        # loop.run_until_complete(asyncio.wait([loop.run_in_executor(EXECUTOR, _ap_process_article, a) for a in articles[:8]]))
        loop.run_until_complete(asyncio.wait([loop.run_in_executor(EXECUTOR, _ap_process_article, a) for a in articles]))
        state['fetched'] += [a[1] for a in articles]
        save_state('ap', state)

    print('\tap | analysis complete')


def benzinga(mode: str = 'all'):
    """

    news api:  https://api.benzinga.com/api/v2/news?tickers=AAPL&displayOutput=full&pageSize=15&page=1&token=91741717e18e45a9ac4701d3cb8b7ed4
    """
    state = get_state('bezinga')
    if 'fetched' not in state: state['fetched'] = []

    # Api fetching on a  per-symbol basis
    token = '91741717e18e45a9ac4701d3cb8b7ed4'
    page_size = 25 # max size is 25
    pages = list(range(0, 20 if mode == 'all' else 1))
    vader = SentimentIntensityAnalyzer()

    for current_symbol in focus_symbols:
        for page in pages:
            analysis = Analysis()
            url = f'https://api.benzinga.com/api/v2/news?tickers={current_symbol}&displayOutput=full&pageSize={page_size}&page={page}&token={token}'
            try:
                raw_articles = requests.get(url).text
                raw_articles: OrderedDict = xmltodict.parse(raw_articles)
                if raw_articles is None or 'result' not in raw_articles: continue

                raw_articles = raw_articles['result']
                for article in raw_articles['item']:
                    if any([key not in article or article[key] is None for key in ['id', 'body', 'created', 'stocks']]): continue
                    
                    article_id, date = article['id'], article['created']
                    if article_id in state['fetched']: continue

                    timestamp = dt.datetime.strptime(date, '%a, %d %b %Y %H:%M:%S %z').replace(tzinfo=dt.timezone.utc).timestamp()                
                    
                    if 'item' in article['stocks']:
                        if isinstance(article['stocks']['item'], list): symbols = [a['name'] for a in article['stocks']['item']]
                        else: symbols = article['stocks']['item']['name']
                    else: continue

                    if len(symbols) > 0:
                        sentiment = vader.polarity_scores(article['body'])
                        for symbol in symbols: analysis.data.append(SentimentContext(symbol, 'benzinga', timestamp, sentiment['compound']))
                        add_analyses([analysis])

                        print(f'\n\tbenzinga: sentiment {sentiment["compound"]} | {url}\n\tsymbols: {symbols}\n')

                    state['fetched'].append(article_id)
            except Exception as e:
                print(f'Error while parsing benzinga page @ {url}:', e)
                continue

            save_state('bezinga', state)

    print('\tbenzinga | analysis complete')


def bloomberg(mode: str = 'all'):
    """
    Multiple digestible monthly feeds available:
        # Tickers provided? search source for tickers
        - Biz article list: https://www.bloomberg.com/feeds/bbiz/sitemap_2021_3.xml
        - Tech article list: https://www.bloomberg.com/feeds/technology/sitemap_2021_3.xml
        - Green article list: https://www.bloomberg.com/feeds/green/sitemap_2021_1.xml
        - Business week list: https://www.bloomberg.com/feeds/businessweek/sitemap_2020_8.xml
        - Robots.txt: https://www.bloomberg.com/robots.txt
    """
    state = get_state('bloomberg')
    if 'fetched' not in state: state['fetched'] = []

    cur_year, cur_month = dt.datetime.now().year, dt.datetime.now().month
    if mode == 'all': years, months = [y for y in range(cur_year, 2015, -1)], [m for m in range(1, 13)]
    # else: years, months = [y for y in range(cur_year, cur_year - 1, -1)], [m for m in range(cur_month - 1 if cur_month > 1 else cur_month, cur_month + 1)]
    else: years, months = [y for y in range(cur_year, cur_year - 1, -1)], [m for m in range(cur_month, cur_month + 1)]
    periods = [f'{y}_{m}' for m in months for y in years if y < cur_year or m <= cur_month]
    categories = ['bbiz', 'technology', 'green', 'businessweek']

    vader = SentimentIntensityAnalyzer()
    for period in periods:
        for category in categories:
            analysis = Analysis()

            # Extract article list
            sitemap_url = f'https://www.bloomberg.com/feeds/{category}/sitemap_{period}.xml'
            sitemap = requests.get(sitemap_url).text
            sitemap: OrderedDict = xmltodict.parse(sitemap)

            if 'urlset' not in sitemap or 'url' not in sitemap['urlset']: continue
            if not isinstance(sitemap['urlset']['url'], list) or len(sitemap['urlset']['url']) == 0: continue
            articles: List[Tuple[str, str, int]] = [] # [link, id, timestamp]
            urls = sitemap['urlset']['url']
            for a in urls:
                if not isinstance(a, OrderedDict) or 'loc' not in a or 'lastmod' not in a: continue
                path = quote_plus(a['loc'].split('/')[-1]).encode('utf8')
                article_id = hashlib.md5(path).hexdigest()
                if article_id in state['fetched']: continue
                timestamp = int(dt.datetime.strptime(a['lastmod'], '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=dt.timezone.utc).timestamp())
                articles.append((a['loc'], article_id, timestamp))


            
            # Analyze articles
            for article_url, article_id, timestamp in articles:
                try:
                    soup: BeautifulSoup = fetch_article(article_url, 'bloomberg')
                    # soup = fetch_article('https://www.bloomberg.com/news/articles/2021-03-13/bear-warning-seen-with-nasdaq-100-velocity-stalling-at-2000-peak?srnd=premium', 'bloomberg', 'html.parser')
                    if soup is None or soup == '':
                        print('empty page source')
                        continue

                    has_symbols = soup.find('h2', text=re.compile(r'In this article')) is not None
                    if not has_symbols: continue
                    symbols = [t.text for t in soup.select('.blens>div>div>div>a>div:first-child') if t.text.isalpha() and len(t.text) <= 5]
                    
                    sentiment = vader.polarity_scores(soup.find('article').text)
                    for symbol in set(symbols): analysis.data.append(SentimentContext(symbol, 'bloomberg', timestamp, sentiment['compound']))
                    # add_analyses([analysis]) # BAD BAd BAD - adding same data many times

                    print(f'bloomberg: sentiment {sentiment["compound"]} | symbols: {set(symbols)}')
                except Exception as e:
                    print(f'Error parsing blooomberg artical @ {article_url}  |  ', e)
                    continue

                state['fetched'].append(article_id)
                save_state('bloomberg', state)


def investors(mode: str = 'all'):
    """
    various feeds:

        primary:
        - https://www.investors.com/post-sitemap[0 -> 169].xml
            - higher number is more recent

        Earnings News:
        - https://www.investors.com/wp-admin/admin-ajax.php?id=&post_id=171066&slug=earnings-preview&canonical_url=https://www.investors.com/category/research/earnings-preview/&posts_per_page=500&page=0&offset=0&post_type=post&repeater=default&seo_start_page=1&preloaded=false&preloaded_amount=0&category=earnings-preview&order=DESC&orderby=date&action=alm_get_posts&query_type=standard
    """
    state = get_state('investors')
    if 'fetched' not in state: state['fetched'] = []
    
    last_index = 169
    if mode == 'all': sitemap_indices = list(range(last_index, 0, -1))
    else: sitemap_indices = list(range(last_index, last_index - 1, -1))
    print('WARNING: investors logic is unsound - TODO: check to ensure we\'re starting from last available sitemap')

    for index in sitemap_indices:
        try:
            sitemap_url = f'https://www.investors.com/post-sitemap{index}.xml'
            sitemap = requests.get(sitemap_url).text
            sitemap: OrderedDict = xmltodict.parse(sitemap)
            if sitemap is None or 'urlset' not in sitemap: continue
            if 'url' not in sitemap['urlset'] or len(sitemap['urlset']['url']) == 0: continue

            urls: List[OrderedDict] = sitemap['urlset']['url']
            articles: List[Tuple[str, str]] = []
            for url in urls:
                if 'loc' not in url or 'lastmod' not in url: continue
        
                # generate id
                path_sections = url['loc'].split('/')
                article_id = path_sections[-1] if len(path_sections[-1]) > 0 else path_sections[-2]
                article_id = quote_plus(article_id).encode('utf8')
                article_id = hashlib.md5(article_id).hexdigest()
                if article_id in state['fetched']: continue

                articles.append((url['loc'], article_id))

        except Exception as e:
            print(f'investors: error parsing xml sitemap @ {sitemap_url}')


        print(f'{index}: {len(articles)} articles')
        if len(articles) == 0: continue

        # for a in articles: _investors_process_articles(a)

        for url, article_id in articles:
            analysis = Analysis()
            vader = SentimentIntensityAnalyzer()
            try:
                soup = fetch_article(url, 'investors')
                if soup is None or soup == '': raise 'Empty/Invalid article response'
                else: print('fetched content')

                # soup = BeautifulSoup(link_content, 'html.parser')
                content: str = soup.find('div', { 'class': 'post-content' })
                if content is None:
                    print('could not find post-content in \n\n', soup.text)
                    return

                content = content.text

                symbols = set([a.text for a in soup.find_all('a', { 'class': 'ticker' })])
                timestamp = soup.find('meta', {'property': 'article:published_time'})['content']
                timestamp = int(dt.datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S%z').replace(tzinfo=dt.timezone.utc).timestamp())

                if len(symbols) > 0:
                    sentiment = vader.polarity_scores(content)
                    for symbol in symbols: analysis.data.append(SentimentContext(symbol, 'investors', timestamp, sentiment['compound']))
                    add_analyses([analysis])

                    print(f'\tinvestors: sentiment {sentiment["compound"]} | symbols: {symbols} | {url}\n')

            except Exception as e:
                print(f'investors:parse article error for link: {url}  |  ', e)
                return

            state['fetched'].append(article_id)
            save_state('investors', state)


        # loop = asyncio.get_event_loop()
        # loop.run_until_complete(asyncio.wait([loop.run_in_executor(EXECUTOR, _investors_process_articles, a) for a in articles]))
        # state['fetched'] += [a[1] for a in articles]
        # save_state('investors', state)


def _reuters_process_article(article: Tuple[str, str], article_index: int):
    # debug
    if article_index is not None and article_index % 5 == 0: print(f'\treuters | article: {article_index}')

    analysis = Analysis()
    vader = SentimentIntensityAnalyzer()
    try:
        url, _ = article
        link_content = requests.get(url).text
        soup = BeautifulSoup(link_content, 'html.parser')
        content: str = soup.find('article').text
        print(f'\treuters | content length: ', len(content))

        # timestamp = soup.find('meta', {'property': 'article:published_time'})['content']
        timestamp = soup.find('meta', {'property': 'article:published_time'})
        if timestamp is None: timestamp = soup.find('meta', {'property': 'og:article:published_time'})
        try: timestamp = int(dt.datetime.strptime(timestamp['content'], '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=dt.timezone.utc).timestamp())
        except: timestamp = int(dt.datetime.strptime(timestamp['content'], '%Y-%m-%dT%H:%M:%S%z').replace(tzinfo=dt.timezone.utc).timestamp())

        # custom ap symbol extraction
        symbols = extract_tickers(content, True, False)
        symbols = [s for s in symbols if s not in extended_blacklist]

        if len(symbols) > 0:
            sentiment = vader.polarity_scores(content)
            for symbol in symbols: analysis.data.append(SentimentContext(symbol, 'reuters', timestamp, sentiment['compound']))
            add_analyses([analysis])

            print(f'\treuters: sentiment {sentiment["compound"]} | symbols: {symbols} | {url}\n')

    except Exception as e:
        print(f'reuters:parse article error for link: {url}  |  ', e)
        return

def reuters(mode: str = 'all'):
    """
    Articles can be accessed on a per-day basis via the following, 1 day spans only:
        - https://www.reuters.com/sitemap_YYYYMMDD-YYYYMMDD.xml
        - e.g. https://www.reuters.com/sitemap_20210101-20210102.xml
    """
    state = get_state('reuters')
    if 'fetched' not in state: state['fetched'] = []

    cur_year, cur_month, cur_day = dt.datetime.now().year, dt.datetime.now().month, dt.datetime.now().day
    if mode == 'all': years, months = [y for y in range(cur_year, 2015, -1)], [m for m in range(1, 13)]
    else:
        from_month = cur_month if cur_month == 1 or cur_day > 3 else cur_month - 1
        years, months = [y for y in range(cur_year, cur_year - 1, -1)], [m for m in range(from_month, cur_month + 1)]

    periods: List[str] = []
    for y in years:
        for m in months:
            if mode == 'all': from_day, to_day = 1, calendar.monthrange(y, m)[1] + 1
            else:
                to_day = cur_day + 1 if from_month == cur_month else calendar.monthrange(cur_year, from_month)[1] + 1
                from_day = cur_day - 3 if from_month == cur_month else to_day - abs(cur_day - 3) - 1
            
            for d in range(from_day, to_day):
                if (y == cur_year and m > cur_month) or (y == cur_year and m == cur_month and d > cur_day): continue
                prev_period_month = m - 1 if d == 1 and m > 1 else 12 if d == 1 else m
                prev_period_year = y if (prev_period_month < 12 or m == 12) else y - 1
                prev_day = d - 1 if d > 1 else calendar.monthrange(prev_period_year, prev_period_month)[1]
                formatted_prev_month, formatted_prev_day = f'{"0" if prev_period_month < 10 else ""}{prev_period_month}', f'{"0" if prev_day < 10 else ""}{prev_day}'
                formatted_month, formatted_day = f'{"0" if m < 10 else ""}{m}', f'{"0" if d < 10 else ""}{d}'
                periods.append(f'{prev_period_year}{formatted_prev_month}{formatted_prev_day}-{y}{formatted_month}{formatted_day}')

    # Build up urls + timestamps
    for period in periods:
        sitemap_url = f'https://www.reuters.com/sitemap_{period}.xml'

        # Extract article list
        raw_articles = requests.get(sitemap_url).text
        try: raw_articles: OrderedDict = xmltodict.parse(raw_articles)['urlset']
        except:
            print(f'error parsing reuters articles for {period}')
            continue
        articles: List[Tuple[str, str]] = []
        for url in raw_articles['url']:
            try:
                article_id: str = url['loc'].split('/')[-1].split('-')[-1]
                if article_id in state['fetched']: continue
                articles.append((url['loc'], article_id))
            except Exception as e: continue


        # Analyze articles
        if len(articles) == 0: continue
        print(f'\treuters | {period}: {len(articles)} new articles')
        try: loop = asyncio.get_event_loop()
        except: loop = asyncio.new_event_loop()
        loop.run_until_complete(asyncio.wait([loop.run_in_executor(EXECUTOR, _reuters_process_article, a, ai) for ai, a in enumerate(articles)]))
        state['fetched'] += [a[1] for a in articles]
        save_state('reuters', state)

    print('\treuters | analysis complete')


def _market_watch_process_article(article: Tuple[str, str, int]):
    analysis = Analysis()
    vader = SentimentIntensityAnalyzer()
    try:
        url, _, timestamp = article
        link_content = requests.get(url).text
        soup = BeautifulSoup(link_content, 'html.parser')
        content: str = soup.find('div', { 'class': 'article__content' }).text

        # custom mw symbol extraction
        tickers = soup.select('div.referenced-tickers .list--tickers span.symbol')
        symbols = set([t.text for t in tickers])

        if len(symbols) > 0:
            sentiment = vader.polarity_scores(content)
            for symbol in symbols: analysis.data.append(SentimentContext(symbol, 'market_watch', timestamp, sentiment['compound']))
            add_analyses([analysis])

            print(f'\tmarket_watch: sentiment {sentiment["compound"]} | symbols: {symbols} | {url}\n')

    except Exception as e:
        print(f'\tmarket_watch:parse article error for link: {url}  |  ', e)
        return

def market_watch(mode: str = 'all'):
    """
    Stock-specific only:
        stock-specific articles: https://www.marketwatch.com/investing/stock/aapl/moreheadlines?channel=MarketWatch&source=ChartingSymbol&pageNumber=4
        latest articles only: https://www.marketwatch.com/mw_news_sitemap.xml
    """
    state = get_state('market_watch')
    if 'fetched' not in state: state['fetched'] = []
    
    pages = list(range(0, 30 if mode == 'all' else 1))

    for current_symbol in focus_symbols:
        for page in pages:
            url = f'https://www.marketwatch.com/investing/stock/{current_symbol.lower()}/moreheadlines?channel=MarketWatch&source=ChartingSymbol&pageNumber={page}'
            try:
                res = requests.get(url)
                if res.status_code != 200: continue

                listing_page = res.text
                listing_content = BeautifulSoup(listing_page, 'html.parser')
                listings = listing_content.find_all('div', { 'class': 'element--article' })
                articles: List[Tuple[str, str, int]] = []
                for listing in listings:
                    article_id = listing.get('data-guid')
                    if article_id is None or article_id in state['fetched']: continue
    
                    timestamp = listing.get('data-timestamp')
                    if timestamp is None or timestamp == '': continue
                    timestamp = int(int(timestamp) / 1000) # Initially stored with microseconds

                    if listing.find('a', { 'class': 'link' }) is None: continue
                    link = listing.find('a', { 'class': 'link' })['href']

                    articles.append((link, article_id, timestamp))

            except Exception as e:
                print(f'Error while parsing market_watch page @ {url}:', e)
                continue

            # Analyze
            if len(articles) == 0: continue
            print(f'\n\tmarket_watch | {current_symbol}, page {page} | {len(articles)} new articles')
            try: loop = asyncio.get_event_loop()
            except: loop = asyncio.new_event_loop()
            loop.run_until_complete(asyncio.wait([loop.run_in_executor(EXECUTOR, _market_watch_process_article, a) for a in articles]))
            state['fetched'] += [a[1] for a in articles]
            save_state('market_watch', state)

    print('\tmarket_watch | analysis complete')


def _cnn_process_article(article: Tuple[str, str]):
    analysis = Analysis()
    vader = SentimentIntensityAnalyzer()
    try:
        url, _ = article
        res = requests.get(url).text
        page = BeautifulSoup(res, 'html.parser')
        article_content: str = page.find('section', { 'id': 'body-text' })
        # if article_content is None: page.find('article')
        if article_content is None: return
        article_content = article_content.text

        timestamp = page.find('meta', {'property': 'og:pubdate'})
        timestamp = int(dt.datetime.strptime(timestamp['content'], '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=dt.timezone.utc).timestamp())

        # custom mw symbol extraction
        symbols = set([a.text for a in page.select('span.inlink_chart a')])

        if len(symbols) > 0:
            sentiment = vader.polarity_scores(article_content)
            for symbol in symbols: analysis.data.append(SentimentContext(symbol, 'cnn', timestamp, sentiment['compound']))
            add_analyses([analysis])

            print(f'\tcnn: sentiment {sentiment["compound"]} | symbols: {symbols} | {url}\n')

    except Exception as e:
        print(f'\tcnn:parse article error for link: {url}  |  ', e)
        return

def cnn(mode: str = 'all'):
    """
    Articles can be fetched on a monthly basis:
        - https://www.cnn.com/article/sitemap-2021-2.html
        - business only (use this one): https://www.cnn.com/business/article/sitemap-2021-1.html
    """
    state = get_state('cnn')
    if 'fetched' not in state: state['fetched'] = []

    cur_year, cur_month = dt.datetime.now().year, dt.datetime.now().month
    if mode == 'all': years, months = [y for y in range(cur_year, 2019, -1)], [m for m in range(1, 13)]
    else: years, months = [y for y in range(cur_year, cur_year - 1, -1)], [m for m in range(cur_month - 1 if cur_month > 1 else cur_month, cur_month + 1)]
    periods = [f'{y}-{m}' for y in years for m in months  if y < cur_year or m <= cur_month]

    for period in periods:
        try:
            # Extract article list
            sitemap_url = f'https://www.cnn.com/business/article/sitemap-{period}.html'
            sitemap = requests.get(sitemap_url).text
            content = BeautifulSoup(sitemap, 'html.parser')

            # extract articles + create ids from href
            articles = [a['href'] for a in content.select('.sitemap-link a') if 'advertorial' not in a['href']]
            articles = [(url, hashlib.md5(quote_plus(url.split('/')[-2]).encode('utf8')).hexdigest()) for url in articles]
            articles = [(url, article_id) for url, article_id in articles if article_id not in state['fetched']]
        except:
            print(f'Error parsing cnn sitemap: {sitemap_url}')
            continue


        # Analyze
        if len(articles) == 0: continue
        else: print(f'\tcnn: {period} | {len(articles)} articles')
        try: loop = asyncio.get_event_loop()
        except: loop = asyncio.new_event_loop()
        loop.run_until_complete(asyncio.wait([loop.run_in_executor(EXECUTOR, _cnn_process_article, a) for a in articles]))
        state['fetched'] += [a[1] for a in articles]
        save_state('cnn', state)


    print('\tcnn | analysis complete')



def _pr_nw_process_article(article: Tuple[str, str, int]):
    analysis = Analysis()
    vader = SentimentIntensityAnalyzer()
    try:
        url, _, timestamp = article
        res = requests.get(url).text
        page = BeautifulSoup(res, 'html.parser')
        article_content: str = page.find('article')
        if article_content is None: return
        article_content = article_content.text


        # custom mw symbol extraction
        symbols = set([ticker.text for ticker in page.find_all('a', {'class': 'ticket-symbol'})])

        if len(symbols) > 0:
            sentiment = vader.polarity_scores(article_content)
            for symbol in symbols: analysis.data.append(SentimentContext(symbol, 'pr_newswire', timestamp, sentiment['compound']))
            add_analyses([analysis])

            print(f'\tpr_newswire: sentiment {sentiment["compound"]} | symbols: {symbols} | {url}\n')

    except Exception as e:
        print(f'\tyahoo:parse article error for link: {url}  |  ', e)
        return

def pr_newswire(mode: str = 'all'):
    """
    Provides press releases
        - https://www.prnewswire.com/sitemap-main-news.xml?page=[1...]
            - lower number is more recent
            - goes to at least 650

        - https://www.prnewswire.com/Sitemap_Index_Feb_2021.xml.gz
            -   Monthly data available
            -   Full list: https://www.prnewswire.com/sitemap-gz.xml

        - https://www.prnewswire.com/sitemap-news.xml?page=[1...8]
            - somewhat recent data only
            - <news:stock_tickers> attributes available
    """
    state = get_state('pr_newswire')
    if 'fetched' not in state: state['fetched'] = []
    
    n_pages = 10 if mode == 'all' else 2
    for page in range(1, n_pages + 1):
        page_url = f'https://www.prnewswire.com/sitemap-main-news.xml?page={page}'
        try:
            # Extract article list
            sitemap = requests.get(page_url).text
            sitemap_urls: OrderedDict = xmltodict.parse(sitemap)['urlset']

            articles: List[Tuple[str, str]] = []
            for article in sitemap_urls['url']:
                article_id: str = article['loc'].split('/')[-1].split('-')[-1][:-5]
                if article_id in state['fetched']: continue

                timestamp = int(dt.datetime.strptime(article['lastmod'], '%Y-%m-%dT%H:%M:%S%z').replace(tzinfo=dt.timezone.utc).timestamp())
                articles.append((article['loc'], article_id, timestamp))

            if len(articles) == 0: continue
            print(f'\tpr_newswire | page {page} / {n_pages} - {len(articles)} new articles')
            try: loop = asyncio.get_event_loop()
            except: loop = asyncio.new_event_loop()
            loop.run_until_complete(asyncio.wait([loop.run_in_executor(EXECUTOR, _pr_nw_process_article, a) for a in articles]))
            state['fetched'] += [a[1] for a in articles]
            save_state('pr_newswire', state)

        except Exception as e:
            print(f'Error parsing pr newswire sitemap: {page_url}:', e)
            continue

    print('\tpr newswire | analysis complete')



def _yahoo_process_article(article: Tuple[str, str, str, int]):
    analysis = Analysis()
    vader = SentimentIntensityAnalyzer()
    try:
        _, url, _, timestamp = article
        res = requests.get(url).text
        page = BeautifulSoup(res, 'html.parser')
        article_content: str = page.find('div', { 'class': 'caas-body' })
        if article_content is None: return
        article_content = article_content.text


        # custom mw symbol extraction
        symbols = set([button.get('data-entity-id') for button in page.find_all('button', {'class': 'caas-xray-pill-type-ticker'})])

        if len(symbols) > 0:
            sentiment = vader.polarity_scores(article_content)
            for symbol in symbols: analysis.data.append(SentimentContext(symbol, 'yahoo', timestamp, sentiment['compound']))
            add_analyses([analysis])

            print(f'\tyahoo: sentiment {sentiment["compound"]} | symbols: {symbols} | {url}\n')

    except Exception as e:
        print(f'\tyahoo:parse article error for link: {url}  |  ', e)
        return

def _forbes_process_article(article: Tuple[str, str, str, int]):
    analysis = Analysis()
    vader = SentimentIntensityAnalyzer()
    try:
        _, url, _, timestamp = article
        res = requests.get(url).text
        page = BeautifulSoup(res, 'html.parser')
        article_content: str = page.find('main')
        if article_content is None: return
        article_content = article_content.text


        # custom mw symbol extraction
        symbols = set([ticker.get('data-name') for ticker in page.find_all('fbs-ticker')])

        if len(symbols) > 0:
            sentiment = vader.polarity_scores(article_content)
            for symbol in symbols: analysis.data.append(SentimentContext(symbol, 'forbes', timestamp, sentiment['compound']))
            add_analyses([analysis])

            print(f'\tforbes: sentiment {sentiment["compound"]} | symbols: {symbols} | {url}\n')

    except Exception as e:
        print(f'\tforbes:parse article error for link: {url}  |  ', e)
        return

def _motley_fool_process_article(article: Tuple[str, str, str, int]):
    analysis = Analysis()
    vader = SentimentIntensityAnalyzer()
    try:
        _, url, _, timestamp = article
        res = requests.get(url).text
        page = BeautifulSoup(res, 'html.parser')
        article_content: str = page.find('span', {'class': 'article-content'})
        if article_content is None: return
        article_content = article_content.text

        # custom mw symbol extraction
        symbols = set([ticker.text.split(':')[1].replace('\n', '') for ticker in page.select('.ticker a')])


        if len(symbols) > 0:
            sentiment = vader.polarity_scores(article_content)
            for symbol in symbols: analysis.data.append(SentimentContext(symbol, 'themotleyfool', timestamp, sentiment['compound']))
            add_analyses([analysis])

            print(f'\tthemotleyfool: sentiment {sentiment["compound"]} | symbols: {symbols} | {url}\n')

    except Exception as e:
        print(f'\tthemotleyfool:parse article error for link: {url}  |  ', e)
        return


# in progress | yahoo, forbes, motleyfool supported
def finurls(mode: str = 'all'):
    """
    finurls (https://finurls.com/) provides an aggregation of various sources
    Not great for absolute latest data but provides easy access to historical data
        - each can be accessed via a request with form data containing two params:
            - "site": one of ['medium'...]
            - "interval": one of ['latest', 'day', 'week', 'month']
            + two additional fields for scroll support
            - "last_id": integer representing last fetched id
            - "load_more": true

        - entries have 6 fields:
            - 'id' (int)
            - 'title' (str)
            - 'url' (str)
            - 'comment_url' (str|None)
            - 'ago' (str like '4h', '1d', '4m')
            - 'data' (str like '2021-03-19 08:41:19PM UTC')
    """
    api_url = 'https://finurls.com/api/get_titles'
    sites = ['yahoo', 'forbes', 'themotleyfool']
    fetch_n_months = 4 if mode == 'all' else 1
    # fetch_n_months = 12 if mode == 'all' else 1
    
    interval = 'month' if mode == 'all' else 'latest'

    def get_date(date_string: str) -> int:
        return int(dt.datetime.strptime(date_string, '%Y-%m-%d %H:%M:%S%p UTC').replace(tzinfo=dt.timezone.utc).timestamp())
    
    for site in sites:
        state = get_state(site)
        if 'fetched' not in state: state['fetched'] = []

        try:
            # Continue fetching entries until fetch_count is met
            last_id: int = None
            for i in range(fetch_n_months):
                entries: List[Tuple[str, str, str, int]] = []
                last_id_for_site: int = np.min(state['fetched']) if len(state['fetched']) > 0 else None

                print(f'{site} fetching | Analyzing month {i} / {fetch_n_months}')
                params = { 'site': site, 'interval': interval }
                if last_id is not None:
                    params['last_id'] = last_id
                    params['load_more'] = True

                data = requests.post(
                    api_url,
                    data=params
                ).json()
                if any([field not in data for field in ['data', 'status']]) or data['status'] != 'success':
                    raise Exception('Invalid finurls response:', data)

                req_entries = [(e['id'], e['url'], e['ago'], get_date(e['date'])) for e in data['data']]
                if len(req_entries) == 0: break
                finished = False
                if any([e[0] in state['fetched'] for e in req_entries]): # starting to hit previously fetched entries
                    # TESTING
                    finished = True
                    # break # ignore old articles

                    last_id = min(last_id_for_site, np.min([e[0] for e in req_entries]))
                    for e in [e for e in req_entries if e[0] not in state['fetched']]: entries.append(e)
                else:
                    last_id = np.min([e[0] for e in req_entries])
                    for e in req_entries: entries.append(e)


                if len(entries) == 0: continue
                print(f'\tfinurls - {site} | {len(entries)} new entries')

                parser: Callable = None
                if site == 'yahoo': parser = _yahoo_process_article
                elif site == 'forbes': parser = _forbes_process_article
                elif site == 'themotleyfool': parser = _motley_fool_process_article
                else:
                    print(f'finurls | no matching parser for site: {site}')
                    continue

                try: loop = asyncio.get_event_loop()
                except: loop = asyncio.new_event_loop()
                loop.run_until_complete(asyncio.wait([loop.run_in_executor(EXECUTOR, parser, e) for e in entries]))
                state['fetched'] += [e[0] for e in entries]
                save_state(site, state)
                if finished: break

        except Exception as e:
            print(f'Error fetching finurls data for {site}:', e)
            continue

    print('\tfinurls | analysis complete')




# Unimplemented
def nyt():
    """
        - news: https://www.nytimes.com/sitemaps/new/sitemap-2021-03.xml.gz
            - should probably filter the urls by path (i.e. ignore /espanol, /sports, /books, /arts)
        - wirecutter reviews: https://www.nytimes.com/wirecutter/post_review.xml
    """
    
    url_checks = ['business', 'economy', 'stock', 'market', 'technology']

def fox():
    """
    Articles can be fetched with a 'from' timestamp
        - https://www.foxbusiness.com/sitemap.xml?type=articles&from=1596020695000

        - sitemap: https://www.foxbusiness.com/sitemap.xml
        - robots.txt: https://www.foxbusiness.com/robots.txt
    """
    pass


def usa_today():
    """
    List of article urls available on monthly basis:
        - https://www.gannett-cdn.com/sitemaps/USAT/web/web-sitemap-2021-03.xml
            - filtering probably required
    """
    pass

def globe_newswire():
    """
    Provides press releases
        - http://www.globenewswire.com/Index?page=1#pagerPos
            - will have to manually scrape articles
    """
    pass

def sp_global():
    """
    """
    pass






# Analysis

def analyze():
    # Fetch article lists for specified sources
    # all_sources = ['seeking_alpha', 'cnbc', 'ap', 'benzinga', 'bloomberg', 'reuters', 'market_watch', 'pr_newswire']
    bg_sources = ['pr_newswire', 'market_watch', 'benzinga', 'finurls', 'cnn', 'ap', 'cnbc']
    sources = bg_sources

    for source in sources:
        if source == 'seeking_alpha': seeking_alpha()
        elif source == 'cnbc': cnbc()
        elif source == 'ap': ap()
        elif source == 'benzinga': benzinga()
        elif source == 'bloomberg': bloomberg()
        elif source == 'investors': investors()
        elif source == 'reuters': reuters()
        elif source == 'market_watch': market_watch()
        elif source == 'cnn': cnn()
        elif source == 'finurls': finurls()
        elif source == 'pr_newswire': pr_newswire()
        
        else: raise Exception(f'Unknown source requested: {source}')

    





# Monitoring
def check_sources():
    start = timer()
    all_sources = ['seeking_alpha', 'cnbc', 'ap', 'benzinga', 'bloomberg', 'reuters', 'market_watch', 'pr_newswire']
    bg_sources = ['market_watch', 'ap', 'benzinga', 'cnn', 'finurls', 'pr_newswire']
    threads: List[threading.Thread] = []
    print(f'Mixed Sources Refresh: {", ".join(bg_sources)}')
    for source in set(bg_sources):
        if source == 'seeking_alpha': threads.append(threading.Thread(target=seeking_alpha, args=['recent']))
        elif source == 'cnbc': threads.append(threading.Thread(target=cnbc, args=['recent']))
        elif source == 'ap': threads.append(threading.Thread(target=ap, args=['recent']))
        elif source == 'benzinga': threads.append(threading.Thread(target=benzinga, args=['recent']))
        elif source == 'bloomberg': threads.append(threading.Thread(target=bloomberg, args=['recent']))
        elif source == 'investors': threads.append(threading.Thread(target=investors, args=['recent']))
        elif source == 'reuters': threads.append(threading.Thread(target=reuters, args=['recent']))
        elif source == 'market_watch': threads.append(threading.Thread(target=market_watch, args=['recent']))
        elif source == 'cnn': threads.append(threading.Thread(target=cnn, args=['recent']))
        elif source == 'finurls': threads.append(threading.Thread(target=finurls, args=['recent']))
        elif source == 'pr_newswire': threads.append(threading.Thread(target=pr_newswire, args=['recent']))

    for th in threads: th.start()
    for th in threads: th.join()

    print(f'Mixed Source Refresh Complete | took {(timer() - start):.2f} seconds\n')


def monitor(frequency: int = 600):
    """
    Checks each of the sources every [frequency] seconds for new articles and stores them in es
    """

    # Kick off timer
    starttime = time.time()
    th = threading.Thread(target=check_sources)
    th.start()
    while True:
        if not th.is_alive():
            time.sleep(frequency - ((time.time() - starttime) % frequency))
            th = threading.Thread(target=check_sources)
            th.start()

