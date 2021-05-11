import requests, time, json, threading, asyncio, os, numpy as np
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from typing import Any, Dict
from concurrent.futures import ThreadPoolExecutor

from data.es import add_analyses
from config.symbols import focus_symbols
from config.models import TweetData, Analysis, SentimentContext

EXECUTOR = ThreadPoolExecutor(max_workers=6)


# Helpers
def get_state() -> Dict[str, Any]:
    if not os.path.exists(f'scrapers/fetched/twitter.json'): save_state({})

    with open(f'scrapers/fetched/twitter.json', 'r') as f:
        try: state = json.loads(f.read())
        except: state = {}
    
    if 'fetched' not in state: state['fetched'] = {}
    return state

def save_state(state: Dict[str, Any]):
    with open(f'scrapers/fetched/twitter.json', 'w') as f:
        try: f.write(json.dumps(state, indent=4))
        except Exception as e: raise Exception(f'Error updating twitter state:', e)



def _process_tweet(tweet: TweetData):
    analysis = Analysis()
    vader = SentimentIntensityAnalyzer()

    try:
        sentiment = vader.polarity_scores(tweet.body)
        for symbol in tweet.symbols: analysis.data.append(SentimentContext(symbol, 'twitter', tweet.timestamp, sentiment['compound']))
        add_analyses([analysis])
        print(f'\ttwitter: sentiment {sentiment["compound"]} | symbols: {tweet.symbols}\n')

    except Exception as e: print('Error processing tweet:', e)


def fetch_tweets(mode: str = 'all'):
    api_prefix = 'https://api.stocktwits.com/api/2/streams/symbol'
    rate_limit = 200 # can do 200 requests / hour
    state = get_state()

    for symbol in focus_symbols:
        n_pages = (rate_limit // len(focus_symbols)) if mode == 'all' else 1
        last_fetched_id = min(state['fetched'][symbol]) if symbol in state['fetched'] and len(state['fetched'][symbol]) > 0 else None
        
        for page in range(n_pages):
            url = f'{api_prefix}/{symbol}.json?filter=all&limit=30'
            if page > 0 and last_fetched_id is not None: url += f'&max={last_fetched_id}'

            res = requests.get(url).json()
            if 'response' not in res or res['response']['status'] != 200:
                raise Exception(f'Invalid Twitter Response Body: ', res)

            tweets = [TweetData(message) for message in res['messages']]
            last_fetched_id = np.min([t.id for t in tweets])
            tweets = [tweet for tweet in tweets if tweet.id not in state['fetched']]
            
            if len(tweets) == 0: continue
            else: print(f'\ttwitter: page {page + 1} | {len(tweets)} tweets')
            try: loop = asyncio.get_event_loop()
            except: loop = asyncio.new_event_loop()
            loop.run_until_complete(asyncio.wait([loop.run_in_executor(EXECUTOR, _process_tweet, t) for t in tweets]))
            
            if symbol not in state['fetched']: state['fetched'][symbol] = []
            for t in tweets: state['fetched'][symbol].append(t.id)
            save_state(state)


                
    

# Analysis
def analyze():
    print('Starting Twitter Analysis')
    try: fetch_tweets()
    except Exception as e: print('Twitter Analysis Error:', e)
    print('Twitter Analysis Complete')


# Monitoring
def monitor(frequency: int = 60):
    """
    Continually checks for recent tweets
    """
    frequency = 3600

    # Kick off monitoring
    starttime = time.time()
    th = threading.Thread(target=fetch_tweets, args=['all'])
    th.start()
    while True:
        if not th.is_alive():
            time.sleep(frequency - ((time.time() - starttime) % frequency))
            th = threading.Thread(target=fetch_tweets, args=['all'])
            th.start()
