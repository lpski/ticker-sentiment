from typing import Any, Dict, List, NamedTuple
import requests, numpy as np, json, time, threading, asyncio
from nltk.sentiment.vader import SentimentIntensityAnalyzer

from config.symbols import enhanced_symbols
from config.models import Analysis, SentimentContext, HN_Item, HN_Post
from data.es import add_analyses


# Based on documentation found @ https://github.com/HackerNews/API
top_stories_url = 'https://hacker-news.firebaseio.com/v0/topstories.json?print=pretty'



# Utils
def item_url(id: str): return f'https://hacker-news.firebaseio.com/v0/item/{id}.json?print=pretty'

def is_subset(parent: list, child: list) -> bool:
    for i in range(len(parent) - len(child)):
        if parent[i:i+len(child)] == child:
            return True
    return False


def get_state() -> Dict[str, Any]:
    with open('scrapers/fetched/hn.json', 'r') as f:
        try: state = json.loads(f.read())
        except: state = {}

    if 'fetched' not in state: state['fetched'] = {}
    if 'last_analyzed_post_id' not in state: state['last_analyzed_post_id'] = -1
    return state

def update_state(post: HN_Post):
    state = get_state()
    state['last_analyzed_post_id'] = post.story.id
    state['fetched'][post.story.id] = [c.id for c in post.comments]

    # Update visited ids with newly analyzed post
    with open('scrapers/fetched/hn.json', 'w') as f:
        try: f.write(json.dumps(state, indent=4))
        except Exception as e: print('Error updating hackernews state')


def last_valid_id() -> int:
    try:
        max_items_url = 'https://hacker-news.firebaseio.com/v0/maxitem.json?print=pretty'
        res = requests.get(max_items_url).json()
        return int(res)
    
    except: raise Exception('Error fetching last valid hacker news id.')




async def analyze_comments_for_post(post: HN_Post):
    symbol_data = enhanced_symbols()
    vader = SentimentIntensityAnalyzer()

    analysis = Analysis()
    # First we determine if it's a relevant story
    # If it is, we then perform sentiment analysis on the comments + title
    title_segments = post.story.title.split(' ') # Removed .lower() to prevent erroneous matches

    contained_companies = [sym for sym, comp in symbol_data.companies.items() if is_subset(title_segments, [c.title() for c in comp.split(' ')])]
    if len(contained_companies) > 0:
        # print(f'\t{post.story.title} contains {len(contained_companies)} companies:', contained_companies)
        
        title_score = vader.polarity_scores(post.story.title)
        sentiments = [title_score['compound']]
        for comment in post.comments:
            comment_score = vader.polarity_scores(comment.text)
            sentiments.append(comment_score['compound'])
        adj_sentiment = np.mean(sentiments) if len(sentiments) > 0 else 0
        print(f'\thacker_news | sentiment: {adj_sentiment} | symbols: {contained_companies}')

        for sym in contained_companies:
            analysis.data.append(SentimentContext(sym, 'hacker_news', post.story.time, adj_sentiment))

    add_analyses([analysis])



def analyze(max_article_count: int = 100, mode: str = 'all') -> List[HN_Post]:
    current_id = last_valid_id()
    remaining_articles = max_article_count

    # Load in previously fetched ids
    state = get_state()

    # Ignore all previously fetched posts except for latest 20 to account for new comments
    previously_fetched = list(state['fetched'].keys())
    ignore_posts = set(previously_fetched[20:])
    # if len(ignore_posts) > 0: 

    try: loop = asyncio.get_event_loop()
    except: loop = asyncio.new_event_loop()

    # First we construct the item dictionary
    items: Dict[int, HN_Item] = {}
    while remaining_articles > 0 and current_id >= 0:
        # Ignore certain posts
        if current_id in ignore_posts:
            if mode == 'recent': return
            current_id = min(ignore_posts) - 1
            continue

        try:
            item = HN_Item.from_json(requests.get(item_url(current_id)).json())
            items[item.id] = item
            if item.type == 'story':
                post = HN_Post(item, [items[kid] for kid in item.kids if kid in items])
                if post.story.id in ignore_posts: # Delete unused post comments
                    for comment in [c for c in post.comments if c.id in items]: del items[comment.id]
                    del items[post.story.id]
                else:
                    print(f'\thacker_news | analyzing post {post.story.id} | {remaining_articles - 1} remaining')
                    loop.run_until_complete(analyze_comments_for_post(post))
                    remaining_articles -= 1
                    update_state(post)

            current_id -= 1

        except Exception as e:
            print(f'Error fetching hackernews item with id {current_id}, skipping:', e)









def fetch_posts(max_article_count: int, mode: str = 'all') -> List[HN_Post]:
    print(f'Fetching {max_article_count} HN Posts')

    max_id = last_valid_id()
    current_id = max_id
    remaining_articles = max_article_count

    # Load in previously fetched ids
    state = get_state()
    skip_ids = set(state['fetched'])

    # First we construct the item dictionary
    items: Dict[int, HN_Item] = {}
    while remaining_articles > 0 and current_id > 0:
        if current_id % 100 == 0: print(f'\thn | {max_id - current_id}/{max_id} | {remaining_articles} remaining articles required')

        if current_id in skip_ids:
            current_id -= 1
            continue
        else: skip_ids.add(current_id)

        try:
            res = requests.get(item_url(current_id)).json()
            if res is not None:
                item = HN_Item.from_json(res)
                items[item.id] = item
                if item.type == 'story': remaining_articles -= 1
            current_id -= 1

        except Exception as e:
            print(res)
            print(f'Error fetching hackernews item with id {current_id}, skipping:', e)

    # Update visited ids
    with open('scrapers/fetched/hn.json', 'w') as f:
        data = {'fetched_ids': list(skip_ids)}
        try: f.write(json.dumps(data, indent=4))
        except Exception as e: print('Error updating hackernews visited ids')

    # Now we can associate the posts with their comments
    posts: List[HN_Post] = []
    for story in [s for s in items.values() if s.type == 'story']:
        posts.append(HN_Post(story, [items[kid] for kid in story.kids if kid in items]))

    return posts


def analyze_v2(max_article_count: int = 2000):
    """
    HackerNews Analysis is performed by iterating (backwards) for a given max article count.
    """
    
    print('\n\nStarting HackerNews Analysis\n_____________\n')
    posts = fetch_posts(max_article_count)
    symbol_data = enhanced_symbols()
    vader = SentimentIntensityAnalyzer()

    print(f'\tFetch Complete: {len(posts)} Posts')
    for post in posts:
        analysis = Analysis()
        # First we determine if it's a relevant story
        # If it is, we then perform sentiment analysis on the comments + title
        title_segments = post.story.title.split(' ') # Removed .lower() to prevent erroneous matches

        contained_companies = [sym for sym, comp in symbol_data.companies.items() if is_subset(title_segments, [c.title() for c in comp.split(' ')])]
        if len(contained_companies) > 0:
            print(f'\t{post.story.title} contains {len(contained_companies)} companies:', contained_companies)
            
            title_score = vader.polarity_scores(post.story.title)
            sentiments = [title_score['compound']]
            for comment in post.comments:
                comment_score = vader.polarity_scores(comment.text)
                sentiments.append(comment_score['compound'])
            adj_sentiment = np.mean(sentiments) if len(sentiments) > 0 else 0

            for sym in contained_companies:
                analysis.data.append(SentimentContext(sym, 'hacker_news', post.story.time, adj_sentiment))

        add_analyses([analysis])

    


# Monitoring    

def monitor(frequency: int = 60):
    """
    Continually checks the api for unviewed posts
    """
    MAX_LIVE_ARTICLE_CHECK = 30

    # Kick off monitoring
    starttime = time.time()
    th = threading.Thread(target=analyze, args=[MAX_LIVE_ARTICLE_CHECK, 'recent'])
    th.start()
    while True:
        if not th.is_alive():
            time.sleep(frequency - ((time.time() - starttime) % frequency))
            th = threading.Thread(target=analyze, args=[MAX_LIVE_ARTICLE_CHECK, 'recent'])
            th.start()
