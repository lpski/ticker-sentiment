import praw, time, pandas as pd, json, threading
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from praw.models import Submission
from typing import Dict, List, Tuple
from timeit import default_timer as timer
from dotenv import load_dotenv, dotenv_values

from config.symbols import us, blacklist
from config.models import Analysis, SentimentContext
from data.es import add_analyses


def save_state(state: Dict):
    with open('scrapers/fetched/wsb.json', 'w') as f:
        data = {'fetched_posts': state }
        try: f.write(json.dumps(data, indent=4))
        except Exception as e: print('Error updating wsb visited ids')

def fetch_creds() -> Tuple[str, str]:
    load_dotenv()
    env = dotenv_values('.env')
    if 'REDDIT_CLIENT_ID' not in env: raise Exception('REDDIT_CLIENT_ID not provided')
    if 'REDDIT_SECRET' not in env: raise Exception('REDDIT_SECRET not provided')
    return env['REDDIT_CLIENT_ID'], env['REDDIT_SECRET']




def extract_posts(subreddits: str = 'wallstreetbets', max_post_count: int = 10) -> Tuple[List[Submission], Dict[str, List[str]]]:
    # Create an authorized praw instance
    client_id, client_secret = fetch_creds()
    reddit = praw.Reddit(user_agent='Comment Extraction', client_id=client_id, client_secret=client_secret)
    subreddit = reddit.subreddit(subreddits)
    submissions: List[Submission] = subreddit.search(
        # query='subreddit:wallstreetbets title:daily discussion',
        query='subreddit:wallstreetbets flair:Daily Discussion',
        sort='new',
        time_filter = 'all', # Can be one of: all, day, hour, month, week, year (
    )
    submissions = [s for s in submissions if ('Daily Discussion Thread' in s.title or 'What Are Your Moves Tomorrow' in s.title) and 'Unpinned' not in s.title]


    # Filter out previously analyzed posts
    with open('scrapers/fetched/wsb.json', 'r') as f:
        try: wsb_state = json.loads(f.read())['fetched_posts']
        except: wsb_state = {}
    # submissions = [s for i, s in enumerate(submissions) if i == 0 or s.id not in wsb_state or len(wsb_state[s.id]) < 5000][:max_post_count]
    submissions = [s for i, s in enumerate(submissions) if s.id not in wsb_state or len(wsb_state[s.id]) < 15000][:max_post_count]

    # Update stored ids
    for sub in submissions:
        if sub.id not in wsb_state: wsb_state[sub.id] = []
    with open('scrapers/fetched/wsb.json', 'w') as f:
        data = {'fetched_posts': wsb_state }
        try: f.write(json.dumps(data, indent=4))
        except Exception as e: print('Error updating wsb visited ids')

    return submissions, wsb_state


def extract_significant_comments(state: Dict[str, List[str]], submissions: List[Submission], comment_limit: int = 10000, min_upvotes: int = 2, sort: str = 'new'):
    post_flairs = ['Daily Discussion', 'Weekend Discussion', 'Discussion']
    vader = SentimentIntensityAnalyzer()

    post_details = {}
    for i, submission in enumerate(submissions):
        print(f'\twsb | analyzing submission {i+1}/{len(submissions)}: {submission.title}')
        try:
            analysis = Analysis()
            if submission.link_flair_text in post_flairs:
                post_details[submission.id] = []
                submission.comment_sort = sort # 'new'
                comments = submission.comments
                prev_comments = set(state[submission.id])
                
                submission.comments.replace_more(limit=min(comment_limit, submission.num_comments))
                check_comments = [c for c in comments if c.score >= min_upvotes and c.id not in prev_comments]
                print(f'\tchecking {len(check_comments)} comments | sorted by {sort} | >= {min_upvotes} upvotes')
                for comment in check_comments:
                    if comment.id in prev_comments: continue
                    else: state[submission.id].append(comment.id)
                    comment_words = comment.body.split(" ")
                    sanitized_words = [w.replace('$', '') for w in comment_words]
                    mentioned_symbols = [word for word in sanitized_words if word.isupper() and len(word) <= 5 and word not in blacklist and word in us]
                    if len(mentioned_symbols) > 0:
                        polarity = vader.polarity_scores(comment.body)
                        for symbol in mentioned_symbols:
                            analysis.data.append(SentimentContext(symbol, 'wsb', comment.created_utc, polarity['compound']))

            add_analyses([analysis])
            save_state(state)

        except: continue



def analyze():
    print('Starting WSB Comment Analysis')
    submissions, state = extract_posts(max_post_count=5)
    print(f'\tBeginning comment analysis of {len(submissions)} posts')
    extract_significant_comments(state, submissions, comment_limit=15000)
    print('WSB Comment Analysis Complete')




# Monitoring
def check_latest_thread(subreddit):
    print('wsb | Checking Latest Thread')

    start = timer()
    with open('scrapers/fetched/wsb.json', 'r') as f:
        try: state = json.loads(f.read())['fetched_posts']
        except: state = {}

    submissions = list(subreddit.search(
        query='subreddit:wallstreetbets title:daily discussion',
        sort='new',
        time_filter = 'day', # Can be one of: all, day, hour, month, week, year (
    ))
    submissions = [s for s in submissions if 'Daily Discussion Thread' in s.title and 'Unpinned' not in s.title]
    if len(submissions) == 0:
        print('wsb | No matching threads')
        return

    submission = submissions[0]
    if submission.id not in state: state[submission.id] = []
    extract_significant_comments(state, [submission], 500)
    print(f'WSB Refresh Complete | took {(timer() - start):.2f} seconds\n')

    

def monitor(frequency: int = 60):
    """
    Continually checks the latest daily discussion post for more comments
    """

    # Set up praw instance
    client_id, client_secret = fetch_creds()
    reddit = praw.Reddit(user_agent='Comment Extraction', client_id=client_id, client_secret=client_secret)
    subreddit = reddit.subreddit('wallstreetbets')

    # Kick off monitoring
    starttime = time.time()
    th = threading.Thread(target=check_latest_thread, args=[subreddit])
    th.start()
    while True:
        if not th.is_alive():
            time.sleep(frequency - ((time.time() - starttime) % frequency))
            th = threading.Thread(target=check_latest_thread, args=[subreddit])
            th.start()
