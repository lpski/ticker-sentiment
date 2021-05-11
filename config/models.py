from typing import Dict, Set, List, NamedTuple, Any
import datetime as dt

class SymbolData():
    symbols: Set[str]
    companies: Dict[str, str]

    def __init__(self) -> None:
        self.symbols = set()
        self.companies = {}


class SentimentContext():
    symbol: str     # Relevant stock symbol
    source: str     # Domain source for the sentiment (e.g. hackernews, forbes, etc.)
    timestamp: int  # Unix timestamp for context creation date
    rating: float   # Compound polarity rating of the post context in [-1,1] range with 0 being neutral

    def __init__(self, symbol: str, source: str, timestamp: int, rating: float) -> None:
        self.symbol = symbol
        self.source = source
        self.timestamp = timestamp
        self.rating = rating


class Analysis():
    """
    Analysis is the class representing an analysis results post-fetch & post-sentiment analysis
    """
    # data: Dict[str, SentimentContext] # Map of stock symbols to a sentiment context
    data: List[SentimentContext] # Map of stock symbols to a sentiment context

    def __init__(self, data: List[SentimentContext] = None) -> None:
        self.data = data if data is not None else []



class Sentiment(NamedTuple):
    symbol: str
    source: str
    timestamp: int
    rating: float


class HN_Item():
    descendants: int
    id: int
    score: int
    time: int
    title: str
    text: str
    type: str # can be any of ['story', 'comment', 'job', 'poll', 'pollopt']
    url: str
    kids: List[int]

    def __init__(self, id: int, descendants: int = 0, score: int = 0, time: int = 0, title: str = '', text: str = '', type: str = '', url: str = '', kids: List[int] = None) -> None:
        self.id = id
        self.descendants = descendants
        self.score = score
        self.time = time
        self.title = title
        self.text = text
        self.type = type
        self.url = url
        self.kids = kids if kids is not None else []

    @classmethod
    def from_json(cls, data: dict):
        # remove unwanted fields
        for field in [k for k in data.keys() if k not in ['descendants', 'id', 'score', 'time', 'title', 'text', 'type', 'url', 'kids']]:
            del data[field]
        return cls(**data)

class HN_Post(NamedTuple):
    story: HN_Item
    comments: List[HN_Item]

class TweetData():
    symbols: List[str]
    timestamp: int
    body: str
    id: int

    def __init__(self, message: Dict[str, Any]):
        self.symbols = [entry['symbol'] for entry in message['symbols']]
        self.id = message['id']
        self.body = message['body']
        self.timestamp = int(dt.datetime.strptime(message['created_at'], '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=dt.timezone.utc).timestamp())

