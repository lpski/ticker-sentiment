# ticker-sentiment


## Setup Instructions
Currently, the program requires elasticsearch to be running locally (remote support planned eventually). For simplicity, a docker-compose file is provided in the data folder. With docker installed locally and having navigated to the data folder, you can run the following to start up elasticsearch:
```
# Start Elasticsearch as a daemon process
docker-compose up -d

# Check if running 
curl http://127.0.0.1:9200/_cat/health

# Kill Elasticsearch
docker-compose down -v
# or 
docker container ls
docker stop [con-id]

# Logs
docker logs elasticsearch
```


## Available News/Post Sources
Currently, there are XX sources to choose from as listed below with notes

*Can run in background without any additional setup*

    - market_watch
    - ap
    - benzinga
    - cnn
    - pr_newswire
    - yahoo finance
    - forbes
    - the motley fool
    - hackernews (buggy implementation currently)

*Require API Credentials*

    - Reddit(wallstreetbets)
        - Requires REDDIT_CLIENT_ID & REDDIT_SECRET in a .env file


*Can run via selenium using a chrome browser driver*

    - Each of the following require CHROME_DRIVER_PATH to be set in a .env file
        - seeking_alpha
        - cnbc
        - bloomberg
        - reuters
        - investors



## Run the Program
With elasticsearch running you can now start the program in one of two modes:
    - historical: Fetches historical postings (how far back depends on the source)
    - live: Fetches very recent postings + continuously checks for new posts every n seconds (default is 300)

You're able to configure which sources are used in the `main.py` and `scrapers/mixed.py` files. By default, the wallstreetbets subreddit and the background-capable sources listed above (excluding hackernews) are used.



## Future Work
TBD
