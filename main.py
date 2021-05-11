import threading, sys
from typing import Callable, List, Tuple
from optparse import OptionParser

from config.models import Analysis
from scrapers import analyze_wsb, analyze_mixed, analyze_hn
from scrapers import monitor_mixed, monitor_wsb, monitor_hn, monitor_twitter, analyze_twitter
from data.es import add_analyses, reset, _initialize





def start_historical_analysis(services: List[Callable]):
    """
    Starts historical article analysis for each of the provided services
    """
    analyses: List[Analysis] = [s() for s in services]
    print('All Service Analyses Complete')

def start_live_analysis(services: List[Callable]):
    """
    Starts live article monitoring for each of the provided services
    """
    
    refresh_frequency = 300
    for service in services:
        threading.Thread(target=service, args=[refresh_frequency]).start()



def initialize() -> Tuple[str, bool]:
    """
    Returns tuple indicating [run mode, reset es data]
    """
    _initialize()

    parser = OptionParser()
    parser.add_option("-m", "--mode", dest="mode", help="", metavar="STR", default="live")
    parser.add_argument('-r', '--reset', help='Reset ElasticSearch Data', action='store_true')
    (options, _) = parser.parse_args()
    return options.mode, options.reset

mode, reset_es = initialize()
print(f'\n\n\nStarting News Fetch Analsis | Mode: {mode} | Reset: {reset_es}\n')

if reset_es: reset()

if mode == 'analysis': print('Analysis Mode Not Yet Implemented')
elif mode == 'historical':
    start_historical_analysis(
        services=[
            analyze_wsb,
            analyze_mixed,
            # analyze_hn,
            # analyze_twitter,
        ]
    )
elif mode == 'live':
    start_live_analysis(
        services=[
            monitor_wsb,
            monitor_mixed,
            # monitor_hn,
            # monitor_twitter,
        ]
    )
