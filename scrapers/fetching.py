import time, requests, re, fake_useragent as fu
from typing import Tuple, Union
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support.expected_conditions import presence_of_element_located
from selenium.webdriver.support.select import By
from dotenv import load_dotenv, dotenv_values

# Source-Specific Support
def _seeking_alpha(bs: BeautifulSoup):
    try:
        blocks = [block.text for block in bs.find_all('div', attrs={'data-test-id' : 'content-container'})]
        return ''.join(blocks)
    except Exception as e:
        print('SeekingAlpha Text Extraction Error:', e)





# Extraction
def browser_verification(browser: webdriver.Chrome, source: str):
    def click_and_hold(driver, element): ActionChains(driver).click_and_hold(element).perform()
    
    print(f'{source} verification required')
    if source == 'seeking_alpha':
        try:
            iframe = browser.find_element_by_tag_name('iframe')
            click_and_hold(browser, iframe)
            time.sleep(4)
            WebDriverWait(browser, 20).until(presence_of_element_located((By.CSS_SELECTOR, "*[data-test-id='content-container']")))
            return
        except: return
    else:
        return

def browser_wait(browser: webdriver.Chrome, source: str, duration: int = 10):
    if source == 'seeking_alpha':
        try: WebDriverWait(browser, duration).until(presence_of_element_located((By.CSS_SELECTOR, "*[data-test-id='content-container']")))
        except:
            soup = BeautifulSoup(browser.page_source, 'html.parser')            
            requires_validation = soup.find('h1', text=re.compile(r'To continue, please prove you are not a robot')) is not None
            if requires_validation: browser_verification(browser, 'seeking_alpha')

    elif source == 'bloomberg':
        """
        For bloomberg we use multiple waits, in order:
            - first we do a short wait for a valid page
            - if capcha page do a long wait, else return
        """

        # content = WebDriverWait(browser, duration).until(presence_of_element_located((By.CSS_SELECTOR, 'div.article-content')))
        try:  WebDriverWait(browser, 10).until(presence_of_element_located((By.CSS_SELECTOR, 'div.instruments__c06d06c1')))
        except: print('browser_wait:bloomberg: could not find div.instruments__c06d06c1 in page')

        return ''

    elif source == 'investors':
        pass

    return

def browser_required(source: str) -> Tuple[bool, bool]:
    """
    returns: [browser required, headless allowed]
    """
    if source in ['seeking_alpha', 'investors', 'bloomberg']: return (True, False)
    return (False, False)




def fetch_article(url: str, source: str, parser: str = 'lxml') -> Union[str, BeautifulSoup]:
    browser_is_required, headless = browser_required(source)

    if browser_is_required:
        try:
            load_dotenv()
            env = dotenv_values('.env')
            if env is None: raise Exception('No .env file')
            if 'CHROME_DRIVER_PATH' in env: chrome_driver = env['CHROME_DRIVER_PATH']
            else: raise Exception('No chrome driver path provided')
        except Exception as e: raise Exception(f'Fetching.py: Error Loading Environment Variables: {e}')

        opts = webdriver.ChromeOptions()
        # ua = fu.UserAgent().random
        opts.add_argument(f'--user-data-dir={chrome_driver}')
        # opts.add_argument(f'user-agent={ua}')
        opts.add_argument("--enable-javascript")
        # opts.add_argument("javascript.enabled", True)
        if headless: opts.add_argument('--headless')
        browser = webdriver.Chrome(executable_path="/usr/local/bin/chromedriver", options=opts)
        # browser = webdriver.Safari()
        browser.delete_all_cookies()
        browser.get(url)

        try: browser_wait(browser, source)
        except Exception as e:
            if source in ['seeking_alpha']:
                # browser_verification(browser, source)
                try: browser_wait(browser, source, 60)
                except:
                    browser.close()
                    return ''
            else:
                browser.close()
                return None
    
        soup = BeautifulSoup(browser.page_source, parser)
        browser.close()
    else:
        res = requests.get(url).text
        soup = BeautifulSoup(res, parser)

    if source == 'seeking_alpha' and False:
        content = _seeking_alpha(soup)
        return content
    else: return soup


