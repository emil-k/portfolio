import requests
import pandas as pd
import numpy as np
import re
from bs4 import BeautifulSoup
import string
from collections import Counter
import time

class WikiUrl:
    def __init__(self, url:str):
        '''
        :param url: str, wikipedia link to a website in german or english.
        '''
        self.url = url
        self.lang = self.detect_language_from_url()
        self.html = self.scrape()

    def detect_language_from_url(self) -> str:
        if "://de." in self.url:
            return "DE"
        elif "://en." in self.url:
            return "EN"
        else:
            raise Exception(f"The url {self.url} is not English or German")

    def scrape(self) -> str:
        '''
        :return: str, raw html data.
        '''
        try:
            r = requests.get(self.url)
            if r.status_code == 200:
                return r.text
            else:
                raise Exception(f"GET request to {self.url} returned code {r.status_code}")
        except Exception as e:
            # Here, catching any exception is bad practice, and ideally I would catch different exceptions,
            # such as timeouts, connection errors, TooManyRedirects and act accordingly
            # (keep retrying if timeout, question url's  validity if too many redirects etc)
            # Also, I wouldn't print the exception, but i would log it somewhere, like a postgres exception table.
            # (but this goes beyond the scope of this challenge)
            print(str(e))
            raise e
    def __str__(self):
        return f"Data in {self.lang} for {self.url}"


class SourceHtml:
    characters = string.ascii_lowercase + "äöü"
    def __init__(self, html):
        self.html = html

    def to_soup(self) -> BeautifulSoup:
        '''
        Parse the raw text GET reponse to a BeautifulSoup element to be able to further parse the content.
        :return: bs4.BeautifulSoup
        '''
        soup = BeautifulSoup(self.html, "lxml")
        return soup

    def remove_tags(self, soup: BeautifulSoup) -> str:
        '''
        The html is polluted with markup elements. We are only interested in semantics
        (alphabet letters) so those should be removed.

        :param soup: bs4.BeautifulSoup
        :return: str
        '''
        text = soup.find_all(text=True)
        assert text, "No text elements found in Soup"
        clean_text = ''
        blacklist = ['[document]', 'script', 'header', 'html',
                     'meta', 'head', 'input', 'script', ]

        for t in text:
            if t.parent.name not in blacklist:
                clean_text += '{} '.format(t)
        clean_text = re.sub("\n|\xa0", " ", clean_text)
        return clean_text.lower()

    def get_frequency(self, clean_txt:str) -> dict:
        '''
        :param clean_txt: str, text with no markup of a url.
        :return: collections.Counter, key: unique letter,
        value: count of letter in document
        '''
        bag = "".join([char for char in clean_txt if char in self.characters])
        cnt = Counter(bag)
        diff = set(self.characters) - set(cnt.keys())
        if diff:
            cnt = self.spoof_missing_keys(cnt, diff)
        return cnt

    def spoof_missing_keys(self, cnt: Counter, missing_keys:set) -> Counter:
        '''
        It can happen, that a certain page doesn't have even one occurence of a certain character.
        This is a problem, as we cannot do vector comparison, if vectors have different lengths.

        :param cnt: collections.Counter, key-frequency pairs for each unique character.
        :param missing_keys: characters not found in the source html.
        :return: collections.Counter, counter will spoofed keys with value 0.
        '''
        for key in missing_keys:
            cnt[key] = 0
        return cnt

    def run_parser(self) -> Counter:
        '''
        :return: collections.Counter, counter with frequency of each character in text.
        '''
        soup = self.to_soup()
        clean_txt = self.remove_tags(soup)
        frequency = self.get_frequency(clean_txt)
        return frequency

def get_frequency_for_group(url_list:list, delay:int=3) -> list:
    '''
    :param url_list: list of strings, url values
    :param delay: time to sleep between each GET request
    :return: list, list of collections.Counter objects representing frequency of each character in text.
    '''
    if type(url_list) is not list:
        raise TypeError(f"List expected, got {type(url_list)}")
    counters = []
    for url in url_list:
        time.sleep(delay)
        target_url = WikiUrl(url)
        html = SourceHtml(target_url.html)
        cnt = html.run_parser()
        counters.append(cnt)
    return counters