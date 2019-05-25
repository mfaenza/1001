import time
import pymongo
from urllib.request import Request, urlopen

import Parser

class Crawler:
    
    def __init__(self, max_depth=1, batch_limit=5000):
        
        # Connect to mongo instance
        myclient = pymongo.MongoClient("mongodb://localhost:27017/")
        db = myclient['1001']
        self.url_html_map = db['url_html_map']
        
        # Instantiate parser
        self.Parser = Parser.Parser()

        # Instantiate page_hash - maps already found urls to html
        # Used to condition on if we've visited a page
        self.page_hash = {}
        for page in self.url_html_map.find({}):
            self.page_hash[page['url']] = 1
            
        # Stopping indicator
        self.stop_search = False
        self.batch_limit = batch_limit
        
        # Vars
        self.urls = {}
        self.max_depth = max_depth


    def find_str(self, s, char, start_index=0):

        index = 0
        s = s[start_index+1:]
        if char in s:
            c = char[0]
            for ch in s:
                if ch == c:
                    if s[index:index+len(char)] == char:
                        return start_index + 1 + index
                index += 1
        return -1

    def request(self,url):

        req = Request(url,\
                      headers={'User-Agent': 'Mozilla/5.0'})
        html = str(urlopen(req).read())
        return html
    
    def parse(self, url, html): 

        self.Parser.parse(url, html)
    
    def crawl(self, url, depth):
        
        if len(self.Parser.tracklist_docs) == self.batch_limit:
            print('STOPPING SEARCH')
            self.stop_search = True
        
        print('Depth:', depth)
        if (depth == self.max_depth) or (self.stop_search):
            return
        
        # Check if already reached by search or in frontier
        if (self.page_hash.get(url, 0) == False) and (self.urls.get(url, 0) == False):
            
            # Only sleep if about to request
            time.sleep(5)
            
            # Make http request
            try:
                html = self.request(url)
            except:
                return
            
            print(url)
            
            # If html, parse and extract necessary data 
            if ('/tracklist/' in url):
                self.parse(url, html)
                    
            index = 0
            # Iterate over links found in html
            while self.find_str(html, 'a href="', index) != -1:
                
                # Extract url
                index = self.find_str(html, 'a href="', index)
                url_chunk = html[index:].split('"')[1]
                
                # Make sure it is either a referenced tracklist or 1001 page
                if ('/tracklist/' in url_chunk) and\
                   ('http' not in url_chunk) and\
                   ('#tlp' not in url_chunk):
                
                    self.urls[url] = 1
                    new_page = 'https://www.1001tracklists.com' + url_chunk
                    self.crawl(new_page, depth + 1)
                
                if ('/dj/' in url_chunk) and\
                   ('http' not in url_chunk) and\
                   ('#tlp' not in url_chunk):
                
                    self.urls[url] = 1
                    new_page = 'https://www.1001tracklists.com' + url_chunk
                    self.crawl(new_page, depth + 1)
                
                if ('www.1001tracklists.com' in url_chunk) and\
                   ('#tlp' not in url_chunk) and\
                   ('.xml' not in url_chunk):

                    self.urls[url] = 1
                    self.crawl(url_chunk, depth + 1)
                    
            # Cache url-html map
            self.page_hash[url] = html
                    
        return
        
    def start_crawl(self, startUrl):
        
        depth = 0
        self.crawl(startUrl, depth)
        
        
        