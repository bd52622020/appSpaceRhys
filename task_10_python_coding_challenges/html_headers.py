#!/usr/bin/env python
import requests

def main(site):
    r = requests.request('GET', site)
    print(r.headers)
    
if __name__ == "__main__":
    site = "http://en.wikipedia.org/wiki/Main_Page"
    main(site)