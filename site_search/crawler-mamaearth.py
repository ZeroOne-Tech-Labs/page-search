import json
import multiprocessing
import os
from dataclasses import dataclass
from typing import List, Optional

from urllib.parse import urlparse, urljoin
import requests
import tqdm as tqdm
from bs4 import BeautifulSoup
from usp.fetch_parse import SitemapFetcher
from usp.objects.sitemap import IndexWebsiteSitemap, InvalidSitemap
from usp.tree import sitemap_tree_for_homepage

from site_search.config import DATA_DIR

HEADER_TAGS = ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']


@dataclass
class ProductData:
    title: str
    description: str
    subtitle: str
    url: str
    sections: Optional[List[str]] = None


def get_path_hierarchy(url: str) -> List[str]:
    """
    Input example: "/foo/bar"
    Output example: ["/foo", "/foo/bar"]

    >>> get_path_hierarchy("/foo/bar")
    ['foo', 'foo/bar']

    >>> get_path_hierarchy("foo")
    ['foo']

    >>> get_path_hierarchy("/foo/")
    ['foo']

    >>> get_path_hierarchy("foo/bar/")
    ['foo', 'foo/bar']

    """
    parsed_url = urlparse(url)
    path = parsed_url.path
    if not path:
        return []
    prefix = ''
    result = []
    for directory in path.strip('/').split("/"):
        prefix += directory
        result.append(prefix)
        prefix += '/'
    return result


def selector_soup(element):
    components = []
    child = element if element.name else element.parent
    for parent in child.parents:
        siblings = parent.find_all(child.name, recursive=False)
        components.append(
            child.name
            if siblings == [child] else
            '%s:nth-of-type(%d)' % (child.name, 1 + siblings.index(child))
        )
        child = parent
    components.reverse()
    return '%s' % ' > '.join(components)


class Crawler:
    def __init__(self, site, relative_urls=True, split_lines=True):
        self.split_lines = split_lines
        self.site = site
        self.relative_urls = relative_urls
        self.pages = []

    def download_sitemap(self, sitemap_url: Optional[str] = None):
        if not sitemap_url:
            tree = sitemap_tree_for_homepage(self.site)
            return tree.all_pages()
        else:
            sitemaps = []
            unpublished_sitemap_fetcher = SitemapFetcher(
                url=sitemap_url,
                web_client=None,
                recursion_level=0,
            )
            unpublished_sitemap = unpublished_sitemap_fetcher.sitemap()

            # Skip the ones that weren't found
            if not isinstance(unpublished_sitemap, InvalidSitemap):
                sitemaps.append(unpublished_sitemap)

            index_sitemap = IndexWebsiteSitemap(url=self.site, sub_sitemaps=sitemaps)
            return index_sitemap.all_pages()

    def crawl_page(self, url: str, content_selector="product") -> ProductData:

        sections = get_path_hierarchy(url)

        resp = requests.get(url)
        if not resp.ok:
            return None

        soup = BeautifulSoup(resp.content, 'html.parser')

        if soup is None:
            raise Exception("null soup exception")

        try:
            product_name = soup.find('h1', attrs={'class':'product-name'})
            description = soup.find('h2', attrs={'class':'product-description-header'}).parent
            subtitle = soup.find('div', attrs={'class':'product-subtitle'})
            return ProductData(
                title=product_name.text,
                description=description.find('p').text,
                subtitle=subtitle.text,
                url=url,
                sections=sections
            )
        except:
            print("Error Occured in parsing data on " + url)
        return

def download_and_save(file_name='abstracts.jsonl', split_lines=True):
    page_url = "https://mamaearth.in/"
    site_map_url = page_url + "sitemap.xml"
    crawler = Crawler(page_url, split_lines=split_lines)

    pages = crawler.download_sitemap(site_map_url)
    
    # print(pages)
    
    with open(os.path.join(DATA_DIR, file_name), 'w') as out:
        page_urls = []
        for page in pages:
            full_page_url = urljoin(page_url, page.url)
            page_urls.append(full_page_url)
        
        page_urls = list(filter(lambda x: "/product/" in x and "reviews" not in x, page_urls))
        # print(page_urls)
        
        with multiprocessing.Pool(processes=10) as pool:
            for product in tqdm.tqdm(pool.imap(crawler.crawl_page, page_urls)):
                if product is not None:
                    out.write(json.dumps(product.__dict__))
                    out.write('\n')

def dump_data(file_name='logs/soup.log', dump_data={}):
    f = open(file_name, "a")
    f.write(str(dump_data))
    f.close()
    return

if __name__ == '__main__':
    download_and_save()

    # page_url = "https://deploy-preview-79--condescending-goldwasser-91acf0.netlify.app/"
    # site_map_url = page_url + "sitemap.xml"
    # crawler = Crawler(page_url)
    #
    # abstracts = crawler.crawl_page(
    #     "https://deploy-preview-79--condescending-goldwasser-91acf0.netlify.app/documentation/search/")
    #
    # for abstract in abstracts:
    #     print(abstract)
