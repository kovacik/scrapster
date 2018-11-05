from concurrent.futures import ThreadPoolExecutor
import datetime
from enum import Enum
import shutil
import sys
import os
import time
from queue import Queue, Empty
import requests
from bs4 import BeautifulSoup
import tldextract

REQUEST_TIMEOUT_SEC = 10
ATTEMPT_SCRAPING_TIMEOUT = 20
ENQUEUE_FOR_SCRAPING_TIMEOUT = 0
MAX_SCRAPE_QUEUE_SIZE = 160
MAX_SCRAPE_WORKERS = 8


class ScrapeAction(Enum):
  GET_IMAGES_AND_SUBLINKS = 1
  DOWNLOAD_IMAGE = 2


def get_valid_url(url):

  if not url.startswith("http"):
    return "http://" + url

  return url


def ensure_file_directory_exists(file_path):

  directory = os.path.dirname(file_path)
  if not os.path.exists(directory):
    os.makedirs(directory)

  return file_path


class SiteImageDownloader:

  def __init__(self, site_url, download_directory="images"):

    self._site_url = site_url
    parsed_site_url = tldextract.extract(site_url)
    self._domain = '{}.{}'.format(parsed_site_url.domain,
                                  parsed_site_url.suffix)
    self._download_directory = download_directory
    self._request_headers = {
        'user-agent':
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 '
            '(KHTML, like Gecko) Chrome/53.0.2785.143 Safari/537.36'
    }

    self._work_queue = Queue(maxsize=MAX_SCRAPE_QUEUE_SIZE)
    self._already_scraped_urls = set([])
    self._already_downloaded_images = set([])

  def _get_same_domain_url_from_link(self, link):

    if self._domain not in link or link.startswith("mailto"):
      return None
    elif link.startswith("/") and len(link) > 1:
      link = self._site_url + link

    return get_valid_url(link)

  def _get_url_from_link(self, link):

    if link is None:
      return link

    if link.startswith("//") and len(link) > 2:
      link = link[2:]
    elif link.startswith("/") and len(link) > 1:
      link = self._site_url + link

    return get_valid_url(link)

  def _download_image(self, url):

    image_file_path = self._download_directory + "/" + url.split("/")[-1]

    if os.path.isfile(image_file_path):
      return

    image_file_path = ensure_file_directory_exists(image_file_path)

    try:
      response = requests.get(
          url,
          headers=self._request_headers,
          stream=True,
          timeout=REQUEST_TIMEOUT_SEC)

      with open(image_file_path, 'wb') as f:
        shutil.copyfileobj(response.raw, f)

    except Exception as ex:
      # TODO - catch more specific exceptions :)
      print("Unable to download image: " + str(url))
      print("Exception caught: " + str(ex))

  def _get_site_sublinks_and_images(self, url):

    sublinks = set()
    images = set()

    try:
      r = requests.get(
          url, headers=self._request_headers, timeout=REQUEST_TIMEOUT_SEC)

      if r.status_code == 200:
        html = r.text
        soup = BeautifulSoup(html, 'lxml')
        content_type = r.headers['content-type']

        if content_type is not None and content_type.startswith("image"):
          images.add(url)
        elif content_type is not None and content_type.startswith("text/html"):
          for img in soup.findAll('img'):
            image = self._get_url_from_link(img.get('src'))
            if image and image not in images:
              self._work_queue.put((ScrapeAction.DOWNLOAD_IMAGE, image),
                                   timeout=ENQUEUE_FOR_SCRAPING_TIMEOUT)
              images.add(image)

          for tag in soup.find_all('a', href=True):
            link = self._get_same_domain_url_from_link(tag['href'])
            if link and link not in sublinks:
              self._work_queue.put((ScrapeAction.GET_IMAGES_AND_SUBLINKS, link),
                                   timeout=ENQUEUE_FOR_SCRAPING_TIMEOUT)
              sublinks.add(link)

    except Exception as ex:
      # TODO - catch more specific exceptions :)
      print("Error during getting of images and sublinks from url: " + str(url))
      print("Exception caught: " + str(ex))

  def download_site_images(self):

    start = time.time()

    with ThreadPoolExecutor(max_workers=MAX_SCRAPE_WORKERS) as executor:

      self._work_queue.put((ScrapeAction.GET_IMAGES_AND_SUBLINKS,
                            self._site_url))

      while True:
        try:
          (action, url) = self._work_queue.get(timeout=ATTEMPT_SCRAPING_TIMEOUT)

          if (action == ScrapeAction.GET_IMAGES_AND_SUBLINKS and
              url not in self._already_scraped_urls):
            self._already_scraped_urls.add(url)
            print("Scraping url: " + url + "\nCount so far ~" +
                  str(len(self._already_scraped_urls)))
            executor.submit(self._get_site_sublinks_and_images, url)

          elif action == ScrapeAction.DOWNLOAD_IMAGE and url not in self._already_downloaded_images:
            self._already_downloaded_images.add(url)
            print("Downloading image: " + url + "\nDownloaded so far ~" +
                  str(len(self._already_downloaded_images)))
            executor.submit(self._download_image, url)

        except Empty:
          break

    end = time.time()

    print("Scraping took [hh:mm:ss]: " +
          str(str(datetime.timedelta(seconds=end - start))))


if __name__ == '__main__':
  if len(sys.argv) > 1:
    website_url = sys.argv[1]
    download_dir = sys.argv[2]
    scraper = SiteImageDownloader(website_url, download_dir)
    scraper.download_site_images()
  else:
    print("No site to download images from provided")
