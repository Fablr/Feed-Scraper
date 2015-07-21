__author__ = 'Chris Day'
__publisher__ = 'Fabler'

from bs4 import BeautifulSoup
from multiprocessing import Pool
from corgi_cache import CorgiCache
from email.utils import formatdate
from xml.sax.saxutils import unescape
import urllib
import requests
import logging
import time
import getopt
import sys
import datetime

RFC_2822_FORMAT = '%a, %d %b %Y %H:%M:%S %Z'
RFC_2822_FORMAT_NO_SECONDS = '%a, %d %b %Y %H:%M %Z'
API_FORMAT = '%Y-%m-%dT%H:%M:%S'
API_DURATION_FORMAT = '%H:%M:%S'
USER_AGENT = 'Fabler Crawler'
FABLER_URL_FORMAT = 'http://fablersite-dev.elasticbeanstalk.com/{0}/'
REFRESH_DATA_FORMAT = 'grant_type=refresh_token&client_id=rA0qBDvsUI4MUYmpeylMPgZUAMojpnLRfvu1L3iW&refresh_token={0}'


class PodcastFeedParser:
    def __init__(self, url, etag="", last_request=""):
        self.url = url
        self.episodes = []

        header = self._generate_header(etag=etag, last_visit=last_request, user_agent=USER_AGENT)
        self.response = requests.get(url=url, headers=header)
        if 200 != self.response.status_code:
            logging.error("status code {0} from {1}".format(self.response.status_code, url))
            raise IOError

        self.xml = BeautifulSoup(self.response.content, "html.parser")
        return

    def get_etag(self):
        if 'ETag' not in self.response.headers:
            logging.error("no etag present for {0}".format(self.url))
            raise IOError
        return self.response.headers['ETag']

    def get_owner(self):
        owner = self.xml.find('itunes:owner')
        if owner is not None:
            name = owner.find('itunes:name')
            if name is None:
                logging.warning("no name for the owner of {0}".format(self.url))
                raise IOError
            else:
                name = self._clean_text(name.text)

            email = owner.find('itunes:email')
            if email is None:
                logging.warning("no email for the owner of {0}".format(self.url))
                email = ''
            else:
                email = self._clean_text(email.text)
        else:
            name = self.get_author()
            email = ''

        return {'name': name, 'email': email}

    def get_title(self):
        title = self.xml.find('title')
        if title is None:
            logging.error("invalid title for {0}".format(self.url))
            raise IOError
        text = self._clean_text(title.text)
        return text

    def get_author(self):
        author = self.xml.find('itunes:author')
        if author is None:
            logging.error("invalid author for {0}".format(self.url))
            raise IOError
        text = self._clean_text(author.text)
        return text

    def get_image(self):
        image = self.xml.find('itunes:image')
        if image is None or not image.has_attr('href'):
            logging.error("invalid image for {0}".format(self.url))
            raise IOError
        url = image['href']
        return url

    def get_summary(self):
        summary = self.xml.find('itunes:summary')
        if summary is None:
            logging.error("no summary for {0}".format(self.url))
            html = ''
        else:
            html = self._clean_text(summary.text)
        return html

    def get_category(self):
        category = self.xml.find('itunes:category')
        if category is None or not category.has_attr('text'):
            logging.error("invalid category for {0}".format(self.url))
            raise IOError
        text = category['text']
        return text

    def get_explicit(self):
        explicit = self.xml.find('itunes:explicit')
        if explicit is None:
            return False
        text = explicit.getText()
        return False if "no" == text else True

    def get_link(self):
        link = self.xml.find('link')
        if link is None:
            logging.error("invalid link for {0}".format(self.url))
            raise IOError
        text = link.getText()
        return text

    def get_language(self):
        language = self.xml.find('language')
        if language is None:
            logging.error("invalid language for {0}".format(self.url))
            raise IOError
        text = language.getText()
        return text

    def has_new_feed(self):
        result = False
        new_feed = self.xml.find('itunes:new-feed-url')
        if new_feed is not None:
            result = True
        return result

    def get_new_feed(self):
        new_feed = self.xml.find('itunes:new-feed-url')
        if new_feed is None:
            logging.error("no new feed for {0}".format(self.url))
            raise IOError
        text = new_feed.getText()
        return text

    def get_copyright(self):
        podcast_copyright = self.xml.find('copyright')
        if podcast_copyright is None:
            logging.error("no copyright for {0}".format(self.url))
            text = ''
        else:
            text = self._clean_text(podcast_copyright.text)
        return text

    def get_blocked(self):
        blocked = self.xml.find('itunes:blocked')
        if blocked is None:
            text = 'no'
        else:
            text = blocked.getText()
        return False if 'no' == text else True

    def get_complete(self):
        complete = self.xml.find('itunes:complete')
        if complete is None:
            text = 'no'
        else:
            text = complete.getText()
        return False if 'no' == text else True

    def get_keywords(self):
        keywords = self.xml.find('itunes:keywords')
        if keywords is None:
            logging.error("no keywords for {0}".format(self.url))
            text = ''
        else:
            text = self._clean_text(keywords.text)
        return text

    def get_episode(self, i):
        result = {}
        self._populate_episodes()

        if len(self.episodes) <= i:
            logging.error("invalid episode number for {0}".format(self.url))
            logging.error("requesting {0} of {1}".format(i, len(self.episodes)))
            raise IndexError

        episode = self.episodes[i]

        title = episode.find('title')
        if title is not None:
            result['title'] = self._clean_text(title.text)

        link = episode.find('enclosure')
        if link is not None:
            if link.has_attr('url'):
                url = link['url']
                result['link'] = url
        else:
            link = episode.find('link')
            if link is not None:
                url = link.getText()
                if url is None:
                    if link.has_attr('url'):
                        url = link['url']
                        result['link'] = url
                else:
                    result['link'] = url

        subtitle = episode.find('itunes:subtitle')
        if subtitle is not None:
            result['subtitle'] = self._clean_text(subtitle.text)

        blocked = episode.find('itunes:blocked')
        if blocked is None:
            text = 'no'
        else:
            text = blocked.getText()
        result['blocked'] = False if 'no' == text else True

        description = episode.find('description')
        if description is None:
            description = episode.find('itunes:summary')
        if description is not None:
            result['description'] = self._clean_text(description.text)

        pubdate = episode.find('pubdate')
        if pubdate is not None:
            pubdate_string = pubdate.getText()
            try:
                result['pubdate'] = time.strptime(pubdate_string, RFC_2822_FORMAT)
            except ValueError:
                try:
                    result['pubdate'] = time.strptime(pubdate_string, RFC_2822_FORMAT_NO_SECONDS)
                except ValueError:
                    logging.warning("invalid time format for pubdate, {0}".format(pubdate_string))

        duration = episode.find('itunes:duration')
        if duration is not None:
            result['duration'] = self._clean_text(duration.text)

        explicit = episode.find('itunes:explicit')
        if explicit is not None:
            explicit_string = explicit.getText()
            result['explicit'] = False if 'no' == explicit_string else True
        else:
            result['explicit'] = False

        keywords = episode.find('itunes:keywords')
        if keywords is not None:
            result['keywords'] = self._clean_text(keywords.text)

        guid = episode.find('guid')
        if guid is None:
            guid = episode.find('link')
        if guid is not None:
            result['guid'] = guid.text

        return result

    def get_all_episodes(self):
        result = []
        self._populate_episodes()
        count = len(self.episodes)
        for i in range(0, count):
            result.append(self.get_episode(i))
        return result

    def get_new_episodes(self, guids):
        if not callable(getattr(guids, '__contains__')):
            logging.error("invalid param does have contains method, {0}".format(guids))
            raise AssertionError
        result = []
        self._populate_episodes()
        count = len(self.episodes)
        for i in range(0, count):
            episode = self.get_episode(i)
            if 'guid' not in episode:
                logging.error("no guid for episode, {0}".format(episode))
                raise ValueError
            if episode['guid'] not in guids:
                result.append(episode)
        return result

    def _populate_episodes(self):
        if len(self.episodes) == 0:
            self.episodes = self.xml.find_all('item')

    @staticmethod
    def _clean_text(text):
        text = unescape(text, {"&apos;": "'", "&quot;": '"'})
        text = ' '.join(text.split())
        text = text.strip()
        return text

    @staticmethod
    def _generate_header(etag="", last_visit="", user_agent=""):
        header = {}

        if "" != etag:
            header['If-Not-Match'] = etag

        if "" != last_visit:
            header['If-Modified-Since'] = last_visit

        if "" != user_agent:
            header['User-Agent'] = user_agent

        return header


def post_data(table_name, data, token):
    authorization = "Bearer {0}".format(token)
    header = {'Authorization': authorization, 'Content-Type': 'application/x-www-form-urlencoded'}

    request_data = ""
    for key in data:
        if '' == data[key]:
            continue

        if request_data != "":
            request_data += "&"

        if type(data[key]) is time.struct_time:
            value = time.strftime(API_FORMAT, data[key])
            value = urllib.parse.quote(value)
        elif type(data[key]) is datetime.datetime:
            value = time.strftime(API_DURATION_FORMAT, data[key])
            value = urllib.parse.quote(value)
        elif type(data[key]) is str:
            value = urllib.parse.quote(data[key])
        else:
            value = data[key]

        request_data += "{0}={1}".format(key, value)

    url = FABLER_URL_FORMAT.format(table_name)

    result = requests.post(url=url, headers=header, data=request_data)

    if 300 <= result.status_code:
        raise IOError

    return result


def get_data(table_name, data_filter, token):
    authorization = "Bearer {0}".format(token)
    header = {'Authorization': authorization, 'Content-Type': 'application/x-www-form-urlencoded'}

    url = FABLER_URL_FORMAT.format(table_name) + '?'
    for key in data_filter:
        if '' == data_filter[key]:
            continue

        url += "&{0}={1}".format(key, data_filter[key])

    result = requests.get(url=url, headers=header)

    if 300 <= result.status_code:
        raise IOError

    return result


def scrap_feed(feed):
    etag = ""
    last_crawled = ""

    cache = CorgiCache()
    tokens = cache.get_token(use='scraper')

    if 'URL' not in feed:
        logging.error("no feed for {0}".format(feed))
        return

    if 'ETAG' in feed:
        etag = feed['ETAG']

    if 'CRAWLED' in feed:
        last_crawled = feed['CRAWLED']

    url = feed['URL']

    parser = PodcastFeedParser(url=url, etag=etag, last_request=last_crawled)
    feed['CRAWLED'] = formatdate()

    try:
        if parser.has_new_feed():
            logging.info("new feed for {0}".format(url))

            url = parser.get_new_feed()
            logging.info("new feed is {0}".format(url))

            feed['URL'] = url
            feed.save()
            parser = PodcastFeedParser(url=url)
            feed['CRAWLED'] = formatdate()

        if parser.get_blocked():
            logging.warning("feed blocked for {0}".format(url))

        try:
            publisher = parser.get_owner()
            pub_filter = {'name': publisher['name']}
            data = get_data(table_name='publisher', data_filter=pub_filter, token=tokens['TOKEN'])
            data = data.json()
            if 0 == len(data):
                data = post_data(table_name='publisher', data=publisher, token=tokens['TOKEN'])

                data = data.json()
                if 'id' not in data:
                    raise IOError
            else:
                data = data[0]

            publisher_id = data['id']
            title = parser.get_title()
            author = parser.get_author()
            summary = parser.get_summary()
            category = parser.get_category()
            explicit = parser.get_explicit()
            link = parser.get_link()
            podcast_copyright = parser.get_copyright()
            blocked = parser.get_blocked()
            complete = parser.get_complete()
            keywords = parser.get_keywords()

            pod_filter = {'publisher': publisher_id,
                          'title': title}

            data = get_data(table_name='podcast', data_filter=pod_filter, token=tokens['TOKEN'])
            data = data.json()
            if 0 == len(data):
                podcast = {'publisher': publisher_id,
                           'title': title,
                           'author': author,
                           'summary': summary,
                           'category': category,
                           'explicit': explicit,
                           'link': link,
                           'copyright': podcast_copyright,
                           'blocked': blocked,
                           'complete': complete,
                           'keywords': keywords}

                data = post_data(table_name='podcast', data=podcast, token=tokens['TOKEN'])

                data = data.json()
                if 'id' not in data:
                    raise IOError
            else:
                data = data[0]

            podcast_id = data['id']

            guids = []
            if 'GUIDS' in feed:
                guids = feed['GUIDS']
                episodes = parser.get_new_episodes(guids)
            else:
                episodes = parser.get_all_episodes()

            for episode in episodes:
                guids.append(episode['guid'])
                episode['podcast'] = podcast_id
                del episode['guid']
                post_data(table_name='episode', data=episode, token=tokens['TOKEN'])

            feed['GUIDS'] = guids

        except IOError:
            return

        try:
            feed['ETAG'] = parser.get_etag()
        except IOError:
            pass

        feed.save()
        logging.info("finished scraping, {0}".format(url))
    except IOError:
        return

    return


def usage():
    print("usage: python Scraper.py [-h,--help] [-v,--verbose,--debug]")
    print("       [-d,--daemon] [-l,--log <path>]")
    return


def async_main(daemon_mode):
    pool = Pool()
    cache = CorgiCache()

    while True:
        feeds = cache.get_all_feeds()

        for feed in feeds:
            pool.apply_async(func=scrap_feed, args=feed)

        pool.close()
        pool.join()

        if not daemon_mode:
            break
    return


def serial_main(daemon_mode):
    cache = CorgiCache()

    while True:
        feeds = cache.get_all_feeds()

        for feed in feeds:
            scrap_feed(feed)

        if not daemon_mode:
            break
    return


if __name__ == "__main__":
    verbose = False
    debug = False
    daemon = False
    log_file = "log.txt"
    level = logging.WARNING
    argv = sys.argv[1:]

    try:
        opts, args = getopt.getopt(argv, "hvdl:", ["help", "verbose", "daemon", "log=", "debug"])
    except getopt.GetoptError:
        usage()
        sys.exit(2)

    for opt, arg in opts:
        if opt in ("-h", "--help"):
            usage()
            sys.exit()
        elif opt in ("-v", "--verbose"):
            verbose = True
        elif opt in ("-d", "--daemon"):
            daemon = True
        elif opt in ("-l", "--log"):
            log_file = arg
        elif opt == "debug":
            debug = True

    if verbose:
        level = logging.INFO

    if debug:
        level = logging.DEBUG

    logging.basicConfig(level=level, filename=log_file)

    serial_main(daemon_mode=daemon)
