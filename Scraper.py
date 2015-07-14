__author__ = 'Chris Day'
__publisher__ = 'Fabler'

from bs4 import BeautifulSoup
import requests
import logging
import time

RFC_2822_FORMAT = '%a, %d %b %Y %H:%M:%S %Z'


class PodcastFeedParser:
    def __init__(self, url):
        self.url = url
        self.episodes = []
        self.response = requests.get(url)
        if 200 != self.response.status_code:
            logging.error("status code {0} from {1}".format(self.response.status_code, url))
            raise IOError
        self.xml = BeautifulSoup(self.response.content, "html.parser")

    def get_title(self):
        title = self.xml.find('title')
        if title is None:
            logging.error("invalid title for {0}".format(self.url))
            raise IOError
        text = title.getText()
        return text

    def get_author(self):
        author = self.xml.find('itunes:author')
        if author is None:
            logging.error("invalid author for {0}".format(self.url))
            raise IOError
        text = author.getText()
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
            logging.error("invalid summary for {0}".format(self.url))
            raise IOError
        html = summary.getText()
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
            logging.error("invalid explicit value for {0}".format(self.url))
            raise IOError
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
            logging.error("invalid copyright for {0}".format(self.url))
            raise IOError
        text = podcast_copyright.getText()
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
            logging.error("invalid keywords for {0}".format(self.url))
            raise IOError
        text = keywords.getText()
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
            result['title'] = title.getText()

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
            result['subtitle'] = subtitle.getText()

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
            result['description'] = description.getText()

        pubdate = episode.find('pubdate')
        if pubdate is not None:
            pubdate_string = pubdate.getText()
            try:
                result['date'] = time.strptime(pubdate_string, RFC_2822_FORMAT)
            except ValueError:
                logging.warning("invalid time format for pubdate, {0}".format(pubdate_string))

        duration = episode.find('itunes:duration')
        if duration is not None:
            duration_string = duration.getText()
            count = duration_string.count(':')
            time_format = None
            if 0 == count:
                time_format = '%S'
            elif 1 == count:
                time_format = '%M:%S'
            elif 2 == count:
                time_format = '%H:%M:%S'

            if time_format is not None:
                try:
                    result['duration'] = time.strptime(duration_string, time_format)
                except ValueError:
                    logging.warning("invalid time format for duration, {0}, {1}".format(duration_string, time_format))

        explicit = episode.find('itunes:explicit')
        if explicit is not None:
            explicit_string = explicit.getText()
            result['explicit'] = False if 'no' == explicit_string else True
        else:
            result['explicit'] = False

        keywords = episode.find('itunes:keywords')
        if keywords is not None:
            result['keywords'] = keywords.getText()

        guid = episode.find('guid')
        if guid is None:
            guid = episode.find('link')
        if guid is not None:
            result['guid'] = guid.getText()

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


def async_main():
    pass


def serial_main():
    pass


if __name__ == "__main__":
    serial_main()
