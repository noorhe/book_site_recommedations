import scrapy
from os import environ as env
import json
from itemadapter import ItemAdapter
from dataclasses import dataclass
from lxml import etree
from io import StringIO
import logging
import sys
import copy
import time

from request_priorityzer import Priorityzer


class UserBookRate(scrapy.Item):
    __type__ = "UserBookRate"
    userId = scrapy.Field()
    book_url = scrapy.Field()
    rate = scrapy.Field()


class UserIdPagePair:
    def __init__(self, userId, page=1):
        self.__type__ = "UserIdPagePair"
        self.userId = userId
        self.page = page

    def __hash__(self):
        return hash((self.userId, self.page))

    def __eq__(self, other):
        if not isinstance(other, type(self)):
            return NotImplemented
        return self.userId == other.userId and self.page == other.page

    def __str__(self):
        return f"UserIdPagePair: (userId: {self.userId}, page: {self.page})"


@dataclass(frozen=True)
class StringPair:
    str1: str
    str2: str
    __type__: str = "StringPair"


@dataclass(frozen=True)
class StringPairAndInt:
    str1: str
    str2: str
    num: int
    __type__: str = "StringPairAndInt"


@dataclass(frozen=True)
class StringQuartet:
    str1: str
    str2: str
    str3: str
    str4: str
    __type__: str = "StringQuartet"


class Book(scrapy.Item):
    __type__ = "Book"
    book_url = scrapy.Field()
    title = scrapy.Field()
    author = scrapy.Field()
    avg_rating = scrapy.Field()
    readers_num = scrapy.Field()
    going_to_read_num = scrapy.Field()
    publishers = scrapy.Field()
    year_published = scrapy.Field()
    series = scrapy.Field()
    pubseries = scrapy.Field()
    number_in_cycle = scrapy.Field()
    language = scrapy.Field()
    cover_type = scrapy.Field()
    age_limit = scrapy.Field()
    rates_distribution = scrapy.Field()


class Genre(scrapy.Item):
    __type__ = "Genre"
    book_url = scrapy.Field()
    genre = scrapy.Field()


class Tag(scrapy.Item):
    __type__ = "Tag"
    book_url = scrapy.Field()
    tag = scrapy.Field()


class Selection(scrapy.Item):
    __type__ = "Selection"
    book_url = scrapy.Field()
    selection = scrapy.Field()


class RateDistribution(scrapy.Item):
    __type__ = "RateDistribution"
    book_url = scrapy.Field()
    rate_distribution = scrapy.Field()


class PrintItem:
    async def process_item(self, item, spider):
        logger.info(f"tr1234 __type__: {item.__type__} item: {item.items()}")
        return item


class WriteItemToJson:
    def open_spider(self, spider):
        self.userBookRates = open(f'run/user_book_rates.json', 'at')
        self.bookGenres = open(f'run/genres.json', 'at')
        self.books = open(f'run/books.json', 'at')
        self.tags = open(f'run/tags.json', 'at')
        self.selections = open(f'run/selections.json', 'at')
        self.rate_distributions = open(f'run/rate_distributions.json', 'at')

    def close_spider(self, spider):
        self.userBookRates.close()
        self.bookGenres.close()
        self.books.close()
        self.tags.close()
        self.selections.close()
        self.rate_distributions.close()

    async def process_item(self, item, spider):
        line = json.dumps(ItemAdapter(item).asdict(),
                          ensure_ascii=False) + "\n"
        logger.info(f"tr1234 __type__: {item.__type__} line: {line}")
        if (item.__type__ == "UserBookRate"):
            self.userBookRates.write(line)
        if (item.__type__ == "Genre"):
            self.bookGenres.write(line)
        if (item.__type__ == "Book"):
            self.books.write(line)
        if (item.__type__ == "Tag"):
            self.tags.write(line)
        if (item.__type__ == "Selection"):
            self.selections.write(line)
        if (item.__type__ == "RateDistribution"):
            self.rate_distributions.write(line)
        return item


class LogEntry:
    def __init__(self, op, type, data):
        self.op = op
        self.type = type
        self.data = data


class SpecialEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, LogEntry) or isinstance(
                obj, UserIdPagePair) or isinstance(
                    obj, StringPair) or isinstance(obj, StringQuartet):
            return obj.__dict__
        return json.JSONEncoder.default(self, obj)


logger = logging.getLogger("BookSiteSpider")
logger.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
logger.addHandler(handler)


class BookSiteSpider(scrapy.Spider):
    custom_settings = {
        "LOG_STDOUT": True,
        "ITEM_PIPELINES": {
            PrintItem: 100,
            WriteItemToJson: 200
        },
        "DOWNLOAD_DELAY": 3
    }
    SELENIUM_DRIVER_NAME = 'firefox'
    SELENIUM_DRIVER_EXECUTABLE_PATH = "../geckodriver/geckodriver"
    SELENIUM_DRIVER_ARGUMENTS = ['-headless']
    DOWNLOADER_MIDDLEWARES = {'scrapy_selenium.SeleniumMiddleware': 800}
    user_id_page_pairs_to_parse = set()
    user_id_page_parsed_pairs = set()
    books_to_parse = set()
    books_parsed = set()
    tags_to_parse = set()
    tags_parsed = set()
    readers_to_parse = set()
    readers_parsed = set()
    selections_to_parse = set()
    selections_parsed = set()
    distributions_to_parse = set()
    distributions_parsed = set()
    prior = Priorityzer()

    name = 'BookSiteSpider'
    parsed_books = {}
    set_limits = {}
    domain = ""

    def __init__(self, book_site_name=None, *args, **kwargs):
        super(BookSiteSpider, self).__init__(*args, **kwargs)
        logger.info(
            f"tr1234 BookSiteSpider.__init__ book_site_name: {book_site_name}")
        self.domain = f"https://www.{book_site_name}.ru"

    def start_requests(self):
        self.log_file = open(f'run/log.json')
        logger.info("initialialization...")
        init_start = time.perf_counter_ns()
        log_entries_num = self.initialize()
        init_stop = time.perf_counter_ns()
        logger.info(
            f"initialialization took {(init_stop - init_start) / 1_000_000_000} sec for {log_entries_num} log entries"
        )
        self.print_limits()
        self.log_file.close()
        self.log_file = open(f'run/log.json', 'at')
        user_id_page_pairs_to_parse_copy = copy.copy(
            self.user_id_page_pairs_to_parse)
        for userIdPagePair in user_id_page_pairs_to_parse_copy:
            logger.info("tr1234 before check_and_descrease_set_limit")
            _url = self.build_user_read_list_url(userIdPagePair)
            logger.info(
                f"tr1234 start_requests: creating request to url (userIdPagePair.userId): {_url}"
            )
            req = scrapy.Request(
                url=_url,
                callback=self.parse,
                cb_kwargs={"user_page_pair": userIdPagePair})
            self.prior.put_user_id_page_pair(req, userIdPagePair.page)
        books_to_parse_copy = copy.copy(self.books_to_parse)
        for book_url in books_to_parse_copy:
            _url = f"{self.domain}{book_url}"
            logger.info(
                f"start_requests: yielding request to url (book_url): {_url}"
            )
            req = scrapy.Request(url=_url,
                                 callback=self.parse_book,
                                 cb_kwargs={"book_url": book_url})
            self.prior.put_book(req)
        readers_to_parse_copy = copy.copy(self.readers_to_parse)
        for reader_list_url in readers_to_parse_copy:
            _url = f"{self.domain}{reader_list_url}"
            logger.info(
                f"start_requests: yielding request to url (reader_list_url): {_url}"
            )
            req = scrapy.Request(
                url=_url,
                callback=self.parse_book_readers,
                cb_kwargs={"reader_list_url": reader_list_url})
            page = self.extract_page_from_href(_url)
            self.prior.put_readers_list(req, page)
        tags_to_parse_copy = copy.copy(self.tags_to_parse)
        for book_tag_pair in tags_to_parse_copy:
            _url = book_tag_pair.str2
            logger.info(
                f"start_requests: yielding request to url (book_tag_pair.str2): {_url}"
            )
            req = scrapy.Request(url=_url,
                                 callback=self.parse_book_tags,
                                 cb_kwargs={
                                     "book_url": book_tag_pair.str1,
                                     "all_tags_href": book_tag_pair.str2
                                 })
            self.prior.put_tag(req)
        selections_to_parse_copy = copy.copy(self.selections_to_parse)
        for book_selections_url in selections_to_parse_copy:
            _url = f"{self.domain}{book_selections_url.str2}"
            logger.info(
                f"start_requests: yielding request to url (book_selections_url.str2): {_url}"
            )
            req = scrapy.Request(url=_url,
                                 callback=self.parse_book_selections,
                                 cb_kwargs={
                                     "book_url":
                                     book_selections_url.str1,
                                     "book_selections_url":
                                     book_selections_url.str2
                                 })
            page = self.extract_page_from_href(_url)
            self.prior.put_selection(req, page)
        distributions_to_parse_copy = copy.copy(self.distributions_to_parse)
        for rating_distribution_quartet in distributions_to_parse_copy:
            _url = rating_distribution_quartet.str2
            logger.info(
                f"start_requests: yielding request to url (rating_distribution_quartet.str2): {_url}"
            )
            req = scrapy.FormRequest(
                url=_url,
                callback=self.extract_book_ratings,
                headers={"X-Requested-With": "XMLHttpRequest"},
                formdata={
                    "edition_id": rating_distribution_quartet.str3,
                    "is_new_design": rating_distribution_quartet.str4
                },
                cb_kwargs={
                    "book_url": rating_distribution_quartet.str1,
                    "rating_distribution_url":
                    rating_distribution_quartet.str2,
                    "edition_id": rating_distribution_quartet.str3,
                    "is_new_design": rating_distribution_quartet.str4
                })
            self.prior.put_distribution(req)
        while(self.prior.has_next()):
            next = self.prior.take_next()
            if(self.check_and_descrease_set_limit(next.__type__)):
                yield next.request


    def build_user_read_list_url(self, user_id_page_pair):
        return f"{self.domain}/reader/{user_id_page_pair.userId}/read/~{user_id_page_pair.page}"

    def build_book_reader_list_url(self, book_readers_list_url, page):
        url_without_page_num = book_readers_list_url[:book_readers_list_url.
                                                     find("~") + 1]
        return url_without_page_num + page

    def closed(self, reason):
        self.log_file.close()

    def parse(self, response, user_page_pair):
        logger.info(f"parse: user_page_pair: {user_page_pair}")
        userId = user_page_pair.userId
        page = user_page_pair.page
        userBookRates = [
            self.createUserBookRate(book_container, userId)
            for book_container in response.css('.brow-data')
        ]
        for userBookRate in userBookRates:
            yield userBookRate
        next_page_number = page + 1
        logger.info(f"parse: nextPageNumber: {next_page_number}")
        isNextPage = self.checkIfThereIsNextPageAfterCurrent(
            response.css(".pagination-page").xpath("@href").getall(), next_page_number)
        if (isNextPage):
            next_user_page_pair = UserIdPagePair(userId, next_page_number)
            if (self.push_user_id_page_pair_to_parse(next_user_page_pair)):
                url = self.build_user_read_list_url(next_user_page_pair)
                logger.info(f"parse: creating request to url: {url}")
                req = scrapy.Request(
                    url=url,
                    callback=self.parse,
                    cb_kwargs={"user_page_pair": next_user_page_pair})
                self.prior.put_user_id_page_pair(req, next_page_number)
        for userBookRate in userBookRates:
            if (self.push_book_to_parse(userBookRate['book_url'])):
                _url = f"{self.domain}{userBookRate['book_url']}"
                logger.info(f"parse: creating request to url: {_url}")
                req = scrapy.Request(
                    url=_url,
                    callback=self.parse_book,
                    cb_kwargs={"book_url": userBookRate["book_url"]})
                self.prior.put_book(req)
        self.pull_parsed_user_id_page_pair(user_page_pair)
        while(self.prior.has_next()):
            next = self.prior.take_next()
            if(self.check_and_descrease_set_limit(next.__type__)):
                yield next.request
                break

    def parse_book(self, response, book_url):
        logger.info(f"parse_book: book_url: {book_url}")
        title = response.css(".bc__book-title").css("::text").get()
        author = response.css(".bc-author__link").css("::text").get()
        stat_labels = response.css(".bc-stat").xpath("//b/text()").getall()
        avg_rating = float(
            response.xpath(
                "//span[@itemprop='ratingValue']/text()").get().replace(
                    ",", "."))
        readers_num = 0 if len(stat_labels) == 0 else int(
            stat_labels[0].replace("\xa0", ""))
        going_to_read_num = 0 if len(stat_labels) == 1 else int(
            stat_labels[1].replace("\xa0", ""))
        publishers = response.xpath(
            "//a[contains(@href, '/publisher/')]/text()").getall()
        year_element = response.xpath(
            "//p[contains(text(), 'Год издания')]/text()")
        year = None if len(year_element) == 0 else int(
            year_element.get().replace("Год издания: ", ""))
        edition_table = response.css(".bc-edition")
        series_href_elements = edition_table.xpath(
            "//a[contains(@href, '/series/')]/@href")
        pubseries_href_elements = edition_table.xpath(
            "//a[contains(@href, '/pubseries/')]/@href")
        series = [] if len(
            series_href_elements) == 0 else series_href_elements.getall()
        pubseries = [] if len(pubseries_href_elements
                              ) == 0 else pubseries_href_elements.getall()
        ratings_table = response.css(".bc-rating-medium__table")
        logger.info(
            f"parse_book: title: {title}, author: {author}, avg_rating: {avg_rating}, readers_num: {readers_num}, going_to_read_num: {going_to_read_num}, publishers: {publishers}, year: {year}, series: {series}, pubseries: {pubseries}"
        )
        book_item = Book(book_url=book_url,
                         title=title,
                         author=author,
                         avg_rating=avg_rating,
                         readers_num=readers_num,
                         going_to_read_num=going_to_read_num,
                         publishers=publishers,
                         year_published=year,
                         series=series,
                         pubseries=pubseries)
        yield book_item
        rating_distribution_url = f"{self.domain}/book/getratingchart"
        book_id = self.extract_book_id_from_url(book_url)
        rating_distribution_quartet = StringQuartet(book_url,
                                                    rating_distribution_url,
                                                    book_id, "ll2019")
        if (self.push_rating_distribution_url(rating_distribution_quartet)):
            logger.info(
                f"tr1234 parse_book: book_url: {book_url}, rating_distribution_url: {rating_distribution_url}, book_id: {book_id}"
            )
            req = scrapy.FormRequest(
                url=rating_distribution_url,
                callback=self.extract_book_ratings,
                headers={"X-Requested-With": "XMLHttpRequest"},
                formdata={
                    "edition_id": book_id,
                    "is_new_design": "ll2019"
                },
                cb_kwargs={
                    "book_url": book_url,
                    "rating_distribution_url": rating_distribution_url,
                    "edition_id": book_id,
                    "is_new_design": "ll2019"
                })
            self.prior.put_distribution(req)
        for genreHref in response.xpath(
                "//a[contains(@href, '/genre/')]").xpath("@href").getall():
            logger.info(f"tr1234 parse_book: genreHref: {genreHref}")
            if (self.isGenreHrefValid(genreHref)):
                yield Genre(book_url=book_url,
                            genre=self.extractGenreNameFromHref(genreHref))
        all_tags_href = response.css(".bc-tag__btn").xpath("@href").get()
        if (all_tags_href is not None):
            book_tag_urls_pair = StringPair(book_url, all_tags_href)
            if (self.push_tags_to_parse(book_tag_urls_pair)):
                logger.info(
                    f"parse_book: creating request to url: {all_tags_href}")
                req = scrapy.Request(url=all_tags_href,
                                     callback=self.parse_book_tags,
                                     cb_kwargs={
                                         "book_url": book_url,
                                         "all_tags_href": all_tags_href
                                     })
                self.prior.put_tag(req)
        book_selections_url = self.build_book_selections_url(book_url)
        if (self.push_selections_to_parse(
                StringPair(book_url, book_selections_url))):
            _url = f"{self.domain}{book_selections_url}"
            logger.info(f"parse_book: creating request to url: {_url}")
            req = scrapy.Request(url=_url,
                                 callback=self.parse_book_selections,
                                 cb_kwargs={
                                     "book_url": book_url,
                                     "book_selections_url": book_selections_url
                                 })
            self.prior.put_selection(req, 1)
        for reader_list_url in response.xpath(
                "//a[contains(@href, 'readers')]").xpath("@href").getall():
            logger.info(
                f"tr1234 parse_book: readersListUrl: {reader_list_url}")
            if (self.isReadersHrefValid(reader_list_url)
                    and self.push_reader_list_url(reader_list_url)):
                _url = f"{self.domain}{reader_list_url}"
                logger.info(f"parse_book: creating request to url: {_url}")
                req = scrapy.Request(
                    url=_url,
                    callback=self.parse_book_readers,
                    cb_kwargs={"reader_list_url": reader_list_url})
                self.prior.put_readers_list(req, 1)
        self.pull_parsed_book(book_url)
        while(self.prior.has_next()):
            next = self.prior.take_next()
            if(self.check_and_descrease_set_limit(next.__type__)):
                yield next.request
                break

    def parse_book_selections(self, response, book_url, book_selections_url):
        logger.info(
            f"parse_book_selections book_url: {book_url}, book_selections_url: {book_selections_url}"
        )
        card_titles = response.css(".lenta-card__title")
        for card_title in card_titles:
            selection_url = card_title.xpath(
                f"a[contains(@href, '/selection/')]").attrib["href"]
            yield Selection(book_url=book_url, selection=selection_url)
        page = self.extract_page_from_href(book_selections_url)
        next_page_number = page + 1
        is_next_page = self.checkIfThereIsNextPageAfterCurrent(
            response.css(".pagination-page").xpath("@href").getall(), next_page_number)
        if (is_next_page):
            next_selections_url = self.build_book_selections_url(
                book_url, next_page_number)
            next_book_selections_pair = StringPair(book_url,
                                                   next_selections_url)
            if (self.push_selections_to_parse(next_book_selections_pair)):
                _url = f"{self.domain}{next_selections_url}"
                logger.info(
                    f"parse_book_selections: creating request to url: {_url}")
                req = scrapy.Request(url=_url,
                                     callback=self.parse_book_selections,
                                     cb_kwargs={
                                         "book_url": book_url,
                                         "book_selections_url":
                                         next_selections_url
                                     })
                self.prior.put_selection(req, next_page_number)
        self.pull_parsed_selections(StringPair(book_url, book_selections_url))
        while(self.prior.has_next()):
            next = self.prior.take_next()
            if(self.check_and_descrease_set_limit(next.__type__)):
                yield next.request
                break

    def parse_book_readers(self, response, reader_list_url):
        for readerHref in response.css(".bc-reader-user__name").xpath(
                "@href").getall():
            logger.info(f"parse_book_readers: readerHref: {readerHref}")
            user_id = self.extractReaderIdFromHref(readerHref)
            logger.info(f"parse_book_readers: user_id: {user_id}")
            if (self.is_user_id_valid(user_id)):
                user_id_page_pair = UserIdPagePair(user_id)
                if (self.push_user_id_page_pair_to_parse(user_id_page_pair)):
                    user_readlist_url = self.build_user_read_list_url(
                        user_id_page_pair)
                    logger.info(
                        f"parse_book_readers: yielding request to url: {user_readlist_url}"
                    )
                    req = scrapy.Request(
                        url=user_readlist_url,
                        callback=self.parse,
                        cb_kwargs={"user_page_pair": user_id_page_pair})
                    self.prior.put_user_id_page_pair(req, 1)
        page = self.extract_page_from_href(reader_list_url)
        next_page_number = page + 1
        is_next_page = self.checkIfThereIsNextPageAfterCurrent(
            response.css(".pagination-page").xpath("@href").getall(), next_page_number)
        if (is_next_page):
            next_user_page_pair = self.build_book_reader_list_url(
                reader_list_url, next_page_number)
            if (self.push_reader_list_url(next_user_page_pair)):
                logger.info(
                    f"parse_book_readers: yielding request to url: {next_user_page_pair}"
                )
                req = scrapy.Request(
                    url=next_user_page_pair,
                    callback=self.parse,
                    cb_kwargs={"reader_list_url": next_user_page_pair})
                self.prior.put_readers_list(req, next_page_number)
        self.pull_reader_list_url(reader_list_url)
        while(self.prior.has_next()):
            next = self.prior.take_next()
            if(self.check_and_descrease_set_limit(next.__type__)):
                yield next.request
                break

    def extract_book_ratings(self, response, book_url, rating_distribution_url,
                             edition_id, is_new_design):
        logger.info(f"extract_book_ratings: book_url: {book_url}")
        json_response = json.loads(response.text)
        logger.info(
            f"tr1234 extract_book_ratings: json_response: {json_response}")
        html_parser = etree.HTMLParser()
        if ("content" in json_response):
            html_tree = etree.parse(StringIO(json_response["content"]),
                                    html_parser)
            tbody = html_tree.xpath("//tbody")
            tr_list = tbody[0].xpath("//tr")
            rates_distribution = []
            for i in range(len(tr_list) - 2):
                tr = tr_list[i]
                rate = int(tr.xpath("td[position()=1]/text()")[0])
                percentage = float(
                    tr.xpath("td[position()=3]/text()")[0].replace("%", ""))
                votes_number = int(tr.xpath("td[position()=4]/text()")[0])
                rates_distribution.append({
                    "rate": rate,
                    "percentage": percentage,
                    "votes_number": votes_number
                })
            rate_distribution = RateDistribution(
                book_url=book_url, rate_distribution=rates_distribution)
            self.pull_rating_distribution_url(
                StringQuartet(book_url, rating_distribution_url, edition_id,
                              is_new_design))
            yield rate_distribution
        while(self.prior.has_next()):
            next = self.prior.take_next()
            if(self.check_and_descrease_set_limit(next.__type__)):
                yield next.request
                break

    def parse_book_tags(self, response, book_url, all_tags_href):
        tag_hrefs = response.xpath(f"//a[contains(@href, '/tag/')]").xpath(
            "@href").getall()
        tags = list(
            map(lambda tag_href: self.extract_tag_from_href(tag_href),
                tag_hrefs))
        for tag in tags:
            yield Tag(book_url=book_url, tag=tag)
        self.pull_parsed_tags(StringPair(book_url, all_tags_href))
        while(self.prior.has_next()):
            next = self.prior.take_next()
            if(self.check_and_descrease_set_limit(next.__type__)):
                yield next.request
                break

    def createUserBookRate(self, book_container, userId):
        book_url = book_container.css('.brow-book-name').xpath("@href").get()
        bookRateGray = book_container.css(
            '.rating-value.stars-color-gray').css('::text').get()
        bookRateGreen = book_container.css(
            '.rating-value.stars-color-green').css('::text').get()
        bookRateRed = book_container.css(
            '.rating-value.stars-color-red').css('::text').get()
        return UserBookRate(userId=userId,
                            book_url=book_url,
                            rate=(bookRateGreen if bookRateGreen is not None
                                  else bookRateRed if bookRateRed is not None else bookRateGray))

    def extractMaxNextPageOrNone(self, hrefList):
        nonNonHrefs = filter(None, hrefList)
        hrefsWithTilda = filter(lambda item: item.find("~") > 0, nonNonHrefs)
        pageNums = list(
            map(lambda item: int(item[item.find("~") + 1:]), hrefsWithTilda))
        return None if (len(pageNums) == 0) else max(pageNums)

    def checkIfThereIsNextPageAfterCurrent(self, hrefList, next_page_number):
        nonNonHrefs = filter(None, hrefList)
        hrefsWithTilda = filter(lambda item: item.find("~") > 0, nonNonHrefs)
        pageNums = list(
            map(lambda item: int(item[item.find("~") + 1:]), hrefsWithTilda))
        return next_page_number in pageNums

    def build_book_selections_url(self, book_url, page_num=1):
        book_id = self.extract_book_id_from_url(book_url)
        book_url_prefix = self.extract_book_url_prefix(book_url)
        page_part = "" if page_num == 1 else f"/~{page_num}"
        return f"{book_url_prefix}{book_id}/selections{page_part}"

    def extract_book_id_from_url(self, book_url):
        book_url_prefix = self.extract_book_url_prefix(book_url)
        book_url_without_prefix = book_url.replace(book_url_prefix, "")
        return book_url_without_prefix[:book_url_without_prefix.find("-")]

    def extract_book_url_prefix(self, book_url):
        return "/book/" if book_url.find("/book/") > -1 else "/work/"

    def extract_tag_from_href(self, tag_href):
        return tag_href.replace("/tag/", "")

    def extractGenreNameFromHref(self, genreHref):
        return genreHref.replace("/genre/", "").replace("/top", "")

    def isGenreHrefValid(self, genreHref):
        return genreHref is not None and genreHref.find(
            "/genre") == 0 and genreHref.find("/top") > 0

    def isReadersHrefValid(self, readerHref):
        return readerHref is not None and readerHref.find(
            "/book/") == 0 and readerHref.find("/readers") > 0

    def extractReaderIdFromHref(self, readerHref):
        return readerHref.replace("/reader/", "")

    def extract_page_from_href(self, readerHref):
        logger.info(
            f"extract_page_from_href: readerHref: {readerHref}, readerHref[readerHref.find('~')+1:]: {readerHref[readerHref.find('~')+1:]}"
        )
        tilda_position = readerHref.find("~")
        return 1 if tilda_position < 0 else int(readerHref[tilda_position +
                                                           1:])

    def is_user_id_valid(self, user_id):
        return user_id != "" and user_id.find("/") == -1

    def push_user_id_page_pair_to_parse(self, pair):
        return self.push_obj_to_parse(pair, "user_page_pair")

    def push_book_to_parse(self, book):
        return self.push_obj_to_parse(book, "books")

    def push_reader_list_url(self, reader_list_url):
        return self.push_obj_to_parse(reader_list_url, "readers")

    def push_tags_to_parse(self, book_tags_pair):
        return self.push_obj_to_parse(book_tags_pair, "tags")

    def push_selections_to_parse(self, book_selections_pair):
        return self.push_obj_to_parse(book_selections_pair, "selections")

    def push_rating_distribution_url(self, rating_distribution_quartet):
        return self.push_obj_to_parse(rating_distribution_quartet,
                                      "distributions")

    def push_obj_to_parse(self, obj, set_name):
        sets = self.find_sets_by_name_in_log(set_name)
        if (obj in sets[0] or obj in sets[1]):
            return False
        line = json.dumps(LogEntry("push", set_name, obj),
                          cls=SpecialEncoder,
                          ensure_ascii=False) + "\n"
        logger.info(f"push_obj_to_parse: set_name: {set_name}, line: {line}")
        self.log_file.write(line)
        sets[0].add(obj)
        return True

    def pull_parsed_user_id_page_pair(self, pair):
        return self.pull_parsed_obj(pair, "user_page_pair")

    def pull_parsed_book(self, book_url):
        return self.pull_parsed_obj(book_url, "books")

    def pull_reader_list_url(self, reader_list_url):
        return self.pull_parsed_obj(reader_list_url, "readers")

    def pull_parsed_tags(self, book_tags_pair):
        return self.pull_parsed_obj(book_tags_pair, "tags")

    def pull_parsed_selections(self, book_selections_pair):
        return self.pull_parsed_obj(book_selections_pair, "selections")

    def pull_rating_distribution_url(self, rating_distribution_quartet):
        return self.pull_parsed_obj(rating_distribution_quartet,
                                    "distributions")

    def pull_parsed_obj(self, obj, set_name):
        sets = self.find_sets_by_name_in_log(set_name)
        line = json.dumps(LogEntry("pull", set_name, obj),
                          cls=SpecialEncoder,
                          ensure_ascii=False) + "\n"
        self.log_file.write(line)
        sets[0].discard(obj)
        sets[1].add(obj)
        logger.info(f"pull_parsed_obj: set_name: {set_name}, line: {line}")

    def check_and_descrease_set_limit(self, set_name):
        self.set_limits[set_name] -= 1
        logger.info(
            f"check_and_descrease_set_limit set_limits[{set_name}]: {self.set_limits[set_name]}"
        )
        return self.set_limits[set_name] > 0

    def print_limits(self):
        logger.info(f"set_limits: {self.set_limits}")

    def initialize(self):
        settings_file = open("run/run_settings.json")
        self.set_limits = json.loads(settings_file.readline())
        settings_file.close()
        pull_log_entries_start = time.perf_counter_ns()
        log_entries = self.pull_log_entries()
        pull_log_entries_stop = time.perf_counter_ns()
        logger.info(
            f"initialize: pull_log_entries took: {(pull_log_entries_stop - pull_log_entries_start) /  1_000_000_000} sec"
        )
        initialize_parse_tasks_start = time.perf_counter_ns()
        self.initialize_parse_tasks(log_entries)
        initialize_parse_tasks_stop = time.perf_counter_ns()
        logger.info(
            f"initialize: initialize_parse_tasks took: {(initialize_parse_tasks_stop - initialize_parse_tasks_start) /  1_000_000_000} sec"
        )
        # debug_line = (
        #     f"initialize: self.user_id_page_pairs_to_parse: {self.print_set(self.user_id_page_pairs_to_parse)}; self.user_id_page_parsed_pairs: {self.print_set(self.user_id_page_parsed_pairs)}\n"
        #     f"self.books_to_parse: {self.print_set(self.books_to_parse)}; self.books_parsed: {self.print_set(self.books_parsed)}"
        # )
        # logger.info(debug_line)
        return len(log_entries)

    def pull_log_entries(self):
        lines = self.log_file.readlines()
        return list(map(lambda line: json.loads(line), lines))

    def map_dict_to_log_entry(self, dict):
        return LogEntry(dict["op"], dict["type"], dict["data"])

    def initialize_parse_tasks(self, log_entries):
        parse_data_from_log_entry_avg = 0
        do_set_operation_by_name_avg = 0
        idx = 0
        for entry in log_entries:
            logger.debug(f"initialize_parse_tasks: entry: {entry}")
            sets = self.find_sets_by_name_in_log(entry["type"])
            parse_data_from_log_entry_start = time.perf_counter_ns()
            data_obj = self.parse_data_from_log_entry(entry["data"])
            parse_data_from_log_entry_start_stop = time.perf_counter_ns()
            parse_data_from_log_entry_avg = parse_data_from_log_entry_avg + (
                parse_data_from_log_entry_start_stop -
                parse_data_from_log_entry_start) / 1_000_000_000

            do_set_operation_by_name_start = time.perf_counter_ns()
            self.do_set_operation_by_name(sets[0], sets[1], entry["op"],
                                          data_obj)
            do_set_operation_by_name_stop = time.perf_counter_ns()
            do_set_operation_by_name_avg = do_set_operation_by_name_avg + (
                do_set_operation_by_name_stop -
                do_set_operation_by_name_start) / 1_000_000_000
            idx += 1
            if (idx % 500 == 0):
                _parse_data_from_log_entry_avg = parse_data_from_log_entry_avg / len(
                    log_entries)
                _do_set_operation_by_name_avg = do_set_operation_by_name_avg / len(
                    log_entries)
                logger.debug(
                    f"initialize_parse_tasks: inermediate parse_data_from_log_entry_avg took: {_parse_data_from_log_entry_avg} sec"
                )
                logger.debug(
                    f"initialize_parse_tasks: inermediate do_set_operation_by_name_avg took: {_do_set_operation_by_name_avg} sec"
                )
        parse_data_from_log_entry_avg = parse_data_from_log_entry_avg / len(
            log_entries)
        do_set_operation_by_name_avg = do_set_operation_by_name_avg / len(
            log_entries)
        logger.debug(
            f"initialize_parse_tasks: parse_data_from_log_entry_avg took: {parse_data_from_log_entry_avg} sec"
        )
        logger.debug(
            f"initialize_parse_tasks: do_set_operation_by_name_avg took: {do_set_operation_by_name_avg} sec"
        )

    def find_sets_by_name_in_log(self, set_name):
        if (set_name == "user_page_pair"):
            return (self.user_id_page_pairs_to_parse,
                    self.user_id_page_parsed_pairs)
        if (set_name == "books"):
            return (self.books_to_parse, self.books_parsed)
        if (set_name == "readers"):
            return (self.readers_to_parse, self.readers_parsed)
        if (set_name == "tags"):
            return (self.tags_to_parse, self.tags_parsed)
        if (set_name == "selections"):
            return (self.selections_to_parse, self.selections_parsed)
        if (set_name == "distributions"):
            return (self.distributions_to_parse, self.distributions_parsed)

    def parse_data_from_log_entry(self, data):
        if ("__type__" in data):
            type = data["__type__"]
            if (type == "UserIdPagePair"):
                return UserIdPagePair(data["userId"], data["page"])
            if (type == "StringPair"):
                return StringPair(data["str1"], data["str2"])
            if (type == "StringQuartet"):
                return StringQuartet(data["str1"], data["str2"], data["str3"],
                                     data["str4"])
        else:
            return data

    def do_set_operation_by_name(self, input_set, output_set, op, obj):
        if (logger.getEffectiveLevel() == logging.DEBUG):
            logger.debug(
                f"do_set_operation_by_name: op: {op}, obj: {obj}, input_set: {self.print_set(input_set)}, output_set: {self.print_set(output_set)}"
            )
        if (op == "push"):
            input_set.add(obj)
        elif (op == "pull"):
            input_set.discard(obj)
            output_set.add(obj)
        if (logger.getEffectiveLevel() == logging.DEBUG):
            logger.debug(
                f"do_set_operation_by_name: op: {op}, obj: {obj}, input_set: {self.print_set(input_set)}, output_set: {self.print_set(output_set)}"
            )

    def print_set(self, s):
        result = ""
        for item in s:
            start = "" if result == "" else f"{result}, "
            result = f"{start}{item}"
        result = "{" + result + "}"
        return result
