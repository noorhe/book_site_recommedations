import scrapy
import uuid
import json
from os import environ as env
from itemadapter import ItemAdapter

book_site_name = env.get("BOOK_SITE_NAME")


class UserBookRate(scrapy.Item):
    __type__ = "UserBookRate"
    userId = scrapy.Field()
    bookUrl = scrapy.Field()
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


class Book (scrapy.Item):
    __type__ = "Book"
    bookUrl = scrapy.Field()
    title = scrapy.Field()
    author = scrapy.Field()


class Genre(scrapy.Item):
    __type__ = "Genre"
    bookUrl = scrapy.Field()
    genre = scrapy.Field()
    
class Tag(scrapy.Item):
    __type__ = "Tag"
    bookUrl = scrapy.Field()
    tag = scrapy.Field()

class PrintItem:
    async def process_item(self, item, spider):
        print(f"tr1234 __type__: {item.__type__} item: {item.items()}")
        return item


class WriteItemToJson:
    def open_spider(self, spider):
        self.userBookRates = open(f'run/user_book_rates.json', 'w')
        self.bookGenres = open(f'run/genres.json', 'w')
        self.books = open(f'run/books.json', 'w')
        self.tags = open(f'run/tags.json', 'w')

    def close_spider(self, spider):
        self.userBookRates.close()
        self.bookGenres.close()
        self.books.close()
        self.tags.close()

    async def process_item(self, item, spider):
        line = json.dumps(ItemAdapter(item).asdict(),
                          ensure_ascii=False) + "\n"
        print(f"tr1234 __type__: {item.__type__} line: {line}")
        if (item.__type__ == "UserBookRate"):
            self.userBookRates.write(line)
        if (item.__type__ == "Genre"):
            self.bookGenres.write(line)
        if (item.__type__ == "Book"):
            self.books.write(line)
        if (item.__type__ == "Tag"):
            self.tags.write(line)
        return item


class LogEntry:
    def __init__(self, op, type, data):
        self.op = op
        self.type = type
        self.data = data
        
class SpecialEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, LogEntry) or isinstance(obj, UserIdPagePair):
            return obj.__dict__
        return json.JSONEncoder.default(self, obj)


class BookSiteSpider(scrapy.Spider):
    custom_settings = {
        #"LOG_FILE": "scrapy.log",
        "ITEM_PIPELINES": {
            PrintItem: 100,
            WriteItemToJson: 200
        },
        "DOWNLOAD_DELAY": 3
    }
    user_id_page_pairs_to_parse = set()
    user_id_page_parsed_pairs = set()
    books_to_parse = set()
    books_parsed = set()
    tags_to_parse = set()
    tags_parsed = set()
    readers_to_parse = set()
    readers_parsed = set()
    
    name = "{book_site_name}"
    domain = f"https://www.{book_site_name}.ru"
    start_urls = [f"{domain}/reader/yanata777/read"]
    parsed_books = {}
    set_limits = {"user_id_page_pair": 3,
            "book": 3,
            "readers": 3,
            "tags": 3
        }

    def start_requests(self):
        self.log_file = open(f'run/log.json')
        self.initialize()
        self.log_file.close()
        self.log_file = open(f'run/log.json', 'at')
        for userIdPagePair in self.user_id_page_pairs_to_parse:
            if (self.check_and_descrease_set_limit("user_id_page_pair")):
                print(f"tr1234 start_requests: userIdPagePair.userId: {userIdPagePair.userId}")
                yield scrapy.Request(url = self.build_user_read_list_url(userIdPagePair),
                    callback = self.parse,
                    cb_kwargs = {
                            "user_page_pair": userIdPagePair
                         }
                    )
        for book_url in self.books_to_parse:
            if (self.check_and_descrease_set_limit("book")):
                print(f"tr1234 start_requests: book_url: {book_url}")
                yield scrapy.Request(url=f"{self.domain}{book_url}",
                    callback=self.parse_book,
                    cb_kwargs={"bookUrl": book_url}
                )
        for reader_list_url in self.readers_to_parse:
            if (self.check_and_descrease_set_limit("readers")):
                yield scrapy.Request(url = f"{self.domain}{reader_list_url}",
                    callback = self.parse,
                    cb_kwargs = {
                            "reader_list_url": reader_list_url
                         }
                    )
        for tags_url in self.tags_to_parse:
            if (self.check_and_descrease_set_limit("readers")):
                yield scrapy.Request(url = tags_url,
                    callback = self.parse,
                    cb_kwargs = {
                            "reader_list_url": reader_list_url
                         }
                    )
        
                
    def build_user_read_list_url(self, user_id_page_pair):
        return f"{self.domain}/reader/{user_id_page_pair.userId}/read/~{user_id_page_pair.page}"
        
    def build_book_reader_list_url(self, book_readers_list_url, page):
        url_without_page_num = url[:book_readers_list_url.find("~")+1]
        return url_without_page_num + page
                
    def closed(self, reason):
        self.log_file.close()

    def parse(self, response, user_page_pair):
        userId = user_page_pair.userId
        page = user_page_pair.page
        userBookRates = [self.createUserBookRate(book_container, userId) for book_container in response.css('.brow-data')]
        for userBookRate in userBookRates:
            yield userBookRate
        for userBookRate in userBookRates:
            if (self.push_book_to_parse(userBookRate['bookUrl']) and self.check_and_descrease_set_limit("book")):
                yield scrapy.Request(
                    url=f"{self.domain}{userBookRate['bookUrl']}",
                    callback=self.parse_book,
                    cb_kwargs={"bookUrl": userBookRate["bookUrl"]}
                )
        nextPageNumber = self.extractMaxNextPageOrNone(
            response.css(".pagination-page").xpath("@href").getall())
        if (nextPageNumber == page + 1):
            next_user_page_pair = UserIdPagePair(userId, nextPageNumber)
            if(self.push_user_id_page_pair_to_parse(next_user_page_pair) and self.check_and_descrease_set_limit("user_id_page_pair")):
                yield scrapy.Request(url = self.build_user_read_list_url(next_user_page_pair),
                    callback = self.parse,
                    cb_kwargs = {
                            "user_page_pair": next_user_page_pair
                         }
                    )
        self.pull_parsed_user_id_page_pair(user_page_pair)
        
    def parse_book(self, response, bookUrl):
        title = response.css(".bc__book-title").css("::text").get()
        author = response.css(".bc-author__link").css("::text").get()
        yield Book(bookUrl=bookUrl, title=title, author=author)

        for genreHref in response.xpath("//a[contains(@href, '/genre/')]").xpath("@href").getall():
            print(f"tr1234 genreHref: {genreHref}")
            if (self.isGenreHrefValid(genreHref)):
                yield Genre(bookUrl=bookUrl, genre=self.extractGenreNameFromHref(genreHref))

        for reader_list_url in response.xpath("//a[contains(@href, 'readers')]").xpath("@href").getall():
            print(f"tr1234 readersListUrl: {reader_list_url}")
            if (self.isReadersHrefValid(reader_list_url) and self.push_reader_list_url(reader_list_url) and self.check_and_descrease_set_limit("readers")):
                yield scrapy.Request(
                    url=f"{self.domain}{reader_list_url}",
                    callback=self.parse_book_readers,
                    cb_kwargs = {
                        "reader_list_url": reader_list_url
                    }
                )
        all_tags_href = response.css(".bc-tag__btn").xpath("@href").get()
        if(self.push_tags_to_parse(all_tags_href) and self.check_and_descrease_set_limit("tags")):
            yield scrapy.Request(
                url = all_tags_href,
                callback = self.parse_book_tags,
                cb_kwargs = {
                    "book_url": bookUrl,
                    "all_tags_href": all_tags_href
                }
            )
        self.pull_parsed_book(bookUrl)
        
    def parse_book_readers(self, response, reader_list_url):
        for readerHref in response.css(".bc-reader-user__name").xpath("@href").getall():
            print(f"parse_book_readers: readerHref: {readerHref}")
            user_id = self.extractReaderIdFromHref(readerHref)
            print(f"parse_book_readers: user_id: {user_id}")
            if (self.is_user_id_valid(user_id)):
                user_id_page_pair = UserIdPagePair(user_id)
                if(self.push_user_id_page_pair_to_parse(user_id_page_pair) and self.check_and_descrease_set_limit("user_id_page_pair")):
                    yield scrapy.Request(
                        url = self.build_user_read_list_url(user_id_page_pair),
                        callback=self.parse,
                        cb_kwargs={"user_page_pair": user_id_page_pair}
                    )
        page = self.extract_page_from_href(reader_list_url)
        nextPageNumber = self.extractMaxNextPageOrNone(response.css(".pagination-page").xpath("@href").getall())
        if (nextPageNumber == page + 1):
            url = self.build_book_reader_list_url(reader_list_url, nextPageNumber)
            if(self.push_reader_list_url(next_user_page_pair) and self.check_and_descrease_set_limit("readers")):
                yield scrapy.Request(url = url,
                    callback = self.parse,
                    cb_kwargs = {
                            "reader_list_url": url
                         }
                    )
        self.pull_reader_list_url(reader_list_url)
        
    def parse_book_tags(self, response, book_url, all_tags_href):
        tag_hrefs = response.xpath(f"//a[contains(@href, '/tag/')]").xpath("@href").getall()
        tags = list(map(lambda tag_href: self.extract_tag_from_href(tag_href), tag_hrefs))
        for tag in tags:
            yield Tag(bookUrl = book_url, tag =  tag)
        self.pull_parsed_tags(all_tags_href)

    def createUserBookRate(self, book_container, userId):
        bookUrl = book_container.css('.brow-book-name').xpath("@href").get()
        bookRateGray = book_container.css(
            '.rating-value.stars-color-gray').css('::text').get()
        bookRateGreen = book_container.css(
            '.rating-value.stars-color-green').css('::text').get()
        return UserBookRate(userId=userId, bookUrl=bookUrl, rate=(bookRateGreen if bookRateGreen is not None else bookRateGray))

    def extractMaxNextPageOrNone(self, hrefList):
        nonNonHrefs = filter(None, hrefList)
        hrefsWithTilda = filter(lambda item: item.find("~") > 0, nonNonHrefs)
        pageNums = list(map(lambda item: int(item[item.find("~") + 1:]), hrefsWithTilda))
        return None if (len(pageNums) == 0) else max(pageNums)
        
    def extract_tag_from_href(self, tag_href):
        return tag_href.replace("/tag/", "")

    def extractGenreNameFromHref(self, genreHref):
        return genreHref.replace("/genre/", "").replace("/top", "")

    def isGenreHrefValid(self, genreHref):
        return genreHref is not None and genreHref.find("/genre") == 0 and genreHref.find("/top") > 0

    def isReadersHrefValid(self, readerHref):
        return readerHref is not None and readerHref.find("/book/") == 0 and readerHref.find("/readers") > 0

    def extractReaderIdFromHref(self, readerHref):
        return readerHref.replace("/reader/", "")
        
    def extract_page_from_href(self, readerHref):
        print(f"extract_page_from_href: readerHref: {readerHref}, readerHref[readerHref.find('~')+1:]: {readerHref[readerHref.find('~')+1:]}")
        tilda_position = readerHref.find("~")
        return 1 if tilda_position < 0 else int(readerHref[tilda_position + 1:])
        
    def is_user_id_valid(self, user_id):
        return user_id != "" and user_id.find("/") == -1

    def loadAdditionalBooksTags(self, bookDataList):
        for bookId in bookDataList:
            print(bookId)
            bookData = bookDataList[bookId]
            print(bookData)

            def uuid_callback(_id):
                print(f"in uuid_callback with uuid: {_id}")

                def response_callback(response):
                    print(f"in response_callback with uuid: {_id}")
                    return self.self.extract_book_tags(_id, response)
                return response_callback

            if(bookData["additional_url"] is not None):
                print("before additional_url request")
                yield scrapy.Request(f"{self.domain}/{bookData['additional_url']}", lambda response: print(f"tr1234 response: {response}"))
                #scrapy.Request(f"{self.domain}/{bookData['additional_url']}", (lambda _id: uuid_callback(_id))(bookId))
                #scrapy.Request(f"{self.domain}/{bookData['additional_url']}", (lambda uuid: lambda response: self.extract_book_tags(uuid, response))(bookId))

    def extract_book_tags(self, bookd_uuid, book_data_response):
        print("extract_book_tags")
        return {
            book_uuid: [tag_a.css("::text").get() for tag_a in book_data_response.xpath(f"contains(@href, '{self.domain}/tag')")]
        }

        
    def push_user_id_page_pair_to_parse(self, pair):
        return self.push_obj_to_parse(pair, "user_page_pair")
        
    def push_book_to_parse(self, book):
        return self.push_obj_to_parse(book, "book")
        
    def push_reader_list_url(self, reader_list_url):
        return self.push_obj_to_parse(reader_list_url, "readers")
        
    def push_tags_to_parse(self, tags_url):
        return self.push_obj_to_parse(tags_url, "tags")
        
        
    def push_obj_to_parse(self, obj, set_name):
        sets = self.find_sets_by_name_in_log(set_name)
        if (obj in sets[0] or obj in sets[1]):
            return False
        line = json.dumps(LogEntry("push", set_name, obj), cls = SpecialEncoder, ensure_ascii=False) + "\n"
        self.log_file.write(line)
        sets[0].add(obj)
        return True
        
    def pull_parsed_user_id_page_pair(self, pair):
        return self.pull_parsed_obj(pair, "user_page_pair")
        
    def pull_parsed_book(self, bookUrl):
        return self.pull_parsed_obj(bookUrl, "book")
        
    def pull_reader_list_url(self, reader_list_url):
        return self.pull_parsed_obj(reader_list_url, "readers")
        
    def pull_parsed_tags(self, all_tags_ref):
        return self.pull_parsed_obj(all_tags_ref, "tags")
        
    def pull_parsed_obj(self, obj, set_name):
        sets = self.find_sets_by_name_in_log(set_name)
        line = json.dumps(LogEntry("pull", set_name, obj), cls = SpecialEncoder, ensure_ascii=False) + "\n"
        self.log_file.write(line)
        sets[0].discard(obj)
        sets[1].add(obj)
        
    def check_and_descrease_set_limit(self, set_name):
        self.set_limits[set_name] -= 1
        print(f"set_limits[{set_name}]: {self.set_limits[set_name]}")
        return self.set_limits[set_name] > 0
        
    def initialize(self):
        log_entries = self.pull_log_entries()
        self.initialize_parse_tasks(log_entries)
        debug_line = (
            f"initialize: self.user_id_page_pairs_to_parse: {self.print_set(self.user_id_page_pairs_to_parse)}; self.user_id_page_parsed_pairs: {self.print_set(self.user_id_page_parsed_pairs)}\n"
            f"self.books_to_parse: {self.print_set(self.books_to_parse)}; self.books_parsed: {self.print_set(self.books_parsed)}"
        )
        print(debug_line)    
    
    def pull_log_entries(self):
        lines = self.log_file.readlines()
        return list(map(lambda line: json.loads(line), lines))
        
    def map_dict_to_log_entry(self, dict):
        return LogEntry(dict["op"], dict["type"], dict["data"])

    def initialize_parse_tasks(self, log_entries):
        for entry in log_entries:
            sets = self.find_sets_by_name_in_log(entry["type"])
            data_obj = self.parse_data_from_log_entry(entry["data"])
            self.do_set_operation_by_name(sets[0], sets[1], entry["op"], data_obj)
            
    def find_sets_by_name_in_log(self, set_name):
        if (set_name == "user_page_pair"):
            return (self.user_id_page_pairs_to_parse, self.user_id_page_parsed_pairs)
        if (set_name == "book"):
            return (self.books_to_parse, self.books_parsed)
        if (set_name == "readers"):
            return (self.readers_to_parse, self.readers_parsed)
        if (set_name == "tags"):
            return (self.tags_to_parse, self.tags_parsed)
    
    def parse_data_from_log_entry(self, data):
        if ("__type__" in data):
            type = data["__type__"]
            if (type == "UserIdPagePair"):
                return UserIdPagePair(data["userId"], data["page"])
        else:
            return data
            
    def do_set_operation_by_name(self, input_set, output_set, op, obj):
        print(f"do_set_operation_by_name: op: {op}, obj: {obj}, obj in input_set: {obj in input_set}, obj in output_set: {obj in output_set}")
        print(f"do_set_operation_by_name: op: {op}, obj: {obj}, input_set: {self.print_set(input_set)}, output_set: {self.print_set(output_set)}")
        if (op == "push"):
            print("do_set_operation_by_name: push")
            input_set.add(obj) 
        elif (op == "pull"):
            print("do_set_operation_by_name: pull")
            input_set.discard(obj)
            output_set.add(obj)
        print(f"do_set_operation_by_name: op: {op}, obj: {obj}, input_set: {self.print_set(input_set)}, output_set: {self.print_set(output_set)}")

    def print_set(self, s):
        result = ""
        for item in s:
            start = "" if result == "" else f"{result}, "
            result = f"{start}{item}"
        result = "{" + result + "}"
        return result
