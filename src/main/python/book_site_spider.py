import scrapy
import uuid
import json
from os import environ as env
from itemadapter import ItemAdapter

book_site_name  = env.get("BOOK_SITE_NAME")

class UserBookRate(scrapy.Item):
    __type__ = "UserBookRate"
    userId = scrapy.Field()
    bookUrl = scrapy.Field()
    rate = scrapy.Field()


class UserIdPagePair:
    def __init__(self, userId, page=1):
        self.userId = userId
        self.page = page

    def __hash__(self):
        return hash((self.userId, self.page))

    def __eq__(self, other):
        if not isinstance(other, type(self)):
            return NotImplemented
        return self.userId == other.userId and self.page == other.page


class Book (scrapy.Item):
    __type__ = "Book"
    bookUrl = scrapy.Field()
    title = scrapy.Field()
    author = scrapy.Field()


class Genre(scrapy.Item):
    __type__ = "Genre"
    bookUrl = scrapy.Field()
    genre = scrapy.Field()

class PrintItem:
    async def process_item(self, item, spider):
        print(f"tr1234 item: {item.items()}")
        return item

class WriteItemToJson:
    def open_spider(self, spider):
        self.userBookRates = open(f'{book_site_name}_data_user_book_rates.jl', 'w')
        self.bookGenres = open(f'{book_site_name}_data_book_genres.jl', 'w')
        self.books = open(f'{book_site_name}_data_books.jl', 'w')

    def close_spider(self, spider):
        self.userBookRates.close()
        self.bookGenres.close()
        self.books.close()

    async def process_item(self, item, spider):
        line = json.dumps(ItemAdapter(item).asdict()) + "\n"
        if (item.__type__ == "UserBookRate"):
            self.userBookRates.write(line)
        if (item.__type__ == "Genre"):
            self.bookGenres.write(line)
        if (item.__type__ == "Book"):
            self.books.write(line)
        return item

class {book_site_name}Spider(scrapy.Spider):
    custom_settings = {
        #"LOG_FILE": "scrapy.log",
        "ITEM_PIPELINES": {
            PrintItem: 100,
            WriteItemToJson: 200
        }
    }
    userIdPagePairsToParse = {UserIdPagePair("yanata777")}
    userIdPageParsedPairs = {}
    name = "{book_site_name}"
    domain = f"https://www.{book_site_name}.ru"
    start_urls = [f"{domain}/reader/yanata777/read"]
    parsed_books = {}

    def start_requests(self):
        for userIdPagePair in self.userIdPagePairsToParse:
            print(f"tr1234 start_requests: userIdPagePair.userId: {userIdPagePair.userId}")
            yield scrapy.Request(url=f"{self.domain}/reader/{userIdPagePair.userId}/read/~{userIdPagePair.page}",
                     callback = self.parse,
                     cb_kwargs = {"userId": userIdPagePair.userId}
                 )

    def parse(self, response, userId):
        userBookRates = [self.createUserBookRate(book_container, userId) for book_container in response.css('.brow-data')]
        for userBookRate in userBookRates:
            yield userBookRate
        for userBookRate in userBookRates:
            if (userBookRate["bookUrl"] not in self.parsed_books):
                yield scrapy.Request(
                    url=f"{self.domain}{userBookRate['bookUrl']}",
                    callback=self.parseBook,
                    cb_kwargs={"bookUrl": userBookRate["bookUrl"]}
                )
        #print(f"\nbookDataDict: {bookDataDict}")
        #print(f"\nadditionalBookTags: {additionalBookTags}")

    def createUserBookRate(self, book_container, userId):
        bookUrl = book_container.css('.brow-book-name').xpath("@href").get()
        bookRateGray = book_container.css('.rating-value.stars-color-gray').css('::text').get()
        bookRateGreen = book_container.css('.rating-value.stars-color-green').css('::text').get()
        return UserBookRate(userId = userId, bookUrl = bookUrl, rate = (bookRateGreen if bookRateGreen is not None else bookRateGray))

    # def extract_book_data(self, book_container):
    #     isMoreTags = len(book_container.css('.label-tag-more'))
    #     bookUrl = book_container.css('.brow-book-name').xpath("@href").get()
    #     return {
    #         #"title": book_container.css('.brow-book-name').css('::text').get(),
    #         #"author": book_container.css('.brow-book-author').css('::text').get(),
    #         "book_url": bookUrl,
    #         "rate": book_container.css('.rating-value .stars-color-green').css('::text').get(),
    #         #"genres": [genre.css('::text').get() for genre in book_container.css('.label-genre')],
    #         #"tags": [] if isMoreTags else [ tag.css('::text').get() for tag in book_container.css('.label-tag') ],
    #         #"additional_url": bookUrl if isMoreTags else None
    #     }

    def parseBook(self, response, bookUrl):
        title = response.css(".bc__book-title").css("::text").get()
        author = response.css(".bc-author__link").css("::text").get()
        yield Book(bookUrl = bookUrl, title = title.encode("utf-8"), author = author)

        for genreHref in response.xpath("//a[contains(@href, '/genre/')]").xpath("@href").getall():
            print(f"tr1234 genreHref: {genreHref}")
            if (self.isGenreHrefValid(genreHref)):
                yield Genre(bookUrl = bookUrl, genre = self.extractGenreNameFromHref(genreHref))

        for readersListUrl in response.xpath("//a[contains(@href, 'readers')]").xpath("@href").getall():
            print(f"tr1234 readersListUrl: {readersListUrl}")
            if (self.isReadersHrefValid(readersListUrl)):
                yield scrapy.Request(
                    url=f"{self.domain}{readersListUrl}",
                    callback=self.parseBookReaders
                )

    def parseBookReaders(self, response):
        for readerHref in response.xpath("//a[contains(@href, 'reader')]").xpath("@href").getall():
            userId = self.extractReaderIdFromHref(readerHref)
            userIdPagePair = UserIdPagePair(userId)
            if(userIdPagePair not in self.userIdPageParsedPairs):
                yield scrapy.Request(
                    url=readersListUrl,
                    callback=self.parse,
                    cb_kwargs={"userId": userId}
                )
        #TODO: yield user page 2, 3...

    def extractGenreNameFromHref(self, genreHref):
        return genreHref.replace("/genre/", "").replace("/top", "")

    def isGenreHrefValid(self, genreHref):
        return genreHref is not None and genreHref.find("/genre") == 0 and genreHref.find("/top") > 0

    def isReadersHrefValid(self, readerHref):
        return readerHref is not None and readerHref.find("/book/") == 0 and readerHref.find("/readers") > 0

    def extractReaderIdFromHref(self, readerHref):
        return readerHref.replace("/reader", "")

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
            book_uuid: [tag_a.css("::text").get() for tag_a in book_data_response.xpath(
                f"contains(@href, '{self.domain}/tag')")]
        }
