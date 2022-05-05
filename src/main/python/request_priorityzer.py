import logging
import sys
from dataclasses import dataclass
from sortedcontainers import SortedList
from request_container import RequestContainer


logger = logging.getLogger("BookSiteSpider")
logger.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
logger.addHandler(handler)

@dataclass(frozen=True)
class SortedContainer:
    priority: int
    value: any

class Priorityzer:
    logger = logging.getLogger("Priorityzer")
    user_id_page_pairs = SortedList([], key = lambda value: value[0])
    books = []
    distributions = []
    tags = []
    selections = SortedList([], key = lambda value: value[0])
    readers = SortedList([], key = lambda value: value[0])

    def put_user_id_page_pair(self, user_id_page_request, page_num):
        logger.info(f"Priorityzer.put_user_id_page_pair page_num: {page_num}")
        self.user_id_page_pairs.add((page_num, RequestContainer(user_id_page_request, "user_id_page_pair")))

    def put_book(self, book_request):
        logger.info(f"Priorityzer.put_book")
        self.books.append(RequestContainer(book_request, "books"))

    def put_distribution(self, distribution_request):
        logger.info(f"Priorityzer.put_distribution")
        self.distributions.append(RequestContainer(distribution_request, "distributions"))

    def put_tag(self, tag_request):
        logger.info(f"Priorityzer.put_tag")
        self.tags.append(RequestContainer(tag_request, "tags"))

    def put_selection(self, selection_request, page_num):
        logger.info(f"Priorityzer.put_selection page_num: {page_num}")
        self.selections.add((page_num, RequestContainer(selection_request, "selections")))

    def put_readers_list(self, readers_list_request, page_num):
        logger.info(f"Priorityzer.put_readers_list page_num: {page_num}")
        self.readers.add((page_num, RequestContainer(readers_list_request, "readers")))

    def take_next(self):
        logger.info(f"Priorityzer.take_next: len(self.user_id_page_pairs): {len(self.user_id_page_pairs)}, len(self.books): {len(self.books)}, len(self.distributions): {len(self.distributions)}, len(self.tags): {len(self.tags)}, len(self.selections): {len(self.selections)}, len(self.readers): {len(self.readers)}")
        if (len(self.user_id_page_pairs) > 0):
            result = self.user_id_page_pairs.pop()
            logger.info(f"Priorityzer.take_next: user_id_page_pairs: result[0]: {result[0]}")
            return result[1]
        if (len(self.books) > 0):
            result = self.books.pop()
            logger.info(f"Priorityzer.take_next: books")
            return result
        if (len(self.distributions) > 0):
            result = self.distributions.pop()
            logger.info(f"Priorityzer.take_next: distributions")
            return result
        if (len(self.tags) > 0):
            result = self.tags.pop()
            logger.info(f"Priorityzer.take_next: tags")
            return result
        if (len(self.selections) > 0):
            result = self.selections.pop()
            logger.info(f"Priorityzer.take_next: selections: result[0]: {result[0]}")
            return result[1]
        if (len(self.readers) > 0):
            result = self.readers.pop()
            logger.info(f"Priorityzer.take_next: readers: result[0]: {result[0]}")
            return result[1]

    def has_next(self):
        return len(self.user_id_page_pairs) > 0 or len(self.selections) > 0 or len(self.books) > 0 or len(self.distributions) > 0 or len(self.tags) > 0 or len(self.readers) > 0
