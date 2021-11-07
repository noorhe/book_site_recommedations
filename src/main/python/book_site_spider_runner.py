from subprocess import Popen
from subprocess import run
import time
import sys
import logging
import time
import datetime

logger = logging.getLogger("BookSiteSpiderRunner")
logger.setLevel(logging.INFO)

streamHandler = logging.StreamHandler(sys.stdout)
streamHandler.setLevel(logging.INFO)
logger.addHandler(streamHandler)

limit = -1 if len(sys.argv) == 1 else int(sys.argv[1])
is_limited = limit >= 0
counter = 0
while((counter <= limit) if is_limited else True):
    # log_file = open('run/run.log', 'at')
    datetime_str = datetime.datetime.now().strftime("%d-%b-%Y_%H:%M")
    log_file_name = f"run/logs/run_#{counter}_{datetime_str}.log"
    fileHandler = logging.FileHandler(log_file_name)
    fileHandler.setLevel(logging.INFO)
    logger.addHandler(fileHandler)
    logger.info(f"run_#{counter}")
    start = time.perf_counter_ns()
    spider_run_command = f"scrapy runspider -a book_site_name=livelib --logfile={log_file_name} src/main/python/book_site_spider.py"
    logger.info(f"spider_run_command: {spider_run_command}")
    run([spider_run_command], shell = True)
    # log_file.close()
    stop = time.perf_counter_ns()
    logger.info(f"run_#{counter} took: {(stop - start) / 1_000_000_000} sec")
    time.sleep(180)
    counter = counter + 1
    logger.removeHandler(fileHandler)
