from subprocess import Popen
from subprocess import run
import time
import sys

limit = -1 if len(sys.argv) == 1 else int(sys.argv[1])
is_limited = limit >= 0
while((limit > 0) if is_limited else True):
    # log_file = open('run/run.log', 'at')
    run(["scrapy runspider src/main/python/book_site_spider.py -a book_site_name=livelib"], shell = True)
    # log_file.close()
    time.sleep(180)
    limit = limit - 1
