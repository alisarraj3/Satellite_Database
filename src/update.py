from fetch import collect_urls, parse_and_insert
import schedule
import time


def do_tasks():
    list_of_urls = collect_urls("test_urls.txt")
    for url in list_of_urls:
        parse_and_insert(url)


schedule.every(2).days.do(do_tasks)

while True:
    schedule.run_pending()
    time.sleep(1)

