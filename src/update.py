import requests
import psycopg2
import sqlite3
from fetch import collect_urls, Satellite
import schedule
import time

conn = psycopg2.connect(host="localhost", dbname="test_satellite", user="postgres", password="1234", port="5432")
# conn = sqlite3.connect('satellite.db')
curr = conn.cursor()

def parse_and_update(url):
    response = requests.get(url)
    if response.status_code == 200:
        list_of_satellites = response.json()

        for satellite_data in list_of_satellites:
            update_db(Satellite(satellite_data))

    else:
        print(response.status_code + "Error")


def update_db(satellite: Satellite):
    # TODO
    curr.execute(""" """)





def tasks():
    list_of_urls = collect_urls("test_urls.txt")
    for url in list_of_urls:
        parse_and_update(url)
schedule.every.day.do(tasks)

while True:
    schedule.run_pending()
    time.sleep(1)

curr.close()
conn.close()