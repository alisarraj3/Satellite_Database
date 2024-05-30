import requests
import psycopg2
import sqlite3


class Satellite:
    def __init__(self, data):
        self.OBJECT_NAME = data["OBJECT_NAME"]
        self.OBJECT_ID = data["OBJECT_ID"]
        self.EPOCH = data["EPOCH"]
        self.MEAN_MOTION = data["MEAN_MOTION"]
        self.ECCENTRICITY = data["ECCENTRICITY"]
        self.INCLINATION = data["INCLINATION"]
        self.RA_OF_ASC_NODE = data["RA_OF_ASC_NODE"]
        self.ARG_OF_PERICENTER = data["ARG_OF_PERICENTER"]
        self.MEAN_ANOMALY = data["MEAN_ANOMALY"]
        self.EPHEMERIS_TYPE = data["EPHEMERIS_TYPE"]
        self.CLASSIFICATION_TYPE = data["CLASSIFICATION_TYPE"]
        self.NORAD_CAT_ID = data["NORAD_CAT_ID"]
        self.ELEMENT_SET_NO = data["ELEMENT_SET_NO"]
        self.REV_AT_EPOCH = data["REV_AT_EPOCH"]
        self.BSTAR = data["BSTAR"]
        self.MEAN_MOTION_DOT = data["MEAN_MOTION_DOT"]
        self.MEAN_MOTION_DDOT = data["MEAN_MOTION_DDOT"]

conn = psycopg2.connect(host="localhost", dbname="test_satellite", user="postgres", password="1234", port="5432")
# conn = sqlite3.connect('satellite.db')
curr = conn.cursor()
curr.execute(""" CREATE TABLE IF NOT EXISTS satellites(
             object_id VARCHAR(255) PRIMARY KEY,
             object_name VARCHAR(255),
             epoch VARCHAR(255),
             mean_motion FLOAT,
             eccentricity FLOAT,
             inclination FLOAT,
             ra_of_asc_node FLOAT,
             arg_of_pericenter FLOAT,
             mean_anomaly FLOAT,
             ephemeris_type INT,
             classification_type CHAR, 
             norad_cat_id INT, 
             element_set_no INT, 
             rev_at_epoch INT, 
             bstar FLOAT,
             mean_motion_dot FLOAT, 
             mean_motion_ddot INT);   
""")


def parse_and_insert(url):
    response = requests.get(url)
    if response.status_code == 200:
        list_of_satellites = response.json()

        for satellite_data in list_of_satellites:
            insert_to_db(Satellite(satellite_data))

    else:
        print(response.status_code + "Error")


def insert_to_db(satellite: Satellite):
    curr.execute("BEGIN TRANSACTION;")
    sql_command = "INSERT INTO satellites (object_id, object_name, epoch, mean_motion, eccentricity, inclination, ra_of_asc_node, arg_of_pericenter, mean_anomaly, ephemeris_type, classification_type, norad_cat_id, element_set_no, rev_at_epoch, bstar, mean_motion_dot, mean_motion_ddot) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    data = (satellite.OBJECT_ID, satellite.OBJECT_NAME, satellite.EPOCH, satellite.MEAN_MOTION, satellite.ECCENTRICITY, satellite.INCLINATION, satellite.RA_OF_ASC_NODE, satellite.ARG_OF_PERICENTER, satellite.MEAN_ANOMALY, satellite.EPHEMERIS_TYPE, satellite.CLASSIFICATION_TYPE, satellite.NORAD_CAT_ID, satellite.ELEMENT_SET_NO, satellite.REV_AT_EPOCH, satellite.BSTAR, satellite.MEAN_MOTION_DOT, satellite.MEAN_MOTION_DDOT)
    
    try:
        curr.execute(sql_command, data)
        conn.commit()

    except psycopg2.errors.InFailedSqlTransaction: # Transaction error, revert changes
        conn.rollback()
        return

    except psycopg2.errors.UniqueViolation: # Duplicate key value violation
        conn.rollback()
        return
    
    except psycopg2.Error:
        conn.rollback()
        return


def collect_urls(file_name):
    cleaned_list = []
    file = open(file_name, "r") 
    read = file.readlines()
    for line in read: 
        if line[-1] == "\n":
            cleaned_list.append(line[:-1])
        else:
            cleaned_list.append(line)
    return cleaned_list


curr.close()
conn.close()
