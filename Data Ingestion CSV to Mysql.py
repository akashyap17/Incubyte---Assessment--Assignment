import mysql.connector as msql
from mysql.connector import Error
import csv


def create_db(db_name):
    try:
        conn = msql.connect(host='localhost', user='root',
                            password='akashyap')  # give ur username, password
        if conn.is_connected():
            cursor = conn.cursor()
            cursor.execute("CREATE DATABASE IF NOT EXISTS {}".format(db_name))
            print("Database is created")
            conn.close()
    except Error as e:
        print("Error while connecting to MySQL", e)


def create_table():
    try:
        conn = msql.connect(host='localhost', user='root',
                            password='akashyap', database='hospital')  # give ur username, password
        if conn.is_connected():
            cursor = conn.cursor()
            cursor.execute("""CREATE TABLE IF NOT EXISTS patients (Customer_name VARCHAR(255) NOT NULL, 
                                                                  Customer_id VARCHAR(25) NOT NULL PRIMARY KEY,
                                                                  Customer_open_date DATE NOT NULL,
                                                                  Last_consult_date DATE NOT NULL,
                                                                  vaccination_type VARCHAR(10) NOT NULL,
                                                                  Doctor_consulted VARCHAR(255) DEFAULT NULL,
                                                                  State VARCHAR(10) DEFAULT NULL,
                                                                  Country VARCHAR(10) NOT NULL,
                                                                  Date_of_birth DATE DEFAULT NULL,
                                                                  Active_customer VARCHAR(3) NOT NULL)""")
            print("Table patients created")
    except Error as e:
        print("Error while connecting to MySQL", e)


patients_file = 'Patients.csv'

create_db("hospital")
create_table()


def insert_data(data, cursor, conn):
    try:
        query_to_insert = """INSERT INTO patients{} VALUES{}""".format(tuple(data.keys()),
                                                                       tuple(len(data.keys()) * "%s ".split()))

        query_to_insert = query_to_insert.replace("'", "")

        values = tuple(data.values())
        cursor.execute(query_to_insert, values)
        conn.commit()
    except Exception as e:
        print("[ERROR] Error i inserting data {}".format(str(e)))
        pass


conn = msql.connect(host='localhost', user='root',
                    password='akashyap', database='hospital')  # give ur username, password
if conn.is_connected():
    cursor = conn.cursor()
    patients_data = list(csv.DictReader(open('Patients.csv')))
    for data in patients_data:
        insert_data(data, cursor, conn)
