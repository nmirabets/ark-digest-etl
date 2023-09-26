import mysql.connector
from mysql.connector import Error
import os
from datetime import datetime

from dotenv import load_dotenv
load_dotenv()

def insert_data_into_db(data_list):
    try:
        db_config = {
            "host": "ark-digest.crubyqmjsrku.eu-west-3.rds.amazonaws.com",
            "user": os.getenv('DB_USER'),
            "password": os.getenv('DB_PASS'),
            #"user": os.environ.get('DB_USER'), 
            #"password": os.environ.get('DB_PASS'), 
            "database": "ark_digest",
        }

        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        table_name = 'holdings'

        fund_name = ''
        data_date = ''

        for row in data_list:
            try:
                insert_query = f"""
                    INSERT INTO {table_name} (date, fund, company, ticker, cusip, shares, market_value, weight)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """

                data_date = row[0].strftime("%d/%m/%Y")

                fund_name = row[1]

                cursor.execute(insert_query, row)
                connection.commit()

            except Error as e:
                print(f"Error inserting row for {fund_name} - {data_date}: {e}")
                connection.rollback()  # Rollback the transaction for the current row

        print(f"{fund_name} - {data_date} - data insertion completed")

        if connection.is_connected():
            cursor.close()
            connection.close()

    except Error as e:
        print("Error connecting to the database:", e)

def clean_data(data_list):
    try:
        cleaned_data = []
        for row in data_list[:-1]:  # Drop the last row
            date = datetime.strptime(row[0], "%m/%d/%Y")
            company = row[2]
            ticker = row[3]
            cusip = row[4]
            shares = int(row[5].replace(',', ''))
            market_value = float(row[6].replace('$', '').replace(',', ''))
            weight = float(row[7].replace('%', '')) / 100

            cleaned_data.append((date, row[1], company, ticker, cusip, shares, market_value, weight))

        return cleaned_data

    except Error as e:
        print(f"Error cleaning data: {e}")
