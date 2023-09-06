import pandas as pd
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
import os

load_dotenv()

def insert_data_into_db(data_frame):
    try:
        db_config = {
            "host": "ark-digest.crubyqmjsrku.eu-west-3.rds.amazonaws.com",
            "user": os.getenv('DB_USER'),
            "password": os.getenv('DB_PASS'),
            "database": "ark_digest",
        }

        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        table_name = 'holdings'

        fund_name = ''
        data_date = ''

        for index, row in data_frame.iterrows():
            try:
                insert_query = f"""
                    INSERT INTO {table_name} (date, fund, company, ticker, cusip, shares, market_value, weight)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """

                data_tuple = (
                    row['date'], row['fund'], row['company'], row['ticker'],
                    row['cusip'], row['shares'], row['market_value'], row['weight']
                )

                data_date = row['date'].strftime("%d/%m/%Y")

                fund_name = row['fund']

                cursor.execute(insert_query, data_tuple)
                connection.commit()

            except Error as e:
                print(f"Error inserting row {index} for {fund_name} - {data_date}: {e}")
                connection.rollback()  # Rollback the transaction for the current row

        print(f"{fund_name} - {data_date} - data insertion completed")

    except Error as e:
        print("Error connecting to the database:", e)

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    try:
        df1 = df.copy()
        df1.drop(df.index[-1], inplace=True) # drop last row
        df1.fillna(0, inplace=True) # fill NaN with 0
        df1.rename(columns={'market value ($)': 'market_value', 'weight (%)': 'weight'}, inplace=True) # rename columns
        df1['date'] = pd.to_datetime(df1['date'], format="%m/%d/%Y") # Convert the "date" column to datetime
        df1['market_value'] = df1['market_value'].str.replace(r'[^\d.]', '', regex=True).astype(float) # remove $ and , and convert to float
        df1['shares'] = df1['shares'].str.replace(r'[^\d.]', '', regex=True).astype(int) # remove , and convert to float
        df1['weight'] = df1['weight'].str.replace(r'[^\d.]', '', regex=True).astype(float) / 100 # remove % and convert to unitary float

        return df1
    
    except Error as e:
        print(f"Error cleaning data: {e}")
    