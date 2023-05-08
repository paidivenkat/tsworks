import pandas_datareader as pdr
import datetime
import yaml
import sqlite3
import pandas as pd


def create_table():
    conn = sqlite3.connect('finance_data.db')
    c = conn.cursor()
    # Create a table to store the finance data
    c.execute('''CREATE TABLE IF NOT EXISTS finance_data
                 (company text, date text, open real, high real, low real, close real, volume real, adj_close real,
                 PRIMARY KEY (company, date))''')
    conn.commit()
    conn.close()


def insert_data(company, data):
    conn = sqlite3.connect('finance_data.db')
    c = conn.cursor()
    # Insert or update the data for the given company
    for index, row in data.iterrows():
        date = index.strftime('%Y-%m-%d')
        values = (company, date, row['Open'], row['High'], row['Low'], row['Close'], row['Volume'], row['Adj Close'])
        c.execute('''INSERT OR REPLACE INTO finance_data
                     (company, date, open, high, low, close, volume, adj_close)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?)''', values)
    conn.commit()
    conn.close()


def download_data(companies):
    # Set the start and end date for the data
    start_date = datetime.datetime(2010, 1, 1)
    end_date = datetime.datetime(2023, 5, 2)

    for company in companies:
        try:
            # Download the data for the given company
            data = pdr.get_data_yahoo(company, start=start_date, end=end_date)
            # Insert or update the data in the finance_data table
            insert_data(company, data)
            print(f"Data for {company} inserted/updated in finance_data table")
        except:
            print(f"Failed to download data for {company}")


if name == 'main':
    # Load the list of companies from a config file
    with open("config.yml", "r") as f:
        config = yaml.load(f, Loader=yaml.FullLoader)
        companies = config["companies"]

    # Create the finance_data table if it doesn't exist
    create_table()

    # Download and insert/update the finance data for each company
    download_data(companies)