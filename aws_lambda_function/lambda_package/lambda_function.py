import csv
import requests

from functions import clean_data, insert_data_into_db

# ARK Invest API base url
base_url = 'https://ark-funds.com/wp-content/uploads/funds-etf-csv/'

# ARK Invest ETF names
funds = ['ARK_INNOVATION_ETF_ARKK_HOLDINGS',
         'ARK_NEXT_GENERATION_INTERNET_ETF_ARKW_HOLDINGS',
         'ARK_FINTECH_INNOVATION_ETF_ARKF_HOLDINGS',
         'THE_3D_PRINTING_ETF_PRNT_HOLDINGS',
         'ARK_GENOMIC_REVOLUTION_ETF_ARKG_HOLDINGS',
         'ARK_AUTONOMOUS_TECH._&_ROBOTICS_ETF_ARKQ_HOLDINGS',
         'ARK_SPACE_EXPLORATION_&_INNOVATION_ETF_ARKX_HOLDINGS',
         'ARK_ISRAEL_INNOVATIVE_TECHNOLOGY_ETF_IZRL_HOLDINGS']

# Request headers
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}

def read_csv_from_url(url):
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        csv_content = response.content.decode('utf-8')
        return csv_content.splitlines()
    else:
        print(f"Failed to download the CSV file. Status code: {response.status_code}")
        return []

def lambda_handler(event, context):
    for fund in funds:
        url = f'{base_url}{fund}.csv'
        csv_lines = read_csv_from_url(url)
        
        if csv_lines:
            csv_reader = csv.reader(csv_lines)
            headers = next(csv_reader)  # Assuming the first row is the header
            data_rows = list(csv_reader)
            
            cleaned_data = clean_data(data_rows)
            insert_data_into_db(cleaned_data)
        else:
            print(f"Skipping fund: {fund}")

lambda_handler(None, None)
