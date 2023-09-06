import pandas as pd
from io import StringIO
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

def main():
    for fund in funds:
        url = f'{base_url}{fund}.csv'
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            csv_content = response.content.decode('utf-8')
            df = pd.read_csv(StringIO(csv_content)) 
            df = clean_data(df)
            insert_data_into_db(df)
        else:
            print(f"Failed to download the CSV file. Status code: {response.status_code}")


if __name__ == '__main__':
    main()