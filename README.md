# ARK Trades - AWS Lambda Function
### Project Overview

---

In this project, the objetive is to develop a python script that:

1. Downloads daily trade data from [ARK Invest's (an ETF fund) website](https://ark-funds.com/download-fund-materials/) in CSV format
2. Cleans and formats the data
3. Inserts the data into a AWS RDS MySQL database

The deployment is done using a combination of AWS services as seen below.

![Screenshot 2023-09-27 at 09.55.11.png](https://prod-files-secure.s3.us-west-2.amazonaws.com/0c1ee3d4-94ea-4f01-acf1-edf81e9378f6/07aadfd0-8b9a-4019-be88-8441383baeca/Screenshot_2023-09-27_at_09.55.11.png)

The trade data is currently available via SQL query. 

### Future developments

---

- [ ]  Add FastAPI to offer data as through an open API
- [ ]  Add podcast transcriptions to database
- [ ]  Add research paper summaries
