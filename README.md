Here are the pythpn scrips to solve the quest. 

1) bls_sync_lambda.py is a Python script designed to run as an AWS Lambda function. It fetches data from the open dataset at https://download.bls.gov/pub/time.series/pr/ and keeps the files in sync by performing daily updates and deletions. The Lambda function is triggered once per day using an Amazon EventBridge scheduler.

1.1) You can access the s3 bucket publically at https://bls-time-series-gp.s3.amazonaws.com/

you can download the files 
https://bls-time-series-gp.s3.amazonaws.com/pr/pr.class
https://bls-time-series-gp.s3.amazonaws.com/pr/pr.contacts
https://bls-time-series-gp.s3.amazonaws.com/pr/pr.data.0.Current
https://bls-time-series-gp.s3.amazonaws.com/pr/pr.data.1.AllData
https://bls-time-series-gp.s3.amazonaws.com/pr/pr.duration
https://bls-time-series-gp.s3.amazonaws.com/pr/pr.footnote
https://bls-time-series-gp.s3.amazonaws.com/pr/pr.measure
https://bls-time-series-gp.s3.amazonaws.com/pr/pr.period
https://bls-time-series-gp.s3.amazonaws.com/pr/pr.seasonal
https://bls-time-series-gp.s3.amazonaws.com/pr/pr.sector
https://bls-time-series-gp.s3.amazonaws.com/pr/pr.series
https://bls-time-series-gp.s3.amazonaws.com/pr/pr.txt

2) population_api.py is a Python script designed to run as an AWS Lambda function, it fetches the data from API and stores it in s3 bucket.

3) analytics.ipynb is a Jupyter notebook built using PySpark, where the BLS and population data are analyzed to extract meaningful insights.

