# Flood and Global Warming: Visualization and Prediction
### Dashboard App Link :  [Flood and Global Warming Visualization Dashboard](https://floods-detection.herokuapp.com/apps/floods)

## Team members:
Pengyu Wang & Kyoun Huh

## Introduction
As a common type of disaster, flood claimed lives and damaged properties throughout history. Notwithstanding the frequent occurrences of floods, our society is still underprepared while facing these disasters. To study the features and patterns of floods could help save lives and minimize economic losses.

With the power of big data technologies, this project aims to study the main floods all over the world since 1985. We present their locations and severities, examine the common causes of floods and how these causes changed through decades. Given the context of global warming, we also present how the temperatures have been changed in different areas of the world. Then, we attempt to link the temperature changes to flood occurrences. In addition to exploring the correlation between temperature and flood, a machine learning model is built to predict probabilities of floods based on historical temperature records. The final results of this project are presented by a visualization dashboard and a report.

## Project Outline
Data Acquisition – Data Wrangling + ETL – Machine Learning – Visualization

## Products
1. Interactive Visualization Dashboard
2. Report
3. Prediction model

## Data Sources
Flood Data: Global Active Archive of Large Flood Events, Dartmouth Flood Observatory, University of Colorado.
Temperature Data: Global Historical Climatology Network Daily (GHCN-D), NOAA (National Oceanic and Atmospheric Administration).


## Technologies
- Data Storage: AWS S3 + AWS RDS (PostgreSQL)
- ETL: PySpark on AWS EMR
- Machine Learning: Spark ML
- Visualization: Plotly Dash + Heroku
- Version control: GitHub

## Operating instruction
- Our project consists of AWS part for data processing and web application (Plotly Data with Heroku) part for visualization.

- In 'src' directory, all the spark codes we worked on AWS EMR cluster are stored.

  Code running sequence: 

  ghcn_etl.py -> page_one.py -> page_two_GHCN.py -> page_two_continent.py -> page_three.py ->  ml_model.py. 

  ghcn_etl.py should be run first since its output would be the input of other scripts.

- In 'floods-detection' directory, you can find all the dashboard related codes and requirements.

- Here are the simple description to operate our data processing part in AWS EMR.
1. Create an AWS S3 bucket for the storage named as 'climate-data-732' and upload 'FloodArchiveRaw.csv' and 'country_dict.json' into the bucket, these files can be found in the 'datasets' directory. Climate data could be visited from AWS S3 open data without being uploaded. Link: https://noaa-ghcn-pds.s3.amazonaws.com/index.html#csv/.
   
2. Create your EC2 key pair.

3. Create an AWS EMR cluster with configuring the applications Spark 3.0.1 and Zeppelin 0.9.0.
   - Hardware : Clusters with 1 master node with 2 core nodes with same specification of 4 vCore, 16 GiB memory(m5.xlarge).
     Using more cluster resources is encouraged to pursue better performance. 

   - Zeppelin: If you want to use Zeppelin notebooks, you need to do additional security configuration. 

     See: https://sbchapin.github.io/how-to-aws-emr-zeppelin/#/start

4. SSH to the master machine with your EC2 key pair.

5. Use pip to install the following Python packages:

   ```
   sudo pip pandas, boto3, psycopg2-binary, sqlalchemy, country-converter, pycountry-convert
   ```

   If you still cannot use PostgreSQL or get error messages regarding psycopg, you may need to:

   ```
   sudo apt-get install postgresql libpq-dev postgresql-client postgresql-client-common
   ```

6. SCP the codes to the master machine and use 'spark-submit' command to run the codes or use Zeppelin notebooks to run the codes on your web browser. 

   If you decide to use spark-submit on EMR, here is an instruction from AWS:  

   https://aws.amazon.com/blogs/big-data/submitting-user-applications-with-spark-submit/

   It is suggested to specify the parameters shown the examples in this article. Using 'spark-submit' on a small cluster without specifying these parameters may cause the program being halted by yarn because EMR's default values of these parameters may exceed the cluster's actual resources.

   



