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
Temperature Data: Earth Surface Temperature, Berkeley Earth.


## Technologies
- Data Storage: AWS S3 + AWS RDS (PostgreSQL)
- ETL: PySpark on AWS EMR
- Machine Learning: Spark ML
- Visualization: Plotly Dash + Heroku
- Version control: GitHub

## Operating instruction
- Our project consists of AWS part for data preocessing and web application (Plotly Data with Heroku) part for visualizaion.
- In 'src' directory, all the spark codes we worked on AWS EMR cluster are stored.
- In 'flood-detection' directory, you can find all the dashboard related codes and requirements.

Here are the simple description to operate our data processing part in AWS EMR.
1. Create an AWS S3 buckets for the storage named as'clitmate-data-732' and load the 'FloodArchiveRaw.csv' for our floods data.
   For average temperature datasets, we used S3 open data directly from https://noaa-ghcn-pds.s3.amazonaws.com/index.html#csv/.
2. Create an AWS EMR clsuter with configuring the applications Spark 3.0.1 and Zeppelin 0.9.0.
   - Hardware : Clusters with 1 master node with 2 core nodes with same specification of 4 vCore, 16 GiB memory(m5.xlarge).
3. After we create the cluster, several third-party pacakges below we deployed in our project need to be installed on the cluster.
   - pandas, boto3, psycopg2-binary, sqlalchemy, country-converter, pycountry-convert
4. Now you can access to the cluster through AWS CLI with your 'EC2 key pair'.
6. In the cluster console, we can use'spark-submit' command to run the source file stored in the S3 or we can also run it in the Zeppelin environment.

   
