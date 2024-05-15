# Spotify ETL Pipeline Project

### About

 Welcome to the repository for an ETL pipeline that extracts data from the Spotify API, processes it, and uploads it to AWS. This pipeline offers a complete solution for pulling and analyzing music data from Spotify.

### About Spotify API
The [Spotify API](https://developer.spotify.com/documentation/) is an online platform that grants developers access to Spotify's vast music database and functionalities. It enables developers to create applications that interact with Spotify’s music streaming service, allowing users to explore and play music, organize playlists, search for songs, and obtain metadata about artists, albums, and tracks.

For this project, we utilized the [**Top 50 - India**](https://open.spotify.com/playlist/37i9dQZEVXbLZ52XmnySJg) Playlist as our data source.

 ### Services Used
 
1. [**Amazon S3**](https://docs.aws.amazon.com/s3/index.html): Amazon S3, offered by Amazon Web Services, is a cloud storage service designed for the large-scale storage and retrieval of data like images, videos, and files, accessible from anywhere on the Internet.
2. [**CloudWatch**](https://docs.aws.amazon.com/cloudwatch/): Amazon CloudWatch is an AWS monitoring tool that tracks metrics, logs, and sets alarms to provide observability into application and infrastructure performance.
3. [**AWS Glue Crawler**](https://docs.aws.amazon.com/glue/latest/webapi/API_Crawler.html): An AWS service, the Glue Crawler automatically scans data repositories such as Amazon S3, RDS, and Redshift to deduce data schemas and detect schema changes, updating the Data Catalog as needed.
4. [**AWS Lambda**](https://docs.aws.amazon.com/lambda/index.html): AWS Lambda is a serverless computing platform by AWS that lets developers run code in response to events, scaling automatically without needing to manage servers.
5. [**AWS Athena**](https://docs.aws.amazon.com/athena/): Amazon Athena is an AWS service providing an interactive query system that simplifies analyzing data in Amazon S3 using SQL, eliminating the need for complex data infrastructure management.
6. [**Data Catalog**](https://docs.aws.amazon.com/glue/latest/dg/catalog-and-crawler.html): The Data Catalog, managed by AWS, serves as a centralized repository for metadata about data sources, tables, and transformations, accessible by AWS services like Athena and EMR for queries and management.

### Install Packages
```
pip install pandas
pip install boto3
pip install spotipy
pip install numpy
```

### Execution Process
1. Data is retrieved from the Spotify API.
2. A CloudWatch event triggers a Lambda function weekly.
3. This Lambda function pulls data and deposits it in the 'raw' directory on S3.
4. A second Lambda function processes the raw data and moves it to the 'transformed' directory in S3.
5. AWS Glue Crawler scans the data about albums, artists, and songs, updating the data catalog accordingly.
6. Amazon Athena queries and performs analyses on the data stored in the data catalog.The project extracts data through the Spotify API.
