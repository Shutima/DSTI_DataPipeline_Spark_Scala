# DSTI Data Pipeline 2 Project, source from Professor Jean-Luc Canela

![Scala CI](https://github.com/jlcanela/dsti-samples/workflows/Scala%20CI/badge.svg)

[Professor Scala Tutorial](https://github.com/jlcanela/spark-hands-on/wiki/Tutorial-Scala-2)

# My Project GitHub: https://github.com/Shutima/DSTI_DataPipeline_Spark_Scala

## 1. Project Overview
This assignment is aimed to use Spark Scala language to query the important information from the data set file and represent the report in json format. It also aims to understand how to write a unit test, how to delivered the project with sbt or maven configuration, and how to package the project automatically the jar file to be run using spark-submit. The project also includes the integration with AWS Glue and AWS S3 services as ETL tool.

## 2. Deliverables
For this assignment, I use the source code template from Professor's repository from his [GitHub](https://github.com/jlcanela/dsti-samples).

- This documentation description of the work done for the project (this document)
- My part of the scala functions are added in my scala project called **CreateJSONreport** under the directory **src/main/scala/CreateJSONreport.scala**
- The dataset file (Web Server log file) from Prof. Jean-Luc's [spark-hands-on wiki](https://github.com/jlcanela/spark-hands-on/wiki/Tutorial-Scala-2). In the code **CreateJSONreport.scala**, the location of dataset file is hardcoded for **val path** and need to be modified for the correct location:

![DatasetPath](https://user-images.githubusercontent.com/57285863/89306456-314c8100-d670-11ea-812e-c85adbb13e22.png)

- After running the scala code **CreateJSONreport.scala**, it will create the json reports on the project root directory as followed:
1. **myjsonreport_reportByDate**: this json report contains the list of number of access by URI for those dates with more than 20,000 connections in the Web server log dataset
2. **myjsonreport_reportByIp**: this json report contains the list of number of access per IP address

## 3. Limitations / Possible Improvements
I was not able to create a jar file using the command "sbt assembly" as learnt in the class because I encountered the following error:

![error](https://user-images.githubusercontent.com/57285863/89316396-57781e00-d67c-11ea-84ea-667b2893c663.png)

Because of this issue, I could not create a new *.jar file for my completed scala code in order to do the spark-submit part.

## 4. Getting Started
In order to run this project, you need to have the following installed:
For Windows:
- [JDK: Java Development Kit](https://www.oracle.com/java/technologies/javase-downloads.html)
- [Apache Spark](https://spark.apache.org/downloads.html): Pre-build version
- Correct path for Environment Variables **SPARK_HOME** and **JAVA_HOME**
- [Scala IDE](http://scala-ide.org/download/sdk.html)

## 5. Operating System
This project was implemented on Windows 10 Professional Operating System using the following applications:
- IntelliJ IDEA
- Spark shell version 3.0.0

## 6. Integration with AWS Glue and AWS S3 Services as ETL Tool (Extract Transform and Load)
This is a tutorial step-by-step describing how to run this scala program in Amazon. All the source files for this AWS part are from Prof. Jean-Luc's [aws-emr-template](https://github.com/jlcanela/aws-emr-template). And, they are also available in my GitHub Project [DSTI_DataPipeline_AWS_ETL](https://github.com/Shutima/DSTI_DataPipeline_AWS_ETL)

- Log into your [AWS account](https://console.aws.amazon.com/console/home?region=us-east-1#)
- Click on **Services** to go to all services page.

![aws1](https://user-images.githubusercontent.com/57285863/89426555-94a2e580-d73a-11ea-9862-349223c046c0.png)

- Preparing S3 Bucket Database if not already existed, look for **S3** serice under Storage

![aws4](https://user-images.githubusercontent.com/57285863/89428771-0b40e280-d73d-11ea-891d-82693f8e15c6.png)

- Click on **Create Bucket**

![S3_1](https://user-images.githubusercontent.com/57285863/89428958-40e5cb80-d73d-11ea-8e71-3a167557b24c.png)

- Enter the name of the Bucket

![S3_2](https://user-images.githubusercontent.com/57285863/89429163-81454980-d73d-11ea-8e01-afaa830f72ac.png)

- Define the appropriate configure options

![S3_3](https://user-images.githubusercontent.com/57285863/89429735-3841c500-d73e-11ea-88fb-1ba406d6ecbd.png)

- Choose Block all public access which is default option

![S3_4](https://user-images.githubusercontent.com/57285863/89429883-645d4600-d73e-11ea-838c-529299a667fd.png)

- Click Create Bucket, the newly created Bucket will be appeared in the S3 list.

![S3_5](https://user-images.githubusercontent.com/57285863/89430102-ab4b3b80-d73e-11ea-9950-d681437ca60f.png)

- Select the newly created S3 Bucket, there will be a pop-up window on the left with the details of the bucket, click on **Copy Bucket ARN**, this will copy Bucket ARN to the clipboard, you will need this ARN number later to configure AWS Glue.

![S3_6](https://user-images.githubusercontent.com/57285863/89430470-1137c300-d73f-11ea-83d1-2b7d0f008c59.png)

- Back on the AWS Services home. Look for **AWS Glue** which is in the Analytics categeory. This is the managed [AWS ETL service](https://aws.amazon.com/glue/features/#:~:text=AWS%20Glue%20features-,AWS%20Glue%20features,time%20spent%20creating%20ETL%20jobs.). Click to go to this service.

![aws2](https://user-images.githubusercontent.com/57285863/89427072-1f83e000-d73b-11ea-83e5-a6448909ea8b.png)

- From the main menu on the left, click on **Databases** under Data Catalog

![aws3](https://user-images.githubusercontent.com/57285863/89428410-a4bbc480-d73c-11ea-8598-4790db071e18.png)

- Click on **Add Database**, then enter anyname for Database name. For Location, paste the ARN Number that was copied from the new S3 Bucket. Remove the begining characters **arn:aws:** and ending with **/database**. Then click Create.

![aws_glue1](https://user-images.githubusercontent.com/57285863/89439345-58778100-d74a-11ea-9f10-e6e8448ed7e3.png)

- Click on newly created database on AWS Glue to access it

![aws_glue2](https://user-images.githubusercontent.com/57285863/89439528-9e344980-d74a-11ea-904e-7f0529cfac75.png)

- Click the link Tables in the database to access the table creation page

![aws_glue3](https://user-images.githubusercontent.com/57285863/89439650-c8860700-d74a-11ea-8202-d3c9d4b21301.png)

- Choose Add tables > Add tables using a crawler

![aws_glue4](https://user-images.githubusercontent.com/57285863/89439965-3fbb9b00-d74b-11ea-8141-42f5d9bed215.png)

- Here please go back to S3 Bucket homepage, for this tutorial using crawler in AWS Glue, we have created another S3 Bucket called **dsti-a19-incoming1**. This S3 bucket will be for keeping the web server access log from Prof. Jean-Luc's [spark-hands-on wiki](https://github.com/jlcanela/spark-hands-on), access.log.gz.

![aws_glue5](https://user-images.githubusercontent.com/57285863/89440645-37179480-d74c-11ea-9ad6-b27b5d417fc0.png)

- The access log should be downloaded if not already done. Click on the S3 bucket **dsti-a19-incoming1** to go to it's homepage, then drag/drop the access.log.gz to upload it on the bucket. Then, click on Upload.

![S3_incoming1](https://user-images.githubusercontent.com/57285863/89440919-a5f4ed80-d74c-11ea-87c0-01f18e3d2e20.png)

- After finish uploading, you will see the access log stored on this S3 bucket.

![S3_incoming2](https://user-images.githubusercontent.com/57285863/89441244-23b8f900-d74d-11ea-8e42-80c681ffdf2b.png)

- Back to adding table with crawler configuration page. Add the details of the new crawler.

![awsglueCrawl1](https://user-images.githubusercontent.com/57285863/89441548-9d50e700-d74d-11ea-8093-846da38a937c.png)

- Add the path to the S3 bucket for the access log.

![awsglueCrawl2](https://user-images.githubusercontent.com/57285863/89441768-f1f46200-d74d-11ea-99ae-8383e47e5069.png)

- Add details for IAM role.

![awsglueCrawl3](https://user-images.githubusercontent.com/57285863/89441868-1f411000-d74e-11ea-89c7-9b774e4dcf8c.png)

- Configure the crawler output.

![awsglueCrawl4](https://user-images.githubusercontent.com/57285863/89442007-54e5f900-d74e-11ea-893c-e17c00a84fc6.png)

- Review everything then click Finish. Now we have a new table using crawler created.

![awsglueCrawl5](https://user-images.githubusercontent.com/57285863/89442086-7cd55c80-d74e-11ea-8af8-30eb49d3a970.png)

- Click on the new crawler and click on Run crawler.

![awsglueCrawl6](https://user-images.githubusercontent.com/57285863/89442271-d178d780-d74e-11ea-803f-978aee6dd5ca.png)

![awsglueCrawl7](https://user-images.githubusercontent.com/57285863/89442323-f2d9c380-d74e-11ea-858a-6b95c21a9b76.png)

- Go to menu **Classifiers** under **Crawlers** on the left AWS Glue menu.

![awsglueCrawl8](https://user-images.githubusercontent.com/57285863/89442544-5237d380-d74f-11ea-952e-38839d41a4b3.png)

- Click Add classifier, then enter the details for the classifier that is related to the access log.

![awsglueCrawl9](https://user-images.githubusercontent.com/57285863/89442812-bc507880-d74f-11ea-85e3-0478d33741c8.png)

- Create a simple **data.csv** file to test this.

![sampleCsv](https://user-images.githubusercontent.com/57285863/89443155-31bc4900-d750-11ea-9306-fdae4f43c638.png)

- Go to the location of data.csv then upload it to the S3 bucket, **dsti-a19-incoming1** where we have the access log stored.

![sampleCsv2](https://user-images.githubusercontent.com/57285863/89443417-8bbd0e80-d750-11ea-8973-f5e68ba5817b.png)

- Back to the crawler that we created, and ran. Click on the Logs. Here we can see the crawler log.

![IncomingCrawler1](https://user-images.githubusercontent.com/57285863/89443876-35040480-d751-11ea-9bae-2bf72897118b.png)

![IncomingCrawler2](https://user-images.githubusercontent.com/57285863/89444080-785e7300-d751-11ea-8655-6642fabb04fe.png)

- Now go back to the AWS Glue > Databases > Tables. It created automatically the table using crawler **incoming_dsti_a19_incoming1**. We can click inside the table to see more details.

![IncomingCrawler3](https://user-images.githubusercontent.com/57285863/89444280-c70c0d00-d751-11ea-85b3-b78cca6d7956.png)

![IncomingCrawler4](https://user-images.githubusercontent.com/57285863/89444355-e014be00-d751-11ea-8762-7b7ed57dfefe.png)

- However, it cannot identify the type as the classification is unknown. We can go back to AWS Glue > Crawlers, then select the crawler we created **dsti-incoming-crawler**, then edit the configuration. On the bottom of the edit page, we can click **Add** to add this classifier of CSV file that we created. Then, keep all the same settings, then click Finish to update the crawler.

![IncomingCrawler5](https://user-images.githubusercontent.com/57285863/89444738-76e17a80-d752-11ea-833b-028a8ba4b62d.png)

- Re-run again this crawler. Now, this crawler will run through both files: access log and the new simple csv file that we created as they are both in the same incoming S3 bucket.

![IncomingCrawler6](https://user-images.githubusercontent.com/57285863/89445479-83b29e00-d753-11ea-8b66-9788559c1a40.png)

- Now, when we go back to AWS Glue > Databases > Tables, there are 2 more new tables created for access log and the simple data.csv file.

![IncomingCrawler7](https://user-images.githubusercontent.com/57285863/89446016-3c78dd00-d754-11ea-9c1f-40155bf8942c.png)

- When clicking in the table **incoming_data_csv**, we can see that it reads through the data inside the CSV file correctly.

![IncomingCrawler8](https://user-images.githubusercontent.com/57285863/89446112-692cf480-d754-11ea-9112-7a1daf7f4c13.png)

- From AWS Services homepage, search for AWS Athena service. Here, you will see the tables from AWS Glue that were created from the crawler. Click on **Preview Table**

![Athena1](https://user-images.githubusercontent.com/57285863/89449615-9fb93e00-d759-11ea-9a2d-13ee1c17a914.png)

- For now, you will get an error no output location provided.

![Athena2](https://user-images.githubusercontent.com/57285863/89449713-c6777480-d759-11ea-823f-338b8f989ccf.png)

- Click on the Settings menu on top right.

![Athena3](https://user-images.githubusercontent.com/57285863/89449811-e9098d80-d759-11ea-9bee-3763fac70263.png)

- Here, you will need to go back to S3 service to create a new bucket for Athena. Go back to S3 service and create a new S3 bucket.

![Athena4](https://user-images.githubusercontent.com/57285863/89450085-47367080-d75a-11ea-955f-29a322acd9d6.png)

![Athena5](https://user-images.githubusercontent.com/57285863/89450184-751bb500-d75a-11ea-99ab-5a41d899bf7f.png)

- Select the new bucket created for Athena, and click on copy the bucket ARN.

![Athena6](https://user-images.githubusercontent.com/57285863/89450340-ad22f800-d75a-11ea-82e1-f1a8f637e92d.png)

- Click to go inside the new bucket for Athen, here you need to create new folder named **output**

![Athena7](https://user-images.githubusercontent.com/57285863/89450806-726d8f80-d75b-11ea-9ca6-09bf63d76502.png)

- Go back to Athena service, and paste the S3 bucket ARN for Athena bucket. Also add the additional information, normally we should use **CSE-KMS** or **SSE-KMS** as Encryption Type. However, here for the sake of the tutorial, we will choose the default SSE-S3.

![Athena8](https://user-images.githubusercontent.com/57285863/89450967-bf516600-d75b-11ea-8206-dae194c05651.png)

- On Athena, select another table **incoming_data_csv** and Preview this table, then run the query.

![Athena9](https://user-images.githubusercontent.com/57285863/89451376-67672f00-d75c-11ea-9b35-aa1d4d06233e.png)

- Then go back to the Athena bucket on S3 service, output folder, here you can see all of the action you did in Athena. It will extract the data as per the query ran in Athena.

![Athena10](https://user-images.githubusercontent.com/57285863/89451592-bb721380-d75c-11ea-8d3a-cf21aca71c45.png)

![Athena11](https://user-images.githubusercontent.com/57285863/89451605-bdd46d80-d75c-11ea-8659-5e957dbbf5aa.png)

- The Athena servie integrates the S3 buckets that we created and also AWS Glue service. You use Athena service to view the results of the table.














