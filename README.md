# Project Introduction: STEDI Human Balance Analytics
In this project, acting as a data engineer for the STEDI team, we'll build a data lakehouse solution for sensor data that trains a machine learning model.

### Project Details
The STEDI Team has been hard at work developing a hardware STEDI Step Trainer that:
<br>
* trains the user to do a STEDI balance exercise;
* and has sensors on the device that collect data to train a machine-learning algorithm to detect steps;
* has a companion mobile app that collects customer data and interacts with the device sensors.
<br>
STEDI has heard from millions of early adopters who are willing to purchase the STEDI Step Trainers and use them.

Several customers have already received their Step Trainers, installed the mobile application, and begun using them together to test their balance. The Step Trainer is just a motion sensor that records the distance of the object detected. The app uses a mobile phone accelerometer to detect motion in the X, Y, and Z directions.

The STEDI team wants to use the motion sensor data to train a machine learning model to detect steps accurately in real-time. Privacy will be a primary consideration in deciding what data can be used.

Some of the early adopters have agreed to share their data for research purposes. Only these customers’ Step Trainer and accelerometer data should be used in the training data for the machine learning model.

### Project Summary
As a data engineer on the STEDI Step Trainer team, we'll need to extract the data produced by the STEDI Step Trainer sensors and the mobile app, and curate them into a data lakehouse solution on AWS so that Data Scientists can train the learning model.

## Project Environment
### AWS Environment
We'll use the data from the STEDI Step Trainer and mobile app to develop a lakehouse solution in the cloud that curates the data for the machine learning model using:
* Python and Spark
* AWS Glue
* AWS Athena
* AWS S3
<br>
### Workflow Environment Configuration
We'll be creating Python scripts using AWS Glue and Glue Studio. These web-based tools and services contain multiple options for editors to write or generate Python code that uses PySpark. Those have been saved to this Github Repository.

## Project Data
STEDI has three JSON data sources to use from the Step Trainer and we'll extract it from their respective public S3 bucket locations:

* **1. Customer Records (from fulfillment and the STEDI website):**
_AWS S3 Bucket URI - s3://cd0030bucket/customers/_
<br>
Contains the following fields:
* serialnumber
* sharewithpublicasofdate
* birthday
* registrationdate
* sharewithresearchasofdate
* customername
* email
* lastupdatedate
* phone
* sharewithfriendsasofdate

* **2. Step Trainer Records (data from the motion sensor):**
_AWS S3 Bucket URI - s3://cd0030bucket/step_trainer/_
<br>
Contains the following fields:
* sensorReadingTime
* serialNumber
* distanceFromObject

* **3. Accelerometer Records (from the mobile app):**
_AWS S3 Bucket URI - s3://cd0030bucket/accelerometer/_
<br>
Contains the following fields:
* timeStamp
* serialNumber
* x
* y
* z

### Relevant Notes:
* `step_trainer_trusted` table has stored user's email address as to later join with user field from `accelerometer_truted` table;
* `machine_learning_curated` was created by joining columns:
    1. `accelerometer_trusted` user with `step_trainer_trusted` email;
    2. `accelerometer_trusted` timeStamp with `step_trainer_trusted` sensorReadingTime.