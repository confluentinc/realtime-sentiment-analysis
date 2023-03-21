#  Real time Sentiment Analysis using Confluent, Amazon Kinesis Data Analytics and Amazon Comprehend

  
This repository accompanies the [Enhance Customer Experience Through Real-Time Sentiment Analysis with Confluent on AWS](https://aws.amazon.com/blogs/apn/enhance-customer-experience-through-real-time-sentiment-analysis-with-confluent-on-aws/) blog post. It contains **one** [AWS Cloudformation](https://aws.amazon.com/cloudformation/) template. 

The template deploys:
1. PyFlink stream processing job that runs as an [Amazon Kinesis Data Analytics](https://aws.amazon.com/kinesis/data-analytics/) application. The job consumes textual data (e.g twitter feeds) stored in a [Confluent Cloud](https://www.confluent.io/) topic (input.topic), invokes [Amazon Comprehend](https://aws.amazon.com/comprehend/) APIs ([DetectSentiment](https://docs.aws.amazon.com/comprehend/latest/APIReference/API_DetectSentiment.html)) in real time to detect sentiment and writes the output to Confluent Cloud again.


```bash
├── Artifacts                             <-- Directory that will hold solution Artifacts
│   ├── dashboard.ndjson                  <-- An export of a sample OpenSearch dashboard to visualise transaction data
├── KafkaFlinkConnector                   <-- Directory contains Kafka connector for Flink.
│   └── pom.xml                           <-- POM for Flink Fat Jar
├── Producer                              <-- Directory contains sample twitter producer code.
│   ├── requirement.txt                   <-- Dependencies file for sample producer
│   └── twitter_producer.py               <-- Sample producer python code
└── RealTimeSentiment                     <-- Directory contains Kinesis Data Analytics PyFlink application code 
│   ├── main.py                           <-- Kinesis Data Analytics PyFlink application code 
│   ├── bin
│   │   ├── requirements.txt              <-- Dependencies file for Kinesis Data Analytics PyFlink application code 
├── Realtime_Sentiment_Analysis_CFN.yml   <-- CFN template for KDA Application
└── README.md
```


## General Requirements

1. [Install Python](https://realpython.com/installing-python/) 3.8.2 or later
2. [AWS CLI already configured](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html) with Administrator permission
3. Confluent cloud cluster with public endpoint. (*Note: private connectivity is supported but KDA needs to be deployed in a VPC*).
4. Confluent API Key and Secret with read access to input topic and write access to output topic.
5. Maven installed


## Package and upload solution artifacts.

1. Clone the repo onto your local development machine using `git clone <repo url>`.
2. Change directory to solution repository.

```

cd realtime-sentiment-analysis
```

### Building and generating fat jar for SASL auth Confluent <==> KDA

Confluent uses SASL/PLAIN as an authentication mechanism. For Kinesis Data Analytics to read/write data from/to Confluent it needs a special Login plugin - **PlainLoginModule**. In order to be able to reference the PlainLoginModule in Kinesis Data Analytics apps we need to build a fat jar containing the Kafka SQL/Table API connector as well as the kafka-clients library.

NOTE: This sample uses *Flink 1.13* but you could follow a similar approach for other versions of Flink as well.

Generate the jar using the following command:

```
(cd KafkaFlinkConnector; mvn package)
```

After running the above command, you should see the built jar file under the `target/` folder:

```
ls -alh target/
...
drwxr-xr-x   8 user  group   256B Aug 26 13:36 .
drwxr-xr-x  11 user  group   352B Aug 27 08:29 ..
-rw-r--r--   1 user  group   3.6M Aug 26 13:36 SASLLoginFatJar-0.1.jar

```

### Install Kinesis Data Analytics PyFlink application dependencies and package code

```bash
pip3 install -r ./RealTimeSentiment/bin/requirements.txt -t ./RealTimeSentiment/lib/packages

cp ./KafkaFlinkConnector/target/SASLLoginFatJar-0.1.jar ./RealTimeSentiment/lib

(mkdir Artifacts; zip -r ./Artifacts/RealTimeSentiment.zip ./RealTimeSentiment)
```

This will:
Install required dependencies for the Apache Flink application as per requirements.txt file.
Then package all artifacts into RealTimeSentiment.zip file that will be created under the Artifacts directory. 


### Upload solution artifacts

1. Run the following command to create a unique Amazon S3 bucket which will be used to store the solution artifacts.

Replace:
* **<S3_Bucket_name>** with your unique bucket name and 
* **<Confluent_Cloud_Region>** with the region where Comfluent Cloud cluster is running E.g. *eu-west-1* 

```

aws s3 mb s3://<S3_Bucket_name> --region <Confluent_Cloud_Region>
```


2. Run the following command to sync the solution artifacts with the newly created buckets. 

**Note: All artifacts should be stored on the bucket root**

```

aws s3 sync ./Artifacts/ s3://<S3_Bucket_name>
```


## Deploy solution


Run the following command to deploy the CloudFormation template

Replace:

* **<S3_Bucket_name>** --> The bucket you created in the upload solution artifacts step above.
* **<Confluent_Input_Topic_Name>** --> Input Kafka topic name. E.g *raw.tweets*
* **<Confluent_Output_Topic_Name>** --> Output Kafka topic name. E.g *processed.tweets*.
* **<Confluent_Bootstrap_Server>** --> Bootstrap server Confluent Cloud Kafka cluster. Get it from here --> https://confluent.cloud/go/clients
* **<Confluent_API_Key>** --> API Key of Confluent Cloud Kafka cluster. Get it from here --> https://confluent.cloud/go/clients
* **<Confluent_API_Secret>** --> API Secret of Confluent Cloud Kafka cluster. Get it from here --> https://confluent.cloud/go/clients
* **<Confluent_Cloud_Region>** --> AWS Region of the Confluent Cloud Kafka cluster.
* **<Stack_name>** CloudFormation stack name. The stack name must satisfy the regular expression pattern: [a-z][a-z0-9\-]+ and must be less than 15 characters long. For example; *realtime-sentiment*

```

aws cloudformation create-stack --template-body file://Realtime_Sentiment_Analysis_CFN.yml --parameters \
ParameterKey=BucketName,ParameterValue=<S3_Bucket_name> \
ParameterKey=KafkaInputTopic,ParameterValue=<Confluent_Input_Topic_Name> \
ParameterKey=KafkaOutputTopic,ParameterValue=<Confluent_Output_Topic_Name> \
ParameterKey=BootstrapServers,ParameterValue=<Confluent_Bootstrap_Server> \
ParameterKey=ConfluentAPIKey,ParameterValue=<Confluent_API_Key> \
ParameterKey=ConfluentAPISecret,ParameterValue=<Confluent_API_Secret> \
--capabilities CAPABILITY_NAMED_IAM \
--region <Confluent_Cloud_Region> \
--stack-name <Stack_name>
```

