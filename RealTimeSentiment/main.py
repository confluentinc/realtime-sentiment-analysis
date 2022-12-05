from pyflink.table import EnvironmentSettings, StreamTableEnvironment, StatementSet
from pyflink.table.udf import udf
from pyflink.table import DataTypes, Row
import os
import json
import logging



env_settings = EnvironmentSettings.new_instance().in_streaming_mode().use_blink_planner().build()
table_env = StreamTableEnvironment.create(environment_settings=env_settings)
table_env.add_python_file("file:///" + os.path.dirname(os.path.realpath(__file__)) + "/lib/packages") # Add python depencies
table_env.set_python_requirements(requirements_file_path="file:///" + os.path.dirname(os.path.realpath(__file__)) + "/bin/requirements.txt") #Boto3 package
table_env.get_config().get_configuration().set_string("taskmanager.memory.task.off-heap.size", '512mb') # Required until v1.11
statement_set = table_env.create_statement_set()


# UDF Funtions
####################################################################################################

@udf(input_types=[DataTypes.INT()],result_type=DataTypes.ROW([DataTypes.FIELD("outcome", DataTypes.STRING()),DataTypes.FIELD("Positive", DataTypes.DOUBLE()),DataTypes.FIELD("Negative", DataTypes.DOUBLE()),DataTypes.FIELD("Neutral", DataTypes.DOUBLE()),DataTypes.FIELD("Mixed", DataTypes.DOUBLE())]))
def get_sentiment(tweet_text, aws_region):
    
    import boto3

    # Get sentiment for each tweet

    client = boto3.client('comprehend', region_name=aws_region)
    sentiment_response = client.detect_sentiment(
        Text=tweet_text,
        LanguageCode='en'
    )

    outcome = sentiment_response['Sentiment']
    Positive = sentiment_response['SentimentScore']['Positive']
    Negative = sentiment_response['SentimentScore']['Negative']
    Neutral = sentiment_response['SentimentScore']['Neutral']
    Mixed = sentiment_response['SentimentScore']['Mixed']

    return Row(outcome,Positive,Negative,Neutral,Mixed)


# UDFs Registry
get_sentiment_name = "get_sentiment"
table_env.register_function(get_sentiment_name, get_sentiment)


# DDL Funtions
####################################################################################################

# Input transactions topic

def create_table_input(table_name, stream_name, broker, jaas_config):
    return """ CREATE TABLE {0} (
                `tweet_text` STRING NOT NULL,
                `tweet_id` VARCHAR(64) NOT NULL,
                `event_time` TIMESTAMP(6) NOT NULL
              )
              WITH (
                'connector' = 'kafka',
                'topic' = '{1}',
                'properties.bootstrap.servers' = '{2}',
                'properties.group.id' = 'testGroupTFI',
                'format' = 'json',
                'json.timestamp-format.standard' = 'ISO-8601',
                'scan.startup.mode' = 'latest-offset',
                'properties.security.protocol' = 'SASL_SSL',
                'properties.sasl.mechanism' = 'PLAIN',
                'properties.sasl.jaas.config' = '{3}'
              ) """.format(table_name, stream_name, broker, jaas_config)

# Input outcome topic

def create_table_output_kafka(table_name, stream_name, broker, jaas_config):
    return """ CREATE TABLE {0} (
                `tweet_text` STRING NOT NULL,
                `tweet_id` VARCHAR(64) NOT NULL,
                `event_time` TIMESTAMP(6) NOT NULL,
                `sentiment` Row<`outcome` STRING, `Positive`  DOUBLE, `Negative`  DOUBLE, `Neutral` DOUBLE, `Mixed` DOUBLE>
              )
              WITH (
                'connector' = 'kafka',
                'topic' = '{1}',
                'properties.bootstrap.servers' = '{2}',
                'properties.group.id' = 'testGroupTFI',
                'format' = 'json',
                'json.timestamp-format.standard' = 'ISO-8601',
                'properties.security.protocol' = 'SASL_SSL',
                'properties.sasl.mechanism' = 'PLAIN',
                'properties.sasl.jaas.config' = '{3}'
            ) """.format(table_name, stream_name, broker, jaas_config)


def compute_sentiment_table(input_table, aws_region):
        scan_input_table = table_env.from_path(input_table)
        sentiment_table = scan_input_table.add_columns(f"'{aws_region}' as aws_region").select(f"tweet_id, event_time, tweet_text, aws_region, {get_sentiment_name}(tweet_text, aws_region) as sentiment")
        return sentiment_table


def insert_stream(insert_into, insert_from):
    return """ INSERT INTO {0}
               Select tweet_text, tweet_id, event_time, sentiment  FROM {1}""".format(insert_into, insert_from)


# Extract application properties


def app_properties():
    file_path = '/etc/flink/application_properties.json'
    if os.path.isfile(file_path):
        with open(file_path, 'r') as file:
            contents = file.read()
            print('Contents of ' + file_path)
            print(contents)
            properties = json.loads(contents)
            return properties
    else:
        print('A file at "{}" was not found'.format(file_path))


def property_map(props, property_group_id):
    for prop in props:
        if prop["PropertyGroupId"] == property_group_id:
            return prop["PropertyMap"]


def main():


    INPUT_PROPERTY_GROUP_KEY = "producer.config.0"
    CONSUMER_PROPERTY_GROUP_KEY = "consumer.config.0"

    INPUT_TOPIC_KEY = "input.topic.name" 

    CONFLUENT_API_KEY = "api.key"
    CONFLUENT_API_SECRET = "api.secret"
    AWS_REGION_KEY = "aws.region"
    BROKER_KEY = "bootstrap.servers"


    OUTPUT_TOPIC_KEY = "output.topic.name"
    

    props = app_properties()

    input_property_map = property_map(props, INPUT_PROPERTY_GROUP_KEY)
    output_property_map = property_map(props, CONSUMER_PROPERTY_GROUP_KEY)

# Getting producer parameters
    input_stream = input_property_map[INPUT_TOPIC_KEY]
    broker = input_property_map[BROKER_KEY]
    aws_region = input_property_map[AWS_REGION_KEY]
    user = input_property_map[CONFLUENT_API_KEY]
    secret = input_property_map[CONFLUENT_API_SECRET]
# Getting consumer parameters
    output_stream = output_property_map[OUTPUT_TOPIC_KEY]

    input_table = "input_table"
    output_table = "output_table"

# Constructing Confluent connection string

    jaas_config = f'org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{user}\" password=\"{secret}\";'

# Create input and output table
    table_env.execute_sql(create_table_input(input_table, input_stream, broker, jaas_config))
    table_env.execute_sql(create_table_output_kafka(output_table, output_stream, broker, jaas_config))

# Compute temp view with Sentiment results
    sentiment_table = compute_sentiment_table(input_table,aws_region)
    table_env.create_temporary_view("sentiment_table", sentiment_table)

# Insert Sentiment view to output topic 
    statement_set.add_insert_sql(insert_stream(output_table, "sentiment_table"))

    statement_set.execute()

if __name__ == '__main__':
    main()
