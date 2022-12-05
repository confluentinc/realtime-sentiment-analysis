import json
import tweepy
from datetime import datetime
from kafka import KafkaProducer, KafkaClient

class TweetListener(tweepy.StreamingClient):

    def on_data(self, data):
        try:
            tweet = json.loads(data.decode("utf-8"))
            event = {}
            event['tweet_id'] = tweet['data']['id']
            event['event_time'] = datetime.now().isoformat(sep='T')
            event['tweet_text'] = tweet['data']['text']
            print(event)
            producer.send('<CONFLUENT_TOPIC_NAME>', value=event)
        except Exception as e:
            print(e)
            return False
        return True # Don't kill the stream

# Twitter Credentials Obtained from http://dev.twitter.com
bearer_token = "<TWITTER_BEARER_TOKEN>"

# Initialise Kafka producer. Get details from https://confluent.cloud/go/clients 
producer = KafkaProducer(security_protocol="SASL_SSL",
                             bootstrap_servers="<CONFLUENT_BOOTSTRAP_SERVER>",
                             sasl_mechanism="PLAIN",
                             sasl_plain_username="<CONFLUENT_API_KEY>",
                             sasl_plain_password="<CONFLUENT_SECRET>",
                             value_serializer=lambda x: json.dumps(x).encode("utf-8"),
                             retry_backoff_ms=500,
                             request_timeout_ms=20000)


stream = TweetListener(bearer_token=bearer_token)

'''
Rule to process tweets that:
  - contain the word "bitcoin"
  - does not contain promotions
  - does not contain images
  - In english lang
'''
  
  
stream.add_rules(tweepy.StreamRule("bitcoin lang:en -has:links -has:mentions -has:images -is:nullcast"))
stream.filter()
