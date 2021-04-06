import psycopg2
import json
from kafka import KafkaConsumer

class Consumer:
    kafka_server = None
    consumer = None
    kafka_topic = None
    db_config = None
    db_conn = None

    def __init__(self):
        config = self.load_config()
        self.kafka_server = config['credentials']['kafka']['uri']
        self.kafka_topic = config['credentials']['kafka']['topic']
        self.db_config  = config['credentials']['db']

        self.init_consumer()
        self.db_connect()
        self.poll_from_topic()

        
    def load_config(self):
        with open('config.json') as json_file:
            config = json.load(json_file)
        return config

    def init_consumer(self):
        try:
            consumer = KafkaConsumer (
                self.kafka_topic,
                bootstrap_servers=self.kafka_server,
                security_protocol="SSL",
                ssl_cafile="keys/ca.pem",
                ssl_certfile="keys/service.cert",
                ssl_keyfile="keys/service.key",
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
        except Exception as ex:
            raise Exception("Unable to connect to Kafka from consumer")

        self.consumer = consumer
        

    def db_connect(self):
        # db connect
        # https://www.postgresqltutorial.com/postgresql-python/connect/
        try:
            conn = psycopg2.connect(
                host=self.db_config['host'],
                database=self.db_config['db_name'],
                port=self.db_config['port'],
                user=self.db_config['user'],
                password=self.db_config['password']
            )
            conn.autocommit = True

            self.db_conn = conn
            print("Connected to the Database")
        except Exception as ex:
            raise Exception("Unable to connect to database")

    def save_message(self, msg):
        try:
            cur = self.db_conn.cursor()
            cur.execute(
                "insert into website_availability(website, status_code, response_time, regex_found) values('{}', {}, {}, {})".format(
                    msg.value["target"],
                    msg.value["status_code"],
                    msg.value["response_time"],
                    msg.value["regex"]
                )
            )
        except Exception as ex:
            print(ex)
        
    def poll_from_topic(self):
        for msg in self.consumer:
            self.save_message(msg)
            print("Topic Name=%s,Message=%s"%(msg.topic,msg.value))

if __name__ == "__main__":
    c = Consumer()