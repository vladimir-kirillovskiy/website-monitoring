import requests
import re
import concurrent.futures
from json import dumps, load
from time import sleep
from kafka import KafkaProducer


class Producer:
    kafka_server = None
    producer = None
    kafka_topic = None

    def __init__(self):
        config = self.load_config()
        self.kafka_server = config['credentials']['kafka']['uri']
        self.kafka_topic = config['credentials']['kafka']['topic']

        self.init_producer()
        self.start_multithreaded_loop(config['websites'])
        
    def init_producer(self):
        try:
            producer = KafkaProducer(
                    bootstrap_servers=self.kafka_server,
                    security_protocol="SSL",
                    ssl_cafile="keys/ca.pem",
                    ssl_certfile="keys/service.cert",
                    ssl_keyfile="keys/service.key",
                    value_serializer=lambda x: dumps(x).encode('utf-8')
                )

        except Exception as ex:
            raise Exception("Unable to connect to Kafka from producer")

        self.producer = producer

    def load_config(self):
        with open('config_aiven.json') as json_file:
            config = load(json_file)
        return config
    
    def start_multithreaded_loop(self, websites):
        print("start_multithreaded_loop")
        # Multithreading to support multiple website monitoring
        # https://docs.python.org/3/library/concurrent.futures.html
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(self.initialize_loop, website, params) for website, params in websites.items()]

    def check(self, target, regex):
        """
            Ping a website and send message to the topic for consumer to pick up.
        """
        try:
            response = requests.get(target)
            is_regex_found = bool(re.search(regex, response.text)) if regex else None      
            message = {
                    'target': target,
                    'status_code': response.status_code,
                    # response time - https://stackoverflow.com/a/43260678/6908587
                    'response_time': response.elapsed.total_seconds(),
                    'regex': is_regex_found 
                }

            self.producer.send(self.kafka_topic, message)
            self.producer.flush()

        except Exception as ex:
            print("Check Exception: ", ex)

    def initialize_loop(self, website, params):
        """
            Loop to keep monitoring websites. Run in multithread
        """
        while True:
            self.check(website, params['regex']) 
            sleep(params['interval'])

if __name__ == "__main__":
    p = Producer()