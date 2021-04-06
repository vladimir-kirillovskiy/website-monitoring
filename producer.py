import requests
import re
import concurrent.futures
from json import dumps, load
from time import sleep
from kafka import KafkaProducer


def check(producer, target, regex, topic):
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

        producer.send(topic, message)
        producer.flush()

    except Exception as ex:
        print("Check Exception: ", ex)


def initialize_loop(producer, topic, website, params):
    """
        Loop to keep monitoring websites. Run in multithread
    """
    while True:
        check(producer, website, params['regex'], topic) 
        sleep(params['interval'])

if __name__ == "__main__":
    
    with open('config.json') as json_file:
        config = load(json_file)
    
    try:
        producer = KafkaProducer(
                bootstrap_servers=config['credentials']['kafka']['uri'],
                value_serializer=lambda x: dumps(x).encode('utf-8')
            )

    except Exception as ex:
        raise Exception("Unable to connect to Kafka from producer")

    topic = config['credentials']['kafka']['topic']

    # Multithreading to support multiple website monitoring
    # https://docs.python.org/3/library/concurrent.futures.html
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(initialize_loop, producer, topic, website, params) for website, params in config['websites'].items()]