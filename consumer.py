import psycopg2
import json
from kafka import KafkaConsumer

if __name__ == "__main__":

    with open('config.json') as json_file:
        config = json.load(json_file)

    topic = config['credentials']['kafka']['topic']

    try:
        consumer = KafkaConsumer (
            topic,
            bootstrap_servers=config['credentials']['kafka']['uri'],
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
    except Exception as ex:
        raise Exception("Unable to connect to Kafka from consumer")

    # db connect
    # https://www.postgresqltutorial.com/postgresql-python/connect/
    try:
        conn = psycopg2.connect(
            host=config['credentials']['db']['host'],
            database=config['credentials']['db']['db_name'],
            user=config['credentials']['db']['user'],
            password=config['credentials']['db']['password']
        )
        conn.autocommit = True

        print("Connected to the Database")
    except Exception as ex:
        raise Exception("Unable to connect to database")

    for msg in consumer:
        try:
            cur = conn.cursor()
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
        print("Topic Name=%s,Message=%s"%(msg.topic,msg.value))