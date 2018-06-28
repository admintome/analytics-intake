from kafka import KafkaProducer
import json


class MyKafka(object):

    def __init__(self, kafka_brokers):
        self.producer = KafkaProducer(
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            bootstrap_servers=kafka_brokers
        )

    def send_page_data(self, json_data, topic):
        result = self.producer.send(topic, json_data)
        print("kafka send result: {}".format(result.get()))
