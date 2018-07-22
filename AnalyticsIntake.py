from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from mykafka import MyKafka
import logging
from logging.config import dictConfig
import time
import os
from datetime import datetime, timedelta


class AnalyticsIntake(object):
    SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
    KEY_FILE_LOCATION = None
    VIEW_ID = None
    kafka_brokers = None
    topic = None
    delay = 3600

    def __init__(self):
        self.init_logging()
        if 'KAFKA_BROKERS' in os.environ:
            self.kafka_brokers = os.environ['KAFKA_BROKERS'].split(',')
            self.logger.info(
                "Set KAFKA_BROKERS: {}".format(self.kafka_brokers))
        else:
            raise ValueError('KAFKA_BROKERS environment variable not set')
        if 'KEY_FILE' in os.environ:
            self.KEY_FILE_LOCATION = os.environ['KEY_FILE']
            self.logger.info("Set KEY_FILE: {}".format(self.KEY_FILE_LOCATION))
        else:
            raise ValueError('KEY_FILE environment variable not set')
        if 'VIEW_ID' in os.environ:
            self.VIEW_ID = os.environ['VIEW_ID']
            self.logger.info("Set VIEW_ID: {}".format(self.VIEW_ID))
        else:
            raise ValueError('VIEW_ID environment variable not set')
        if 'TOPIC' in os.environ:
            self.topic = os.environ['TOPIC']
            self.logger.info("Set TOPIC: {}".format(self.topic))
        else:
            raise ValueError('TOPIC environment variable not set')
        if 'DELAY' in os.environ:
            self.delay = int(os.environ['DELAY'])
            self.logger.info("Set DELAY: {} s".format(self.delay))
        else:
            self.delay = 3600
            self.logger.info(
                "DELAY environment variable not set - Setting to default {} s".format(self.delay))

    def init_logging(self):
        logging_config = dict(
            version=1,
            formatters={
                'f': {'format':
                      '%(asctime)s %(name)-12s %(levelname)-8s %(message)s'}
            },
            handlers={
                'h': {'class': 'logging.StreamHandler',
                      'formatter': 'f',
                      'level': logging.INFO}
            },
            root={
                'handlers': ['h'],
                'level': logging.INFO,
            },
        )
        self.logger = logging.getLogger()
        logging.getLogger("googleapiclient").setLevel(logging.ERROR)
        dictConfig(logging_config)

    def init_reporting(self):
        creds = ServiceAccountCredentials.from_json_keyfile_name(
            self.KEY_FILE_LOCATION, self.SCOPES)
        analytics = build('analyticsreporting', 'v4', credentials=creds)
        return analytics

    def get_reports(self, analytics):
        return analytics.reports().batchGet(
            body={
                'reportRequests': [
                    {
                        'viewId': self.VIEW_ID,
                        'dateRanges': [{'startDate': '7daysAgo', 'endDate': 'today'}],
                        'metrics': [{'expression': 'ga:sessions'}],
                        'dimensions': [{'name': 'ga:pageTitle'}]
                    }]
            }
        ).execute()

    def get_page_visit_data(self):
        analytics = self.init_reporting()
        response = self.get_reports(analytics)
        return response

    def publish_metrics(self, logger, response):
        #kafka_brokers = ['mslave1.admintome.lab:31000']
        logger.info(
            'Publishing site data to Kafka Broker {}'.format(self.kafka_brokers))
        mykafka = MyKafka(self.kafka_brokers)
        mykafka.send_page_data(response, self.topic)
        logger.info(
            'Successfully published site data to Kafka Broker {}'.format(self.kafka_brokers))

    def main(self):
        starttime = time.time()
        self.logger.info('Starting Google Analytics API Intake Daemon')
        while True:
            self.logger.info('Pulling site data from Google Analytics API')
            response = self.get_page_visit_data()
            self.logger.info(
                'Got back data of type: {}'.format(type(response)))
            self.logger.info(
                'Successfully pulled site data from Google Analytics API')
            self.publish_metrics(self.logger, response)
            now = datetime.now()
            self.logger.info('Scheduling next run at {}'.format(
                now + timedelta(seconds=self.delay)))
            time.sleep(self.delay - ((time.time() - starttime) % self.delay))


if __name__ == '__main__':
    intake = AnalyticsIntake()
    intake.main()
