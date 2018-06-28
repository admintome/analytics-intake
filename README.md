# Analytics Intake

This is a sample Python application to pull metrics data from [Google Analytics API](https://developers.google.com/analytics/devguides/reporting/core/v4/) and push those metrics in JSON form to a Kafka Topic.

## Running

You will need to setup several environment variables to get this to work

__KAFKA_BROKERS__ - A comma separted list of your Kafka brokers including port
__KEY_FILE__ - Location of your keyfile.  See [
Hello Analytics Reporting API v4; Python quickstart for service accounts](https://developers.google.com/analytics/devguides/reporting/core/v4/quickstart/service-py).  Follow the instructions under Enable the API -> Create Credentials.
__VIEW_ID__ - You can use the [Account Explorer](https://ga-dev-tools.appspot.com/account-explorer/) to find a View ID.
__TOPIC__ - The name of the Kafka Topic that you want to publish to.
__DELAY__ - The delay between pulls from Google Analytics.  Defaults to 3600s (1 Hour)

## Running the app

Running the app is pretty easy.

```
$ virtualenv ../venvs/analytics-intake
$ source ../venvs/analytics-intake/bin/activate
$ pip install -r requirements.txt
$ python AnalyticsIntake.py
```

## More Information

For more info about this app, checkout my Fast Data Architecture Series on my blog:

[AdminTome Blog: Fast Data Architecture Seriess](http://www.admintome.com/blog/category/howto/fast-data-architecture-series/)
