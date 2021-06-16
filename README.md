## Google Transparency Report data scraper

This script uses the Google Traffic Analysis API from
https://github.com/Jigsaw-Code/net-analysis to fetch data from the Google
Transparency Report internal API and write it into a kafka pipe
in a format that is compatible with the data collection backend used by IODA.

### Dependencies

 * pytimeseries -- Install the python3-pytimeseries package from the CAIDA
                   package repository (https://pkg.caida.org/os/ubuntu) or
                   build from source at https://github.com/CAIDA/pytimeseries
 * netanalysis -- Install by running `pip install git+git://github.com/Jigsaw-Code/net-analysis.git@master`; more information at https://github.com/Jigsaw-Code/net-analysis


### Usage
```
usage: gTransScraper.py [-h] --broker BROKER --channel CHANNEL --topicprefix
                        TOPICPREFIX [--products PRODUCTS]
                        [--starttime STARTTIME] [--endtime ENDTIME]

required arguments:
  --broker BROKER       The kafka broker to connect to
  --channel CHANNEL     Kafka channel to write the data into
  --topicprefix TOPICPREFIX
                        Topic prefix to prepend to each Kafka message

optional arguments:
  -h, --help            show this help message and exit
  --products PRODUCTS   Comma-separated list of products to get data for
  --starttime STARTTIME
                        Fetch traffic data starting from the given Unix
                        timestamp
  --endtime ENDTIME     Fetch traffic data up until the given Unix timestamp

```

### Argument notes

`--products` -- By default, the script will fetch data for *all* valid
Google products. Use this argument to limit the fetching to a specific subset
of products, e.g. `--products translate,youtube` will only fetch data for
Translate and YouTube.

`--starttime` and `--endtime` -- the default is to fetch all data going back
24 hours from now, but you can use these arguments to specify a specific time
range to fetch data for. The times given must be Unix timestamps.


### Kafka Message Format

The fetched data is inserted into Kafka using the graphite text format, as
follows:

```
<KEY> <VALUE> <TIMESTAMP>
```

The key is a dot-separated series of terms that describes the particular data
series that the given data point belongs to, constructed using the following
schema:

```
google_tr.<CONTINENT_CODE>.<COUNTRY_CODE>.<PRODUCT_NAME>.traffic
```

Continent and country codes are the official 2-letter ISO codes for those
regions. The product name is the name provided by the data fetching API, which
will always be in upper case.

For example, the translate traffic for USA will have the key
`google_tr.NA.US.TRANSLATE.traffic`.


### Telegraf -> InfluxDB templating

If you are using telegraf to consume time series data from Kafka and insert it
into an InfluxDB database, there are two changes to make to your
`inputs.kafka_consumer` config to add support for the data produced by this
script (assuming your existing config is already consuming from the broker
that the script will be writing to).

 1. Add `"<TOPICPREFIX>.<CHANNEL>"` to the list of `topics`, where `TOPICPREFIX`
    and `CHANNEL` match the arguments you will provide on the command line when
    you run the script.

 2. Add the following entry to the list of `templates`:
       `"google_tr.*.*.*.* measurement.continent_code.country_code.product.field"`

Any data points fetched by this script should now appear in your Influx
database under the `google_tr` measurement.

