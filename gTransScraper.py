#!/usr/bin/python3

# This software is Copyright (C) 2021 The Regents of the University of
# California. All Rights Reserved. Permission to copy, modify, and distribute
# this software and its documentation for educational, research and non-profit
# purposes, without fee, and without a written agreement is hereby granted,
# provided that the above copyright notice, this paragraph and the following
# three paragraphs appear in all copies. Permission to make commercial use of
# this software may be obtained by contacting:
#
# Office of Innovation and Commercialization
# 9500 Gilman Drive, Mail Code 0910
# University of California
# La Jolla, CA 92093-0910
# (858) 534-5815
# invent@ucsd.edu
#
# This software program and documentation are copyrighted by The Regents of the
# University of California. The software program and documentation are supplied
# "as is", without any accompanying services from The Regents. The Regents does
# not warrant that the operation of the program will be uninterrupted or
# error-free. The end-user understands that the program was developed for
# research purposes and is advised not to rely exclusively on the program for
# any reason.
#
# IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY FOR
# DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
# LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION,
# EVEN IF THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED OF THE POSSIBILITY OF
# SUCH DAMAGE. THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY
# WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED
# HEREUNDER IS ON AN "AS IS" BASIS, AND THE UNIVERSITY OF CALIFORNIA HAS NO
# OBLIGATIONS TO PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR
# MODIFICATIONS.

# This code uses the Google Traffic Analysis API from
# https://github.com/Jigsaw-Code/net-analysis to fetch data from the
# Google Transparency Report internal API and write it into a kafka
# pipe (using pytimeseries) in a format that is compatible with the
# data collection backend for IODA.

# Author: Shane Alcock <shane@alcock.co.nz>

import _pytimeseries
import datetime, sys, argparse, logging

import netanalysis.traffic.data.api_repository as gapi
from netanalysis.traffic.data import model

# Hard-coded by hand because pyipmeta doesn't give us easy
# access to the mappings and they're pretty unlikely to
# change too often.
#
# There may possibly be some errors due to typos or Shane's
# geography being not as good as he thought, so be prepared
# for some initial weirdness...
CONTINENT_MAP = {
    "AD": "EU", "AE": "AS", "AF": "AS", "AG": "NA", "AI": "NA",
    "AL": "EU", "AM": "AS", "AO": "AF", "AQ": "AN", "AR": "SA",
    "AT": "EU", "AU": "OC", "AW": "NA", "AX": "EU", "AZ": "AS",
    "BA": "EU", "BB": "NA", "BD": "AS", "BE": "EU", "BF": "AF",
    "BG": "EU", "BH": "AS", "BI": "AF", "BJ": "AF", "BM": "NA",
    "BN": "AS", "BO": "SA", "BR": "SA", "BS": "NA", "BT": "AS",
    "BW": "AF", "BY": "EU", "BZ": "NA", "CA": "NA", "CD": "AF",
    "CF": "AF", "CG": "AF", "CH": "EU", "CI": "AF", "CK": "OC",
    "CL": "SA", "CM": "AF", "CN": "AS", "CO": "SA", "CR": "NA",
    "CU": "NA", "CV": "AF", "CY": "EU", "CZ": "EU", "DE": "EU",
    "DJ": "AF", "DK": "EU", "DM": "NA", "DO": "NA", "DZ": "AF",
    "EC": "SA", "EE": "EU", "EG": "AF", "EH": "AF", "ER": "AF",
    "ES": "EU", "ET": "AF", "FI": "EU", "FJ": "OC", "FM": "OC",
    "FO": "EU", "FR": "EU", "GA": "AF", "GB": "EU", "GD": "NA",
    "GE": "EU", "GF": "SA", "GG": "EU", "GH": "AF", "GI": "EU",
    "GL": "NA", "GM": "AF", "GN": "AF", "GP": "NA", "GQ": "AF",
    "GR": "EU", "GT": "NA", "GU": "OC", "GW": "AF", "GY": "SA",
    "HK": "AS", "HN": "NA", "HR": "EU", "HT": "NA", "HU": "EU",
    "ID": "AS", "IE": "EU", "IL": "AS", "IM": "EU", "IN": "AS",
    "IQ": "AS", "IR": "AS", "IS": "EU", "IT": "EU", "JE": "EU",
    "JM": "NA", "JO": "AS", "JP": "AS", "KE": "AF", "KG": "AS",
    "KH": "AS", "KI": "OC", "KM": "AF", "KN": "NA", "KW": "AS",
    "KY": "NA", "KZ": "AS", "LA": "AS", "LB": "AS", "LC": "NA",
    "LI": "EU", "LK": "AS", "LR": "AF", "LS": "AF", "LT": "EU",
    "LU": "EU", "LV": "EU", "LY": "AF", "MA": "AF", "MC": "EU",
    "MD": "EU", "ME": "EU", "MG": "AF", "MH": "OC", "MK": "EU",
    "ML": "AF", "MM": "AS", "MN": "AS", "MO": "AS", "MP": "OC",
    "MQ": "NA", "MR": "AF", "MS": "NA", "MT": "EU", "MU": "AF",
    "MV": "AS", "MW": "AF", "MX": "NA", "MY": "AS", "MZ": "AF",
    "NA": "AF", "NC": "OC", "NE": "AF", "NF": "OC", "NG": "AF",
    "NI": "NA", "NL": "EU", "NO": "EU", "NP": "AS", "NR": "OC",
    "NU": "OC", "NZ": "OC", "OM": "AS", "PA": "NA", "PE": "SA",
    "PF": "OC", "PG": "OC", "PH": "AS", "PK": "AS", "PL": "EU",
    "PM": "NA", "PN": "OC", "PR": "NA", "PS": "AS", "PT": "EU",
    "PW": "OC", "PY": "SA", "QA": "AS", "RE": "AF", "KR": "AS",
    "KP": "AS", "VG": "NA", "SH": "AF", "RO": "EU", "RS": "EU",
    "RU": "EU", "RW": "AF", "SA": "AS", "SB": "OC", "SC": "AF",
    "SD": "AF", "SE": "EU", "SG": "AS", "SI": "EU", "SK": "EU",
    "SL": "AF", "SM": "EU", "SN": "AF", "SO": "AF", "SR": "SA",
    "SS": "AF", "ST": "AF", "SV": "NA", "SY": "AS", "SZ": "AF",
    "TC": "NA", "TD": "AF", "TG": "AF", "TH": "AS", "TJ": "AS",
    "TK": "OC", "TL": "AS", "TM": "AS", "TN": "AF", "TO": "OC",
    "TR": "EU", "TT": "NA", "TV": "OC", "TW": "AS", "TZ": "AF",
    "UA": "EU", "UG": "AF", "US": "NA", "VI": "NA", "UY": "SA",
    "UZ": "AS", "VA": "EU", "VC": "NA", "VE": "SA", "VN": "AS",
    "VU": "OC", "WS": "OC", "YE": "AS", "YT": "AF", "ZA": "AF",
    "ZM": "AF", "ZW": "AF",
}

BASEKEY="google_tr"

def filterProducts(args):
    """ Populates the list of products to query for, either by fetching
        all available products or extracting them from a user-specified
        list.

        Parameters:
          args -- an argument parser containing the command line arguments
                  for this program.

        Returns:
          a list of products that should be queried for by this script
    """

    if not args.products:
        # User has not requested specific products so give them all of
        # them except:
        #   unknown, because of obvious reasons
        #   all, because this has been deprecated long ago
        #   videos, because this also appears to not be used anymore
        products = [p for p in model.ProductId \
                if p.value != model.ProductId.UNKNOWN and \
                    p.value != model.ProductId.ALL and \
                    p.value != model.ProductId.VIDEOS ]
    else:
        # Otherwise, at least try to make sure that the user has listed
        # products that actually exist in the API
        products = []
        for ps in args.products.split(','):
            name = ps.strip().upper()
            if name in model.ProductId.__members__ and name not in \
                    ["ALL", "UNKNOWN", "VIDEOS"]:
                products.append(model.ProductId[name])
            else:
                logging.warning("Product name '%s' is not a valid product",
                        name)

    return products


def fetchData(trafrepo, start_time, end_time, productid, region, saved):
    """ Fetches traffic data from the Google Transparency API for a given
        product / region combination and saves it into a dictionary that is
        keyed by timestamp.

        Each dictionary item is a list of 2-tuples (key, value) where
        key is the dot-delimited string describing the series that will
        be passed into telegraf via kafka, and value is the traffic value
        fetched for the series at that timestamp.

        Parameters:
          trafrepo -- the APITrafficRepository that will be used to construct
                      and perform the REST API request for this query
          start_time -- the start of the time period to query for (as a
                        Datetime object)
          end_time -- the end of the time period to query for (as a
                        Datetime object)
          productid -- the identifier for the product to query for
          region -- the ISO 2-letter country code for the region to query
                    for
          saved -- the dictionary to save the fetched data into

        Returns:
         -1 on error, 0 if no data was available, 1 if successful
    """

    # IODA uses a "continent.country" format to hierarchically structure
    # geographic time series so we need to add the appropriate continent
    # for our requested region to the time series label.
    if region not in CONTINENT_MAP:
        logging.error("No continent mapping for %s?" % (region))
        contcode = "??"
    else:
        contcode = CONTINENT_MAP[region]

    # This is the key that we're going to write into kafka for this
    # region + product. They key must be encoded because pytimeseries
    # expects a bytes object for the key, not a string.
    key="%s.%s.%s.%s.traffic" % (BASEKEY, contcode, region, productid.name)
    key = key.encode()

    try:
        fetched = trafrepo.get_traffic(region, productid, start_time, end_time)
    except Exception as error:
        logging.warning("Failed to get traffic for %s.%s: %s", \
                region, productid.name, str(error))
        return -1

    if fetched.empty:
        return 0

    # pytimeseries works best if we write all datapoints for a given timestamp
    # in a single batch, so we will save our fetched data into a dictionary
    # keyed by timestamp. Once we've fetched everything, then we can walk
    # through that dictionary to emit the data in timestamp order.
    for k,v in fetched.items():
        ts = int(k.timestamp())

        if ts not in saved:
            saved[ts] = []

        # The traffic data is stored as a normalised float (with 10 d.p. of
        # precision -- we'd rather deal with integers so scale it up
        saved[ts].append((key, int(10000000000 * v)))

    return 1

def main(args):
    datadict = {}

    # Boiler-plate libtimeseries setup for a kafka output
    pyts = _pytimeseries.Timeseries()
    be = pyts.get_backend_by_name('kafka')
    if not be:
        logging.error('Unable to find pytimeseries kafka backend')
        return -1
    if not pyts.enable_backend(be, "-b %s -c %s -f ascii -p %s" % ( \
            args.broker, args.channel, args.topicprefix)):
        logging.error('Unable to initialise pytimeseries kafka backend')
        return -1

    kp = pyts.new_keypackage(reset=False, disable=True)
    # Boiler-plate ends

    # Get the regions and products available in the Google Transparency data
    trafrepo = gapi.ApiTrafficRepository()
    products = filterProducts(args)
    regions = trafrepo.list_regions()

    # Determine the start and end time periods for our upcoming query
    if args.endtime:
        endtime = datetime.datetime.fromtimestamp(args.endtime)
    else:
        endtime = datetime.datetime.now()

    if args.starttime:
        starttime = datetime.datetime.fromtimestamp(args.starttime)
    else:
        starttime = endtime;

    # Due to a bug in the netanalysis API, we must fetch at least one
    # days worth of data -- otherwise we will generate a 400 Bad Request.
    if (starttime > endtime or \
            endtime - starttime < datetime.timedelta(days=1)):
        starttime = endtime - datetime.timedelta(days=1)

    for p in products:
        for r in regions:
            ret = fetchData(trafrepo, starttime, endtime, p, r, datadict)

    for ts, dat in sorted(datadict.items()):
        # If our fetched time range was expanded out to a full day, now
        # is a good time for us to ignore any time periods that the user
        # didn't explicitly ask for
        if args.starttime and ts < args.starttime:
                continue

        # pytimeseries code to save each key and value for this timestamp
        for val in dat:
            idx = kp.get_key(val[0])
            if idx is None:
                idx = kp.add_key(val[0])
            else:
                kp.enable_key(idx)
            kp.set(idx, val[1])

        # Write to the kafka queue
        kp.flush(ts)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
            description='Continually fetches traffic data from the Google Transparency Report and writes it into kafka')

    parser.add_argument("--broker", type=str, required=True, help="The kafka broker to connect to")
    parser.add_argument("--channel", type=str, required=True, help="Kafka channel to write the data into")
    parser.add_argument("--topicprefix", type=str, required=True, help="Topic prefix to prepend to each Kafka message")
    parser.add_argument("--products", type=str, help="Comma-separated list of products to get data for")
    parser.add_argument("--starttime", type=int, help="Fetch traffic data starting from the given Unix timestamp")
    parser.add_argument("--endtime", type=int, help="Fetch traffic data up until the given Unix timestamp")

    args = parser.parse_args()

    main(args)

# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
