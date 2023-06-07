"""Generates a stream to Kafka from a time series csv file.
"""

import argparse
import csv
import json
import sys
import time
from dateutil.parser import parse
from confluent_kafka import Producer
import socket


def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg.value()), str(err)))
    else:
        print("Message produced: %s" % (str(msg.value())))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('filename', type=str,
                        help='Time series csv file.')
    parser.add_argument('topic', type=str,
                        help='Name of the Kafka topic to stream.')
    parser.add_argument('--speed', type=float, default=1, required=False,
                        help='Speed up time series by a given multiplicative factor.')
    args = parser.parse_args()

    topic = args.topic
    p_key = args.filename

    conf = {'bootstrap.servers': "broker:9092",
            'client.id': socket.gethostname()}
    producer = Producer(conf)

    rdr = csv.reader(open(args.filename))
    next(rdr)  # Skip header
    firstline = True

    while True:

        try:

            if firstline is True:
                line1 = next(rdr, None)
                ID = int(line1[0])
                Year_Birth = int(line1[1])
                Education = str(line1[2])
                Marital_Status = str(line1[3])
                Income = int(line1[4])
                Kidhome = int(line1[5])
                Teenhome = int(line1[6])
                Dt_Customer = str(line1[7])
                Recency = int(line1[8])
                MntWines = int(line1[9])
                MntFruits = int(line1[10])
                MntMeatProducts = int(line1[11])
                MntFishProducts = int(line1[12])
                MntSweetProducts = int(line1[13])
                MntGoldProds = int(line1[14])
                NumDealsPurchases = int(line1[15])
                NumWebPurchases = int(line1[16])
                NumCatalogPurchases = int(line1[17])
                NumStorePurchases = int(line1[18])
                NumWebVisitsMonth = int(line1[19])
                AcceptedCmp3 = int(line1[20])
                AcceptedCmp4 = int(line1[21])
                AcceptedCmp5 = int(line1[22])
                AcceptedCmp1 = int(line1[23])
                AcceptedCmp2 = int(line1[24])
                Complain = int(line1[25])
                Z_CostContact = int(line1[26])
                Z_Revenue = int(line1[27])
                Response = int(line1[28])
                # Convert csv columns to key value pair
                result = {}
                result["ID"] = ID
                result["Year_Birth"] = Year_Birth
                result["Education"] = Education
                result["Marital_Status"] = Marital_Status
                result["Income"] = Income
                result["Kidhome"] = Kidhome
                result["Teenhome"] = Teenhome
                result["Dt_Customer"] = Dt_Customer
                result["Recency"] = Recency
                result["MntWines"] = MntWines
                result["MntFruits"] = MntFruits
                result["MntMeatProducts"] = MntMeatProducts
                result["MntFishProducts"] = MntFishProducts
                result["MntSweetProducts"] = MntSweetProducts
                result["MntGoldProds"] = MntGoldProds
                result["NumDealsPurchases"] = NumDealsPurchases
                result["NumWebPurchases"] = NumWebPurchases
                result["NumCatalogPurchases"] = NumCatalogPurchases
                result["NumStorePurchases"] = NumStorePurchases
                result["NumWebVisitsMonth"] = NumWebVisitsMonth
                result["AcceptedCmp3"] = AcceptedCmp3
                result["AcceptedCmp4"] = AcceptedCmp4
                result["AcceptedCmp5"] = AcceptedCmp5
                result["AcceptedCmp1"] = AcceptedCmp1
                result["AcceptedCmp2"] = AcceptedCmp2
                result["Complain"] = Complain
                result["Z_CostContact"] = Z_CostContact
                result["Z_Revenue"] = Z_Revenue
                result["Response"] = Response
                # Convert dict to json as message format
                jresult = json.dumps(result)
                firstline = False

                producer.produce(topic, key=p_key, value=jresult, callback=acked)

            else:
                line = next(rdr, None)
                #d1 = parse(timestamp)
                #d2 = parse(line[0])
                #diff = ((d2 - d1).total_seconds())/args.speed
                time.sleep(3)
                ID = int(line1[0])
                Year_Birth = int(line1[1])
                Education = str(line1[2])
                Marital_Status = str(line1[3])
                Income = int(line1[4])
                Kidhome = int(line1[5])
                Teenhome = int(line1[6])
                Dt_Customer = str(line1[7])
                Recency = int(line1[8])
                MntWines = int(line1[9])
                MntFruits = int(line1[10])
                MntMeatProducts = int(line1[11])
                MntFishProducts = int(line1[12])
                MntSweetProducts = int(line1[13])
                MntGoldProds = int(line1[14])
                NumDealsPurchases = int(line1[15])
                NumWebPurchases = int(line1[16])
                NumCatalogPurchases = int(line1[17])
                NumStorePurchases = int(line1[18])
                NumWebVisitsMonth = int(line1[19])
                AcceptedCmp3 = int(line1[20])
                AcceptedCmp4 = int(line1[21])
                AcceptedCmp5 = int(line1[22])
                AcceptedCmp1 = int(line1[23])
                AcceptedCmp2 = int(line1[24])
                Complain = int(line1[25])
                Z_CostContact = int(line1[26])
                Z_Revenue = int(line1[27])
                Response = int(line1[28])
                result = {}
                result["ID"] = ID
                result["Year_Birth"] = Year_Birth
                result["Education"] = Education
                result["Marital_Status"] = Marital_Status
                result["Income"] = Income
                result["Kidhome"] = Kidhome
                result["Teenhome"] = Teenhome
                result["Dt_Customer"] = Dt_Customer
                result["Recency"] = Recency
                result["MntWines"] = MntWines
                result["MntFruits"] = MntFruits
                result["MntMeatProducts"] = MntMeatProducts
                result["MntFishProducts"] = MntFishProducts
                result["MntSweetProducts"] = MntSweetProducts
                result["MntGoldProds"] = MntGoldProds
                result["NumDealsPurchases"] = NumDealsPurchases
                result["NumWebPurchases"] = NumWebPurchases
                result["NumCatalogPurchases"] = NumCatalogPurchases
                result["NumStorePurchases"] = NumStorePurchases
                result["NumWebVisitsMonth"] = NumWebVisitsMonth
                result["AcceptedCmp3"] = AcceptedCmp3
                result["AcceptedCmp4"] = AcceptedCmp4
                result["AcceptedCmp5"] = AcceptedCmp5
                result["AcceptedCmp1"] = AcceptedCmp1
                result["AcceptedCmp2"] = AcceptedCmp2
                result["Complain"] = Complain
                result["Z_CostContact"] = Z_CostContact
                result["Z_Revenue"] = Z_Revenue
                result["Response"] = Response
                jresult = json.dumps(result)

                producer.produce(topic, key=p_key, value=jresult, callback=acked)

            producer.flush()

        except TypeError:
            sys.exit()


if __name__ == "__main__":
    main()
