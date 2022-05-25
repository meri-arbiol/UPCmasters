import operator

from pyspark import RDD
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.functions import explode, col
from typing import Tuple


def loadMongoRDD(collection: str):
    spark = SparkSession \
        .builder \
        .master(f"local[*]") \
        .appName("myApp") \
        .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
        .getOrCreate()

    dataRDD = spark.read.format("mongo") \
        .option('uri', f"mongodb://10.4.41.48/opendata.{collection}") \
        .load() \
        .rdd

    return dataRDD


def mean(x, var):
    suma = 0
    num = len(x)
    for i in range(0,num):
        suma = suma + x[i][var]
    mean = suma/num
    return mean


def mostrecent(x, var):
    x.sort(reverse=True, key=lambda x: x['year'])
    return x[0][var]


def increase(x, var):
    x.sort(reverse=True, key=lambda x: x['year'])
    diff = x[0][var] - x[1][var] #difference between the var of the last year and the year before
    return float("{:.2f}".format(diff))


def unroll(x: Tuple[str, Tuple[float, str]]):
    (_, (price, ne_re)) = x
    return (ne_re, price)


if __name__ == '__main__':

    collections = ['income', 'preu', 'income_lookup_district', 'income_lookup_neighborhood', 'rent_lookup_district', 'rent_lookup_neighborhood']

    incomeRDD = loadMongoRDD(collections[0]).cache()
    preuRDD = loadMongoRDD(collections[1]).cache()
    lookup_income_neighborhood_RDD = loadMongoRDD(collections[3]).map(lambda x: (x['neighborhood'], x['neighborhood_reconciled'])).cache()
    #lookup_rent_neighborhood_RDD = loadMongoRDD(collections[5]).map(lambda x: (x['ne'], x['ne_re'])).cache()


    #mean of RFD (family income index) and mean of population per neighborhood (key)
    rdd1 = incomeRDD \
        .map(lambda x: (x['neigh_name '], (x['district_name'], mostrecent(x['info'], 'RFD'), increase(x['info'], 'RFD'), mostrecent(x['info'], 'pop'), increase(x['info'], 'pop')))) \
        .join(lookup_income_neighborhood_RDD) \
        .distinct() \
        .map(unroll) \
        .cache()

    # print('####################')
    # print('****** RDD1 ******')
    # rdd1.foreach(lambda r: print(r))


    preuRDD = preuRDD\
        .filter(lambda x: x['Preu'] != '--') \
        .filter(lambda x: x['Preu'] != None) \
        .filter(lambda x: 2020 <= x['Any']) \
        .cache()

    rdd2_sup = preuRDD \
        .filter(lambda x: 'superfície' in x['Lloguer_mitja']) \
        .map(lambda x: ((x['Nom_Barri'], x['Any'], x['Nom_Districte']), float(x['Preu']))) \
        .mapValues(lambda x: (x, 1)) \
        .reduceByKey(lambda a,b: (a[0]+b[0],a[1]+b[1])) \
        .mapValues(lambda x: float("{:.2f}".format(x[0]/x[1])) ) \
        .distinct() \
        .cache()

    rdd2_men = preuRDD \
        .filter(lambda x: 'mensual' in x['Lloguer_mitja']) \
        .map(lambda x: ((x['Nom_Barri'], x['Any'], x['Nom_Districte']), float(x['Preu']))) \
        .mapValues(lambda x: (x, 1)) \
        .reduceByKey(lambda a,b: (a[0]+b[0],a[1]+b[1])) \
        .mapValues(lambda x: float("{:.2f}".format(x[0]/x[1])) ) \
        .distinct() \
        .cache()

    rdd2_join = rdd2_sup.join(rdd2_men) \
        .cache()

    rdd2_all = rdd2_join \
        .map(lambda x: (x[0][0], (x[0][2], x[0][1], x[1][0], x[1][1]))) \
        .cache()

    rdd2_join2 = rdd2_all\
        .filter(lambda x: 2021 == x[1][1]) \
        .join(rdd2_all.filter(lambda x: 2020 == x[1][1])) \
        .cache()

    # neigh (key), district, precio superficie ultimo año, precio mensual ultimo año, incremento precio superficie, incremento precio mensual
    rdd2 = rdd2_join2 \
        .map(lambda x: (x[0], (x[1][0][0], x[1][0][2], x[1][0][3], float("{:.2f}".format(x[1][0][2]-x[1][1][2])), float("{:.2f}".format(x[1][0][3]-x[1][1][3]))))) \
        .join(lookup_income_neighborhood_RDD) \
        .map(unroll) \
        .cache()

    rdd3 = rdd1 \
        .join(rdd2) \
        .map(lambda x: (x[0], (x[1][0][0], x[1][0][1], x[1][0][2], x[1][0][3], x[1][0][4], x[1][1][1], x[1][1][2], x[1][1][3], x[1][1][4]))) \
        .cache()

    print('####################')
    print('****** RDD3 ******')
    rdd3.foreach(lambda r: print(r))
    print(rdd3.count())
