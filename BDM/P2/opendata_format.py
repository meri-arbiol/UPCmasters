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



if __name__ == '__main__':

    collections = ['income', 'preu', 'income_lookup_district', 'income_lookup_neighborhood', 'rent_lookup_district', 'rent_lookup_neighborhood']

    incomeRDD = loadMongoRDD(collections[0]).cache()
    preuRDD = loadMongoRDD(collections[1]).cache()
    lookup_income_district_RDD = loadMongoRDD(collections[2]).cache().map(lambda x: (x['district'], x['district_reconciled'])).cache()
    lookup_income_neighborhood_RDD = loadMongoRDD(collections[3]).map(lambda x: (x['neighborhood'], x['neighborhood_reconciled'])).cache()
    lookup_rent_district_RDD = loadMongoRDD(collections[4]).cache().map(lambda x: (x['di'], x['di_re'])).cache()
    lookup_rent_neighborhood_RDD = loadMongoRDD(collections[5]).map(lambda x: (x['ne'], x['ne_re'])).cache()

    def mean(x, var):
        suma = 0
        num = len(x)
        for i in range(0,num):
            suma = suma + x[i][var]
        mean = suma/num
        return mean


    def unroll(x: Tuple[str, Tuple[float, str]]):
        (_, (price, ne_re)) = x
        return (ne_re, price)

    # mean of RFD (family income index) and mean of population per neighborhood (key)
    rdd1 = incomeRDD \
        .map(lambda x: (x['neigh_name '], (mean(x['info'], 'RFD'), mean(x['info'], 'pop')))) \
        .join(lookup_income_neighborhood_RDD) \
        .map(unroll) \
        .cache()

    print('####################')
    print('****** RDD1 ******')
    rdd1.foreach(lambda r: print(r))

    # rdd2_sup = preuRDD \
    #     .filter(lambda x: 'superfície' in x['Lloguer_mitja']) \
    #     .map(lambda x: ()) \
    #     .cache()
    # rdd2_men = preuRDD \
    #     .filter(lambda x: 'mensual' in x['Lloguer_mitja']) \
    #     .cache()


    # print('####################')
    # print('****** RDD2 ******')
    # rdd2.foreach(lambda r: print(r))
