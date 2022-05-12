from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col


def convert_data(data):
    data = (data
            .select("_id", "district_id", "district_name", "neigh_name ",explode("info").alias("new_info"))
            .select("_id", "district_id", "district_name", "neigh_name ", "new_info.*")
            .withColumnRenamed("neigh_name ", "neigh_name"))
    return(data)


def clean(data):
    # removing null values
    columns = data.columns
    for column in columns:
        data = data.filter(col(column).isNotNull())
    return(data)


def duplicates(data):
    # removing duplicate values
    data = data.dropDuplicates()
    return(data)


if __name__ == '__main__':

    spark = SparkSession.builder.master("local[*]").appName("Learning_Spark").getOrCreate()
    path = "landing/persistent/data_solution/income_opendata/income_opendata_neighborhood.json"
    data = spark.read.json(path)

    ds = convert_data(data)
    ds.show()

    ds = clean(ds)
    ds = duplicates(ds)
    ds.show()

    ds.write.csv("formatted/income_opendata_neighborhood.csv")
