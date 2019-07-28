from pyspark import SparkContext
from pyspark.sql import SQLContext

sc = SparkContext("local", "bdm_assignment")
sqlCntxt = SQLContext(sc)


from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col

from pyspark.sql.functions import when, lit

df_old = sqlCntxt.read.option("header", "true") \
    .option("delimiter", ",") \
    .option("inferSchema", "true") \
    .csv("file:///E:/Residency2/BDM/price_paid_records.csv")\
    .withColumnRenamed('Transaction unique identifier', 'TransID')\
    .withColumnRenamed('Date of Transfer', 'DateOfTrans')\
    .withColumnRenamed('Property Type','PropType')\
    .withColumnRenamed('Old/New','Old_Property')\
    .withColumnRenamed('Town/City', 'Town')\
    .withColumnRenamed('PPDCategory Type','PPDCatType')\
    .withColumnRenamed('Record Status - monthly file only','RecStatus')
#df_hPrice.cache()
#df_hPrice.show(10, truncate=False)
#df_old.registerTempTable("df_old")

from pyspark.ml.feature import StringIndexer

indexer = StringIndexer(inputCol="Old_Property", outputCol="propIndex")
indexed_df = indexer.fit(df_old).transform(df_old)
indexed_df.distinct("propIndex").show()
