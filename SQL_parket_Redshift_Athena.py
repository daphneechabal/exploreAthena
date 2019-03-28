%pyspark
#Set schema for the CSV files
from pyspark.sql.types import *

region_s = StructType([
    StructField('r_regionkey', IntegerType(), True),
    StructField('r_name', StringType(), True),
    StructField('r_comment', StringType(), True)
    ])

nation_s = StructType([
    StructField('n_nationkey', IntegerType(), True),
    StructField('n_name', StringType(), True),
    StructField('n_regionkey', IntegerType(), True),
    StructField('n_comment', StringType(), True)
    ])

supplier_s = StructType([
    StructField('s_suppkey', IntegerType(), True),
    StructField('s_name', StringType(), True),
    StructField('s_address', StringType(), True),
    StructField('s_nationkey', IntegerType(), True),
    StructField('s_phone', StringType(), True),
    StructField('s_acctbal', FloatType(), True),
    StructField('s_comment', StringType(), True)
    ])

customer_s = StructType([
    StructField('c_custkey', IntegerType(), True),
    StructField('c_name', StringType(), True),
    StructField('c_address', StringType(), True),
    StructField('c_nationkey', IntegerType(), True),
    StructField('c_phone', StringType(), True),
    StructField('c_acctbal', FloatType(), True),
    StructField('c_mktsegment', StringType(), True),
    StructField('c_comment', StringType(), True)
    ])

part_s = StructType([
    StructField('p_partkey', IntegerType(), True),
    StructField('p_name', StringType(), True),
    StructField('p_mfgr', StringType(), True),
    StructField('p_brand', StringType(), True),
    StructField('p_type', StringType(), True),
    StructField('p_size', IntegerType(), True),
    StructField('p_container', StringType(), True),
    StructField('p_retailprice', FloatType(), True),
    StructField('p_comment', StringType(), True)
    ])


partsupp_s = StructType([
    StructField('ps_partkey', IntegerType(), True),
    StructField('ps_suppkey', IntegerType(), True),
    StructField('ps_availqty', IntegerType(), True),
    StructField('ps_supplycost', FloatType(), True),
    StructField('ps_comment', StringType(), True)
    ])

orders_s = StructType([
    StructField('o_orderkey', IntegerType(), True),
    StructField('o_custkey', IntegerType(), True),
    StructField('o_orderstatus', StringType(), True),
    StructField('o_totalprice', FloatType(), True),
    StructField('o_orderdate', DateType(), True),
    StructField('o_orderpriority', StringType(), True),
    StructField('o_clerk', StringType(), True),
    StructField('o_shippriority', IntegerType(), True),
    StructField('o_comment', StringType(), True)
    ])

lineitem_s = StructType([
    StructField('l_orderkey', IntegerType(), True),
    StructField('l_partkey', IntegerType(), True),
    StructField('l_suppkey', IntegerType(), True),
    StructField('l_linenumber', IntegerType(), True),
    StructField('l_quantity', IntegerType(), True),
    StructField('l_extendedprice', FloatType(), True),
    StructField('l_discount', FloatType(), True),
    StructField('l_tax', FloatType(), True),
    StructField('l_returnflag', StringType(), True),
    StructField('l_linestatus', StringType(), True),
    StructField('l_shipdate', DateType(), True),
    StructField('l_commitdate', DateType(), True),
    StructField('l_receiptdate', DateType(), True),
    StructField('l_shipinstruct', StringType(), True),
    StructField('l_shipmode', StringType(), True),
    StructField('l_comment', StringType(), True)
    ])

#Convert CSV's to Parquet files
data = "s3://bigdatacourse2019/tpch_csv/SF10/"
S3 = "s3://group19-lab5/"

spark.read.csv(data + "region", sep='|', schema=region_s).write.parquet(S3 + "region")
spark.read.csv(data + "nation", sep='|', schema=nation_s).write.parquet(S3 + "nation")
spark.read.csv(data + "supplier", sep='|', schema=supplier_s).write.parquet(S3 + "supplier")
spark.read.csv(data + "customer", sep='|', schema=customer_s).write.parquet(S3 + "customer")
spark.read.csv(data + "part", sep='|', schema=part_s).write.parquet(S3 + "part")
spark.read.csv(data + "partsupp", sep='|', schema=partsupp_s).write.parquet(S3 + "partsupp")
spark.read.csv(data + "orders", sep='|', schema=orders_s).write.parquet(S3 + "orders")
spark.read.csv(data + "lineitem", sep='|', schema=lineitem_s).write.parquet(S3 + "lineitem")
