#Load data - SF 1
%pyspark
region  = sqlContext.read.format("com.databricks.spark.csv").option("header", "false").option("delimiter", "|").load("s3://bigdatacourse2019/tpch_csv/SF1/region/").withColumnRenamed("_c0", "r_regionkey").withColumnRenamed("_c1", "r_name").withColumnRenamed("_c2", "r_comment").drop('_c3')
nation = sqlContext.read.format("com.databricks.spark.csv").option("header", "false").option("delimiter", "|").load("s3://bigdatacourse2019/tpch_csv/SF1/nation/").withColumnRenamed("_c0", "n_nationkey").withColumnRenamed("_c1", "n_name").withColumnRenamed("_c2", "n_regionkey").withColumnRenamed("_c3", "n_comment").drop('_c4')
supplier = sqlContext.read.format("com.databricks.spark.csv").option("header", "false").option("delimiter", "|").load("s3://bigdatacourse2019/tpch_csv/SF1/supplier/").withColumnRenamed("_c0", "s_suppkey").withColumnRenamed("_c1", "s_name").withColumnRenamed("_c2", "s_address").withColumnRenamed("_c3", "s_nationkey").withColumnRenamed("_c4", "s_phone").withColumnRenamed("_c5", "s_acctbal").withColumnRenamed("_c6", "s_comment").drop('_c7')
customer = sqlContext.read.format("com.databricks.spark.csv").option("header", "false").option("delimiter", "|").load("s3://bigdatacourse2019/tpch_csv/SF1/customer/").withColumnRenamed("_c0", "c_custkey").withColumnRenamed("_c1", "c_name").withColumnRenamed("_c2", "c_address").withColumnRenamed("_c3", "c_nationkey").withColumnRenamed("_c4", "c_phone").withColumnRenamed("_c5", "c_acctbal").withColumnRenamed("_c6", "c_mktsegment").withColumnRenamed("_c7", "c_comment").drop('_c8')
part = sqlContext.read.format("com.databricks.spark.csv").option("header", "false").option("delimiter", "|").load("s3://bigdatacourse2019/tpch_csv/SF1/part/").withColumnRenamed("_c0", "p_partkey").withColumnRenamed("_c1", "p_name").withColumnRenamed("_c2", "p_mfgr").withColumnRenamed("_c3", "p_brand").withColumnRenamed("_c4", "p_type").withColumnRenamed("_c5", "p_size").withColumnRenamed("_c6", "p_container").withColumnRenamed("_c7", "p_retailprice").withColumnRenamed("_c8", "p_comment").drop('_c9')
partsupp = sqlContext.read.format("com.databricks.spark.csv").option("header", "false").option("delimiter", "|").load("s3://bigdatacourse2019/tpch_csv/SF1/partsupp/").withColumnRenamed("_c0", "ps_partkey").withColumnRenamed("_c1", "ps_suppkey").withColumnRenamed("_c2", "ps_availqty").withColumnRenamed("_c3", "ps_supplycost").withColumnRenamed("_c4", "ps_comment").drop('_c5')
orders  = sqlContext.read.format("com.databricks.spark.csv").option("header", "false").option("delimiter", "|").load("s3://bigdatacourse2019/tpch_csv/SF1/orders/").withColumnRenamed("_c0", "o_orderkey").withColumnRenamed("_c1", "o_custkey").withColumnRenamed("_c2", "o_orderstatus").withColumnRenamed("_c3", "o_totalprice").withColumnRenamed("_c4", "o_orderdate").withColumnRenamed("_c5", "o_orderpriority").withColumnRenamed("_c6", "o_clerk").withColumnRenamed("_c7", "o_shippriority").withColumnRenamed("_c8", "o_comment").drop('_c9')
lineitem  = sqlContext.read.format("com.databricks.spark.csv").option("header", "false").option("delimiter", "|").load("s3://bigdatacourse2019/tpch_csv/SF1/lineitem/").withColumnRenamed("_c0", "l_orderkey").withColumnRenamed("_c1", "l_partkey").withColumnRenamed("_c2", "l_suppkey").withColumnRenamed("_c3", "l_linenumber").withColumnRenamed("_c4", "l_quantity").withColumnRenamed("_c5", "l_extendedprice").withColumnRenamed("_c6", "l_discount").withColumnRenamed("_c7", "l_tax").withColumnRenamed("_c8", "l_returnflag").withColumnRenamed("_c9", "l_linestatus").withColumnRenamed("_c10", "l_shipdate").withColumnRenamed("_c11", "l_commitdate").withColumnRenamed("_c12", "l_receiptdate").withColumnRenamed("_c13", "l_shipinstruct").withColumnRenamed("_c14", "l_shipmode").withColumnRenamed("_c15", "l_comment").drop('_c16')

#registerTempTable
%pyspark
region.registerTempTable('region')
nation.registerTempTable('nation')
supplier.registerTempTable('supplier')
customer.registerTempTable('customer')
partsupp.registerTempTable('partsupp')
orders.registerTempTable('orders')
lineitem.registerTempTable('lineitem')

#query one
%pyspark
query = "SELECT lineitem.l_returnflag, lineitem.l_linestatus, SUM(lineitem.l_quantity) AS sum_qty, SUM(lineitem.l_extendedprice) AS sum_base_price, SUM(lineitem.l_extendedprice * (1 - lineitem.l_discount)) AS sum_disc_price, SUM(lineitem.l_extendedprice * (1 - lineitem.l_discount) * (1 + lineitem.l_tax)) AS sum_charge, AVG(lineitem.l_quantity) AS avg_qty, AVG(lineitem.l_extendedprice) AS avg_price, AVG(lineitem.l_discount) AS avg_disc, COUNT(*) AS count_order FROM lineitem WHERE lineitem.l_shipdate <= date '1998-12-01' - interval '108' day GROUP BY lineitem.l_returnflag, lineitem.l_linestatus ORDER BY lineitem.l_returnflag, lineitem.l_linestatus"

output_1 = sqlContext.sql(query)
output_1.take(10)

#query five
query_5 = "SELECT nation.n_name, sum(lineitem.l_extendedprice * (1 - lineitem.l_discount)) as revenue FROM customer, orders, lineitem, supplier, nation, region WHERE customer.c_custkey = orders.o_custkey AND lineitem.l_orderkey = orders.o_orderkey AND lineitem.l_suppkey = supplier.s_suppkey AND customer.c_nationkey = supplier.s_nationkey AND supplier.s_nationkey = nation.n_nationkey AND nation.n_regionkey = region.r_regionkey AND region.r_name = 'MIDDLE EAST' GROUP BY nation.n_name ORDER BY revenue DESC"

output_5 = sqlContext.sql(query_5)
output_5.take(10)


#Load data SF 10
region  = sqlContext.read.format("com.databricks.spark.csv").option("header", "false").option("delimiter", "|").load("s3://bigdatacourse2019/tpch_csv/SF10/region/").withColumnRenamed("_c0", "r_regionkey").withColumnRenamed("_c1", "r_name").withColumnRenamed("_c2", "r_comment").drop('_c3')
nation = sqlContext.read.format("com.databricks.spark.csv").option("header", "false").option("delimiter", "|").load("s3://bigdatacourse2019/tpch_csv/SF10/nation/").withColumnRenamed("_c0", "n_nationkey").withColumnRenamed("_c1", "n_name").withColumnRenamed("_c2", "n_regionkey").withColumnRenamed("_c3", "n_comment").drop('_c4')
supplier = sqlContext.read.format("com.databricks.spark.csv").option("header", "false").option("delimiter", "|").load("s3://bigdatacourse2019/tpch_csv/SF10/supplier/").withColumnRenamed("_c0", "s_suppkey").withColumnRenamed("_c1", "s_name").withColumnRenamed("_c2", "s_address").withColumnRenamed("_c3", "s_nationkey").withColumnRenamed("_c4", "s_phone").withColumnRenamed("_c5", "s_acctbal").withColumnRenamed("_c6", "s_comment").drop('_c7')
customer = sqlContext.read.format("com.databricks.spark.csv").option("header", "false").option("delimiter", "|").load("s3://bigdatacourse2019/tpch_csv/SF10/customer/").withColumnRenamed("_c0", "c_custkey").withColumnRenamed("_c1", "c_name").withColumnRenamed("_c2", "c_address").withColumnRenamed("_c3", "c_nationkey").withColumnRenamed("_c4", "c_phone").withColumnRenamed("_c5", "c_acctbal").withColumnRenamed("_c6", "c_mktsegment").withColumnRenamed("_c7", "c_comment").drop('_c8')
part = sqlContext.read.format("com.databricks.spark.csv").option("header", "false").option("delimiter", "|").load("s3://bigdatacourse2019/tpch_csv/SF10/part/").withColumnRenamed("_c0", "p_partkey").withColumnRenamed("_c1", "p_name").withColumnRenamed("_c2", "p_mfgr").withColumnRenamed("_c3", "p_brand").withColumnRenamed("_c4", "p_type").withColumnRenamed("_c5", "p_size").withColumnRenamed("_c6", "p_container").withColumnRenamed("_c7", "p_retailprice").withColumnRenamed("_c8", "p_comment").drop('_c9')
partsupp = sqlContext.read.format("com.databricks.spark.csv").option("header", "false").option("delimiter", "|").load("s3://bigdatacourse2019/tpch_csv/SF10/partsupp/").withColumnRenamed("_c0", "ps_partkey").withColumnRenamed("_c1", "ps_suppkey").withColumnRenamed("_c2", "ps_availqty").withColumnRenamed("_c3", "ps_supplycost").withColumnRenamed("_c4", "ps_comment").drop('_c5')
orders  = sqlContext.read.format("com.databricks.spark.csv").option("header", "false").option("delimiter", "|").load("s3://bigdatacourse2019/tpch_csv/SF10/orders/").withColumnRenamed("_c0", "o_orderkey").withColumnRenamed("_c1", "o_custkey").withColumnRenamed("_c2", "o_orderstatus").withColumnRenamed("_c3", "o_totalprice").withColumnRenamed("_c4", "o_orderdate").withColumnRenamed("_c5", "o_orderpriority").withColumnRenamed("_c6", "o_clerk").withColumnRenamed("_c7", "o_shippriority").withColumnRenamed("_c8", "o_comment").drop('_c9')
lineitem  = sqlContext.read.format("com.databricks.spark.csv").option("header", "false").option("delimiter", "|").load("s3://bigdatacourse2019/tpch_csv/SF10/lineitem/").withColumnRenamed("_c0", "l_orderkey").withColumnRenamed("_c1", "l_partkey").withColumnRenamed("_c2", "l_suppkey").withColumnRenamed("_c3", "l_linenumber").withColumnRenamed("_c4", "l_quantity").withColumnRenamed("_c5", "l_extendedprice").withColumnRenamed("_c6", "l_discount").withColumnRenamed("_c7", "l_tax").withColumnRenamed("_c8", "l_returnflag").withColumnRenamed("_c9", "l_linestatus").withColumnRenamed("_c10", "l_shipdate").withColumnRenamed("_c11", "l_commitdate").withColumnRenamed("_c12", "l_receiptdate").withColumnRenamed("_c13", "l_shipinstruct").withColumnRenamed("_c14", "l_shipmode").withColumnRenamed("_c15", "l_comment").drop('_c16')

#registerTempTable
%pyspark
region.registerTempTable('region')
nation.registerTempTable('nation')
supplier.registerTempTable('supplier')
customer.registerTempTable('customer')
partsupp.registerTempTable('partsupp')
orders.registerTempTable('orders')
lineitem.registerTempTable('lineitem')

#query one
%pyspark
query = "SELECT lineitem.l_returnflag, lineitem.l_linestatus, SUM(lineitem.l_quantity) AS sum_qty, SUM(lineitem.l_extendedprice) AS sum_base_price, SUM(lineitem.l_extendedprice * (1 - lineitem.l_discount)) AS sum_disc_price, SUM(lineitem.l_extendedprice * (1 - lineitem.l_discount) * (1 + lineitem.l_tax)) AS sum_charge, AVG(lineitem.l_quantity) AS avg_qty, AVG(lineitem.l_extendedprice) AS avg_price, AVG(lineitem.l_discount) AS avg_disc, COUNT(*) AS count_order FROM lineitem WHERE lineitem.l_shipdate <= date '1998-12-01' - interval '108' day GROUP BY lineitem.l_returnflag, lineitem.l_linestatus ORDER BY lineitem.l_returnflag, lineitem.l_linestatus"

output_1 = sqlContext.sql(query)
output_1.take(10)

#query five
query_5 = "SELECT nation.n_name, sum(lineitem.l_extendedprice * (1 - lineitem.l_discount)) as revenue FROM customer, orders, lineitem, supplier, nation, region WHERE customer.c_custkey = orders.o_custkey AND lineitem.l_orderkey = orders.o_orderkey AND lineitem.l_suppkey = supplier.s_suppkey AND customer.c_nationkey = supplier.s_nationkey AND supplier.s_nationkey = nation.n_nationkey AND nation.n_regionkey = region.r_regionkey AND region.r_name = 'MIDDLE EAST' GROUP BY nation.n_name ORDER BY revenue DESC"

output_5 = sqlContext.sql(query_5)
output_5.take(10)
