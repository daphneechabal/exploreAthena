%pyspark
SF1 = "s3://bigdatacourse2019/tpch_parquet/SF1/"
SF10 = "s3://group19-lab5/"
dataset = SF1

#load data
region = spark.read.parquet(dataset + 'region/').drop('skip')
nation = spark.read.parquet(dataset + 'nation/').drop('skip')
supplier = spark.read.parquet(dataset + 'supplier/').drop('skip')
customer = spark.read.parquet(dataset + 'customer/').drop('skip')
part = spark.read.parquet(dataset + 'part/').drop('skip')
partsupp = spark.read.parquet(dataset + 'partsupp/').drop('skip')
orders = spark.read.parquet(dataset + 'orders/').drop('skip')
lineitem = spark.read.parquet(dataset + 'lineitem/').drop('skip')

#create temp tables
region.registerTempTable('region')
nation.registerTempTable('nation')
supplier.registerTempTable('supplier')
customer.registerTempTable('customer')
part.registerTempTable('part')
partsupp.registerTempTable('partsupp')
orders.registerTempTable('orders')
lineitem.registerTempTable('lineitem')

#Query 1
query = "SELECT lineitem.l_returnflag, lineitem.l_linestatus, SUM(lineitem.l_quantity) AS sum_qty, SUM(lineitem.l_extendedprice) AS sum_base_price, SUM(lineitem.l_extendedprice * (1 - lineitem.l_discount)) AS sum_disc_price, SUM(lineitem.l_extendedprice * (1 - lineitem.l_discount) * (1 + lineitem.l_tax)) AS sum_charge, AVG(lineitem.l_quantity) AS avg_qty, AVG(lineitem.l_extendedprice) AS avg_price, AVG(lineitem.l_discount) AS avg_disc, COUNT(*) AS count_order FROM lineitem WHERE lineitem.l_shipdate <= date '1998-12-01' - interval '108' day GROUP BY lineitem.l_returnflag, lineitem.l_linestatus ORDER BY lineitem.l_returnflag, lineitem.l_linestatus"

output_1 = sqlContext.sql(query)
output_1.take(10)

#query five
query_5 = "SELECT nation.n_name, sum(lineitem.l_extendedprice * (1 - lineitem.l_discount)) as revenue FROM customer, orders, lineitem, supplier, nation, region WHERE customer.c_custkey = orders.o_custkey AND lineitem.l_orderkey = orders.o_orderkey AND lineitem.l_suppkey = supplier.s_suppkey AND customer.c_nationkey = supplier.s_nationkey AND supplier.s_nationkey = nation.n_nationkey AND nation.n_regionkey = region.r_regionkey AND region.r_name = 'MIDDLE EAST' GROUP BY nation.n_name ORDER BY revenue DESC"

output_5 = sqlContext.sql(query_5)
output_5.take(10)
