# Databricks notebook source
# DBTITLE 1,Sales Dataframe
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

schema=StructType([
    StructField("product_id",IntegerType(),True),
    StructField("customer_id",StringType(),True),
    StructField("order_date",DateType(),True),
    StructField("location",StringType(),True),
    StructField("source_order",StringType(),True)
])

sales_df=spark.read.format("csv").option("inferschema","true").schema(schema).load("/FileStore/tables/sales_csv.txt")
display(sales_df)

# COMMAND ----------

# DBTITLE 1,deriving Year, Month, Quarter
from pyspark.sql.functions import month, year, quarter

sales_df=sales_df.withColumn("order_year",year(sales_df.order_date))
sales_df=sales_df.withColumn("order_month",month(sales_df.order_date))
sales_df=sales_df.withColumn("order_quarter",quarter(sales_df.order_date))
display(sales_df)

# COMMAND ----------

# DBTITLE 1,Menu dataframe
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

schema=StructType([
    StructField("product_id",IntegerType(),True),
    StructField("product_name",StringType(),True),
    StructField("price",StringType(),True)
])

menu_df=spark.read.format("csv").option("inferschema","true").schema(schema).load("/FileStore/tables/menu_csv.txt")
display(menu_df)

# COMMAND ----------

# DBTITLE 1,Total Amount spent by each customer
total_amount_spent = (sales_df.join(menu_df,'product_id').groupBy('customer_id').agg({'price':'sum'}).orderBy('customer_id'))
display(total_amount_spent)

# COMMAND ----------

# DBTITLE 1,Total Amount spent by each food category
total_amount_spent=(sales_df.join(menu_df,'Product_ID').groupBy('Product_name').agg({"Price":"sum"}).orderBy('Product_name'))
display(total_amount_spent)

# COMMAND ----------

# DBTITLE 1,Total Amount of sales in each month
total_amount_eac_month = (sales_df.join(menu_df,'product_id').groupBy('order_month').agg({'price':'sum'}).orderBy('order_month'))
display(total_amount_eac_month)

# COMMAND ----------

# DBTITLE 1,Yearly sales
total_amount_eac_yearly = (sales_df.join(menu_df,'product_id').groupBy('order_year').agg({'price':'sum'}).orderBy('order_year'))
display(total_amount_eac_yearly)

# COMMAND ----------

# DBTITLE 1,Quarterly sales
total_amount_eac_quarterly = (sales_df.join(menu_df,'product_id').groupBy('order_quarter').agg({'price':'sum'}).orderBy('order_quarter'))
display(total_amount_eac_quarterly)

# COMMAND ----------

# DBTITLE 1,Total number of orders by each category
from pyspark.sql.functions import count

each_product_parched = (sales_df.join(menu_df,'product_id').groupBy('product_id','product_name')
                        .agg(count('product_id').alias('product_count'))
                        .orderBy('product_count',ascending=0))

display(each_product_parched)

# COMMAND ----------

# DBTITLE 1,Top 5 ordered items
from pyspark.sql.functions import count

each_product_parched = (sales_df.join(menu_df,'product_id').groupBy('product_id','product_name')
                        .agg(count('product_id').alias('product_count'))
                        .orderBy('product_count',ascending=0)).drop('product_id').limit(5)

display(each_product_parched)

# COMMAND ----------

# DBTITLE 1,Frequency of customers visited 
from pyspark.sql.functions import countDistinct

df = (sales_df.filter(sales_df.source_order=='Restaurant').groupBy('customer_id').agg(countDistinct('order_date').alias('order_date')))
display(df)

# COMMAND ----------

# DBTITLE 1,Total sales by each country
each_country = (sales_df.join(menu_df,'product_id').groupBy('location').agg({'price':'sum'}).orderBy('location'))
display(each_country)

# COMMAND ----------

# DBTITLE 1,Total sales by order source
sales_by_source_order = (sales_df.join(menu_df,'product_id').groupBy('source_order').agg({'price':'sum'}))
display(sales_by_source_order)

# COMMAND ----------


