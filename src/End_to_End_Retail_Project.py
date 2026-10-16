# Databricks notebook source
# MAGIC %md 
# MAGIC
# MAGIC # Bronze layer

# COMMAND ----------

dbutils.widgets.text("catalog", "")

catalog = dbutils.widgets.get("catalog")

# Bronze layer
bronze_customer_table = f"{catalog}.bronze.bronze_customers"
bronze_orders_table = f"{catalog}.bronze.bronze_orders"

# Silver layer
silver_customer_table = f"{catalog}.silver.silver_customers"
silver_orders_table = f"{catalog}.silver.silver_orders"

# Gold layer
fact_sales_table = f"{catalog}.gold.fact_sales"
dim_customers_table = f"{catalog}.gold.dim_customers"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 - Define Schema as STRING
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType
customers_schema = ([
    StructField("Customer_id", StringType(), True),
    StructField("Customer_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone_number", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("date_of_birth", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("registration_date", StringType(), True),
    StructField("loyalty_points", StringType(), True),
    StructField("status", StringType(), True)]) 


orders_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("order_date", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("quantity", StringType(), True),
    StructField("unit_price", StringType(), True),
    StructField("discount", StringType(), True),
    StructField("total_amount", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("store_location", StringType(), True),
    StructField("order_status", StringType(), True)
])

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Step 2 - Loading the csv files

# COMMAND ----------

oders_bronze_df = spark.read.format('csv')\
                .option('header','True').schema(StructType(orders_schema))\
                .load("/Volumes/retail_project/raw_data/raw_file/retail_orders_messy.csv")


customers_bronze_df = spark.read.format('csv')\
                .option('header','True').schema(StructType(customers_schema))\
                .load("/Volumes/retail_project/raw_data/raw_file/retail_customers_messy.csv")

                

# COMMAND ----------

oders_bronze_df.display()
customers_bronze_df.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Step 3 - Ingestion Metadata

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

oders_bronze_df = oders_bronze_df\
                    .withColumn('Ingested_date', current_timestamp())
customers_bronze_df = customers_bronze_df\
                    .withColumn('Ingested_date', current_timestamp())


# COMMAND ----------

# MAGIC %md 
# MAGIC ## Step 4 - Save as Delta Table in Bronze Schema

# COMMAND ----------

oders_bronze_df.write.format('delta') \
    .mode('overwrite') \
    .saveAsTable(bronze_orders_table)

customers_bronze_df.write.format('delta') \
                    .mode('overwrite') \
                    .saveAsTable(bronze_customer_table)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Step 5 - Reading the Bronze tabes back to df 
# MAGIC
# MAGIC

# COMMAND ----------

oders_bronze_df = spark.read.table(bronze_orders_table)
customers_bronze_df = spark.read.table(bronze_customer_table)

# COMMAND ----------

oders_bronze_df.display()


# COMMAND ----------

# MAGIC %md 
# MAGIC # Sivler layer 
# MAGIC ##Step 1 - Craeating the silver df after cleaning the data
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import try_to_date,col,coalesce

silver_df = oders_bronze_df.withColumn(
            'order_date',
            coalesce(try_to_date(col('order_date'),'yyyy-MM-dd'),
                     try_to_date(col('order_date'),'dd-MM-yyyy'),
                     try_to_date(col('order_date'),'yyyy/MM/dd'),
                     try_to_date(col('order_date'),'dd/MM/yyyy')
                    ))

silver_df.show()

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Step 2 - Standardization of Columns

# COMMAND ----------

silver_df.select('category').distinct().show()

# COMMAND ----------

from pyspark.sql.functions import when,upper, trim

silver_df = silver_df.withColumn('category',
                                 when(upper(col('category')) == 'ELECTRONICS','ELECTRONIC').
                                 when(upper(col('category')) == 'FASHON','FASHION').
                                 when(upper(col('category')) == 'HOME AND LIVING','HOME & LIVING').
                                 otherwise(col('category'))
                                           )
silver_df = silver_df.withColumn('category',upper(col('category')))

silver_df.show()

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Step 3 - Remove duplicate orders

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct

silver_df.select(count(col('order_id')),countDistinct(col('order_id'))).display()

# COMMAND ----------

silver_df = silver_df.dropDuplicates(['order_id'])

silver_df.select(count(col('order_id')),countDistinct(col('order_id'))).display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Step 4 -handling null values

# COMMAND ----------

silver_df = silver_df.withColumn('discount',
                                 when(col('discount').isNull(),'0').when(col('discount')=='ten','0.1').otherwise(col('discount')))

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Step 5 -Convert Discount % to Decimal

# COMMAND ----------

from pyspark.sql.functions import regexp_replace

silver_df = silver_df.withColumn('discount',
                                 when(col('discount').like('5%'),regexp_replace('discount','5%','0.05')).
                                 when(col('discount').like('10%'),regexp_replace('discount','10%','0.1')).
                                 otherwise(col('discount'))
                                 )

silver_df.show()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Step 6 -Remove Negative Quantity

# COMMAND ----------


silver_df = silver_df.filter(col('quantity')>0)
silver_df.show()

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Step 7 -Convert Numeric to Decimal values 

# COMMAND ----------

from pyspark.sql.types import DecimalType

silver_df = silver_df.withColumn('unit_price',col('unit_price').try_cast(DecimalType(10,2)))

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Step 7 -Calculating total amount

# COMMAND ----------

silver_df = silver_df.withColumn('total_amount', col('quantity').cast('int')* col('unit_price')* (1-col('discount').cast('double'))
                                 )

silver_df.show()

# COMMAND ----------

silver_df.write.format('delta').mode('overwrite').saveAsTable(silver_orders_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Customers 
# MAGIC ### Step 1 - Writing the brone table to silver df 

# COMMAND ----------

from pyspark.sql.functions import trim, col 

bronze_customers_df = spark.table(bronze_customer_table)

silver_customer_df = bronze_customers_df \
                        .select(
                            trim(col('Customer_id')).alias('customer_id'),
                            trim(col('Customer_name')).alias('customer_name'),
                            trim(col("email")).alias("email"),
                            trim(col("phone_number")).alias("phone_number"),
                            trim(col("gender")).alias("gender"),
                            trim(col("date_of_birth")).alias("date_of_birth"),
                            trim(col("city")).alias("city"),
                            trim(col("state")).alias("state"),
                            trim(col("registration_date")).alias("registration_date"),
                            trim(col("loyalty_points")).alias("loyalty_points"),
                            trim(col("status")).alias("status"),
                            col("Ingested_date").alias("ingested_date")
                            )


silver_customer_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - Removing Duplicates based on Customer Id and latest date time

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number,desc

Window_spec = Window.partitionBy(col('customer_id')).orderBy(desc('ingested_date'))

silver_customer_df = silver_customer_df.withColumn('rn', row_number().over(Window_spec))

silver_customer_df = silver_customer_df.filter('rn=1')\
                        .drop('rn')

silver_customer_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Stadardizing the status column

# COMMAND ----------

silver_customer_df = silver_customer_df.withColumn('status',upper(col('status')))

silver_customer_df = silver_customer_df.withColumn('status',
                                                   when(col('status').isin ('ACTIVE','INACTIVE'),col('status')).otherwise('UNKNOWN')
                                                
                                                )
silver_customer_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Covert loyalty Points to integer

# COMMAND ----------

from pyspark.sql.functions import regexp_replace


silver_customer_df = silver_customer_df.withColumn('loyalty_points', regexp_replace(col('loyalty_points'),'[^0-9]',"")
                                                   )
silver_customer_df = silver_customer_df.withColumn('loyalty_points', col('loyalty_points').try_cast('int'))
silver_customer_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4 - Valid email format

# COMMAND ----------

silver_customer_df = silver_customer_df.withColumn(
    "email_valid",
    col("email").rlike("^[A-Za-z0-9+_.-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$")
)

# Example: meenadas@email.com - valid, karansingh@email - invalid

# customers_df.select("email", "email_valid").display()

silver_customer_df = silver_customer_df.filter(col("email_valid") == True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 - Fix Date Format

# COMMAND ----------

silver_customer_df = silver_customer_df.withColumn(
    'date_of_birth',
    coalesce(
        try_to_date(col('date_of_birth'),'yyyy-MM-dd'),
        try_to_date(col('date_of_birth'),'dd-MM-yyyy'),
        try_to_date(col('date_of_birth'),'yyyy/MM/dd'),
        try_to_date(col('date_of_birth'),'dd/MM/yyyy')
    )
)

silver_customer_df = silver_customer_df.withColumn(
    'registration_date',
    coalesce(
        try_to_date(col('registration_date'),'yyyy-MM-dd'),
        try_to_date(col('registration_date'),'dd-MM-yyyy'),
        try_to_date(col('registration_date'),'yyyy/MM/dd'),
        try_to_date(col('registration_date'),'dd/MM/yyyy')
    )
)
silver_customer_df.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Step 6 - Standaridize Gender Column 

# COMMAND ----------

from pyspark.sql.functions import col
silver_customer_df = silver_customer_df.withColumn('gender', upper(col('gender'))
                                                   )


silver_customer_df = silver_customer_df.withColumn(
    "gender",
    when(col("gender") == "MALE", "M")
    .when(col("gender") == "FEMALE", "F")
    .otherwise("U")
)
                                                  

silver_customer_df.display()                                                                                                     

# COMMAND ----------

silver_customer_df.write.format('delta').mode('overwrite').saveAsTable(silver_customer_table)

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold Layer

# COMMAND ----------

silver_customers_df = spark.read.table(silver_customer_table)
silver_orders_df = spark.read.table(silver_orders_table)

silver_customers_df.display()
silver_orders_df.display()
     

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Step 1 - Create Customer Dimension Tables

# COMMAND ----------

dim_customers = silver_customers_df.select('customer_id',
                                               'customer_name',
                                               'email',
                                               'city',
                                               'state',
                                               'loyalty_points',
                                               'status')
dim_customers.display()

# COMMAND ----------

dim_customers.write.format('delta').mode('overwrite').saveAsTable(dim_customers_table)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Step 2 - Create Fact Sales Table 

# COMMAND ----------

from pyspark.sql.functions import year , month , dayofmonth,col

silver_orders_df = silver_orders_df.alias("o")
silver_customers_df = silver_customers_df.alias("c")

fact_sales = silver_orders_df.join(
                                    silver_customers_df,
                                            col('c.customer_id')== col('o.customer_id'),
                                            'left'
                                            ) \
                                   .select(col('o.order_id'),
                                           col('o.order_date'),
                                           col('o.customer_id'),
                                           col('c.customer_id'),
                                           col('c.customer_name'),
                                           col('o.product_id'),
                                           col('o.product_name'),
                                           col('o.category'),
                                           col('o.quantity'),
                                           col('o.unit_price'),
                                           col('o.discount'),
                                           col('o.total_amount'),
                                           col('c.city'),
                                           col('c.state'),
                                           col('c.gender'),
                                           col('c.loyalty_points'))




order_enriched = fact_sales.withColumn('year',year('order_date')) \
                        .withColumn('month',month('order_date')) \
                        .withColumn('day',dayofmonth('order_date'))

order_enriched.display()

# COMMAND ----------

from pyspark.sql.functions import sum, countDistinct, avg, min,max ,round

gold_df = order_enriched.groupBy(
    "c.customer_id",
    "o.product_id",
    "o.product_name",
    "o.category",
    'year'
    
).agg(
    round(sum("total_amount"),2).alias("Total_revenue"),
    sum("quantity").alias("Total_quantity"),
    countDistinct("order_id").alias("Total_orders"),
    avg("total_amount").alias("Avg_revenue"),
    avg("quantity").alias("Avg_quantity"),
    # avg("loyalty_points").alias("Avg_loyalty_points"),
    min("unit_price").alias("Min_price"),
    max("unit_price").alias("Max_price")
) .orderBy(col("Total_revenue").desc(),col("Total_quantity").desc())

gold_df.display()

# COMMAND ----------

gold_df.write.format('delta').mode('overwrite').saveAsTable(fact_sales_table)