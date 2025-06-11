
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, month, year

# Create Spark session
spark = SparkSession.builder     .appName("Retail Customer Insights")     .master("local[*]")     .getOrCreate()

# Load data
df = spark.read.option("header", True).csv("data/transactions.csv", inferSchema=True)

# Add total amount column
df = df.withColumn("TotalAmount", col("Quantity") * col("Price"))

# Top customers by spending
top_customers = df.groupBy("CustomerID")     .agg(spark_sum("TotalAmount").alias("TotalSpent"))     .orderBy(col("TotalSpent").desc())

print("Top Customers by Spending:")
top_customers.show()

# Monthly revenue
df = df.withColumn("Month", month(col("Date"))).withColumn("Year", year(col("Date")))
monthly_revenue = df.groupBy("Year", "Month")     .agg(spark_sum("TotalAmount").alias("MonthlyRevenue"))     .orderBy("Year", "Month")

print("Monthly Revenue:")
monthly_revenue.show()

# Most frequently purchased products
popular_products = df.groupBy("Product")     .agg(spark_sum("Quantity").alias("TotalQuantity"))     .orderBy(col("TotalQuantity").desc())

print("Most Popular Products:")
popular_products.show()

# Average Order Value
avg_order_value = df.select("TransactionID", "TotalAmount")     .groupBy("TransactionID")     .agg(spark_sum("TotalAmount").alias("OrderValue"))     .agg({"OrderValue": "avg"})

print("Average Order Value:")
avg_order_value.show()

spark.stop()
