
from pyspark.sql.types import *
import re

"""define a custom schema"""
customSchema = StructType([ \
    StructField("_id", StringType(), True), \
    StructField("tags", StringType(), True), \
    StructField("body", StringType(), True)])

postXml = sc.textFile("spark_data/Posts.xml")
          .map(lambda x:x.strip())
          .filter(lambda x:x.startswith("<?xml version=")==False)
          .filter(lambda x:x!="<posts>" and x!="</posts>")
          
xml = spark.createDataFrame(postXml.map(parse).filter(lambda x:x!=None), customSchema)
