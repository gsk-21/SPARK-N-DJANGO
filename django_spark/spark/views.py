from django.shortcuts import render

# Create your views here.

from django.http import HttpResponse

import json

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
# from pyspark.sql.types import *
from pyspark.sql.types import StructType, StructField, IntegerType, ArrayType, StringType, DateType, LongType
from pyspark.sql.functions import struct, when, col, lit, expr, concat

global spark
spark = SparkSession.builder.master("local").appName("Databricks").getOrCreate()


def index(request):
    df = spark.sql('Select cast(current_date as string) as dt')
    df.show()
    date = list(df.select('dt').collect())[0][0]
    # date = list(df.select('dt').toPandas()['dt']).pop()
    print(date)

    response = f"""<center></br></br> 
                    Hello World! </br></br> 
                    Today's date - {date}
                    </center> 
                """
    return HttpResponse(response)
