from __future__ import print_function
import re
import HTMLParser
from pyspark import SparkContext, SQLContext
from pyspark.sql.functions import udf, explode
from pyspark.ml.feature import Tokenizer, StopWordsRemover

sc = SparkContext(appName="SparkWorkshop")
sqlContext = SQLContext(sc)


expression = re.compile(r'<.*?>')
parser = HTMLParser.HTMLParser()

def strip_tags(html):
    return parser.unescape(
        expression.sub('', html)
        )

strip_tags_udf = udf(strip_tags)
tokenizer = Tokenizer(inputCol="comment_clean", outputCol="words")
stopWordsRemover = StopWordsRemover(inputCol="words", outputCol="tokens")

# Load data
#comments = sqlContext.read.json("hdfs://cloudera0.acis.ufl.edu:8020/user/mcollins/hacker_news_small.json")
comments = sqlContext.read.json("data/hacker_news_small.json")

# Calcualte tokens dataframe
tokens = stopWordsRemover.transform(
             tokenizer.transform(comments\
                 .withColumn("comment_clean", strip_tags_udf(comments["comment_text"]))\
             )\
         )\
         .select(explode("tokens").alias("token"))\
         .groupBy("token")\
         .count()\
         .orderBy("count", ascending=False)\
         .select("count")\
         .limit(1000)

tokens_pdf = tokens.toPandas()
tokens_pdf["rank"] = range(1, tokens_pdf.shape[0] + 1)
print(tokens_pdf.head())
