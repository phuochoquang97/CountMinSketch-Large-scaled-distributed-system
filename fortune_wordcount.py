from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, desc

# create a Spark session
spark = SparkSession.builder.appName("WordFrequencyCount").getOrCreate()

# read the text file as a DataFrame
# I use the Fortune book, whoes content is cleaned by removing:
# numbers, punctuation, special characters, extra white spaces and stop words
lines = spark.read.text("Fortune_clean.txt")

# tokenize each line into words
words = lines.select(explode(split(lines.value, " ")).alias("word"))

# calculate word counts with descending order
word_counts = words.groupBy("word").count().orderBy(desc("count"))

# show top 20 word counts
word_counts.show(truncate=False)

# stop the Spark session
spark.stop()
