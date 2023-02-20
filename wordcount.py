
from pyspark import SparkContext

# create a new SparkContext
sc = SparkContext("local", "Word Count App")

# read the input file
input_file = sc.textFile(sys.argv[1])

# split each line into words and create a list of (word, 1) pairs
word_counts = input_file.flatMap(lambda line: line.split()) \
                        .map(lambda word: (word, 1)) \
                        .reduceByKey(lambda x, y: x + y)

# save the word counts to a text file
word_counts.saveAsTextFile(sys.argv[2])

# stop the SparkContext
sc.stop()