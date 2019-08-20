from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

movie_rdd = sc.textFile("C:/SparkCourse/ml-100k/u.data")
movie_ratings = movie_rdd.map(lambda x: x.split()[2])
ratings_count = movie_ratings.countByValue()
sorted_ratings = sorted(ratings_count.items())

for rating in sorted_ratings:
    print(rating[0], rating[1])