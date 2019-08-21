from pyspark.sql import SparkSession
from pyspark.sql import Row


def load_movie_names():
    movie_names = {}
    with open("ml-100k/u.ITEM") as f:
        for line in f:
            fields = line.split('|')
            movie_names[int(fields[0])] = fields[1]
    return movie_names


# Create a SparkSession (the config bit is only for Windows!)
spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///C:/tmp").appName("PopularMovies").getOrCreate()

name_dict = load_movie_names()

lines = spark.sparkContext.textFile("file:///SparkCourse/ml-100k/u.data")
movies = lines.map(lambda x: Row(movieID =int(x.split()[1])))
movie_dataframe = spark.createDataFrame(movies)
top_movie_id = movie_dataframe.groupBy("movieID").count().orderBy("count", ascending=False).cache()
top_movie_id.show()
top10 = top_movie_id.take(10)
print("\n")
for result in top10:
    print("%s: %d" % (name_dict[result[0]], result[1]))
spark.stop()
