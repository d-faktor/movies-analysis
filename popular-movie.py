from pyspark import SparkConf, SparkContext


def find_movie_name():
    movie_file = open("C:/SparkCourse/ml-100k/u.item", "r")
    name_dict = dict()
    for movie in movie_file:
        movie_list = movie.split('|')
        name_dict[movie_list[0]] = movie_list[1]
    return name_dict


configuration = SparkConf().setMaster("local").setAppName("popular-movie")
sc = SparkContext(conf=configuration)

movie_dict = sc.broadcast(find_movie_name())

input_file = sc.textFile("C:/SparkCourse/ml-100k/u.data")
movies = input_file.map(lambda line: (line.split()[1], 1))
movie_count = movies.reduceByKey(lambda watch1, watch2: watch1 + watch2)

movie_with_names_count = movie_count.map(lambda movie: (movie[1], movie_dict.value[movie[0]]))
movie_with_names_count_sorted = movie_with_names_count.sortByKey(ascending=False)
movie_count_list = movie_with_names_count_sorted.collect()

for pair in movie_count_list:
    print(pair[1], pair[0])

