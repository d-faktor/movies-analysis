from pyspark import SparkConf, SparkContext


def find_hero_name():
    hero_dict = dict()
    hero_name_file = open("C:/SparkCourse/Marvel-Names.txt")
    for line in hero_name_file:
        hero_name = line.split("\"")
        hero_dict[int(hero_name[0])] = hero_name[1]
    return hero_dict


configuration = SparkConf().setMaster("local").setAppName("most-popular-superhero")
sc = SparkContext(conf=configuration)

name_dict = sc.broadcast(find_hero_name())

hero_graph_file = sc.textFile("C:/SparkCourse/Marvel-Graph.txt")
hero_graph = hero_graph_file.map(lambda line: (name_dict.value[int(line.split()[0])], len(line.split()) - 1))
hero_popularity = hero_graph.reduceByKey(lambda count1, count2: count1 + count2)
hero_popularity_sorted = hero_popularity.sortBy(lambda pair: pair[1])
hero_popularity_sorted_list = hero_popularity_sorted.collect()

for hero in hero_popularity_sorted_list:
    print(hero[0], hero[1])