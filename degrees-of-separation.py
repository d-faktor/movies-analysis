from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("DegreesOfSeparation")
sc = SparkContext(conf=conf)

# The characters we wish to find the degree of separation between:
start_character_id = 5306  # SpiderMan
target_character_id = 14  # ADAM 3,031 (who?)

# Our accumulator, used to signal when we find the target character during
# our BFS traversal.
hit_counter = sc.accumulator(0)


def convert_to_bfs(line):
    fields = line.split()
    hero_id = int(fields[0])
    connections = []
    for connection in fields[1:]:
        connections.append(int(connection))

    color = 'WHITE'
    distance = 9999

    if hero_id == start_character_id:
        color = 'GRAY'
        distance = 0

    return (hero_id, (connections, distance, color))


def create_starting_rdd():
    input_file = sc.textFile("file:///sparkcourse/marvel-graph.txt")
    return input_file.map(convert_to_bfs)


def bfs_map(node):
    character_id = node[0]
    data = node[1]
    connections = data[0]
    distance = data[1]
    color = data[2]
    results = []

    #If this node needs to be expanded...
    if color == 'GRAY':
        for connection in connections:
            new_character_id = connection
            new_distance = distance + 1
            new_color = 'GRAY'
            if target_character_id == connection:
                hit_counter.add(1)

            new_entry = (new_character_id, ([], new_distance, new_color))
            results.append(new_entry)

        #We've processed this node, so color it black
        color = 'BLACK'

    #Emit the input node so we don't lose it.
    results.append((character_id, (connections, distance, color)))
    return results


def bfs_reduce(data1, data2):
    edges1 = data1[0]
    edges2 = data2[0]
    distance1 = data1[1]
    distance2 = data2[1]
    color1 = data1[2]
    color2 = data2[2]

    distance = 9999
    color = color1
    edges = []

    # See if one is the original node with its connections.
    # If so preserve them.
    if len(edges1) > 0:
        edges.extend(edges1)
    if len(edges2) > 0:
        edges.extend(edges2)

    # Preserve minimum distance
    if distance1 < distance:
        distance = distance1

    if distance2 < distance:
        distance = distance2

    # Preserve darkest color
    if color1 == 'WHITE' and (color2 == 'GRAY' or color2 == 'BLACK'):
        color = color2

    if color1 == 'GRAY' and color2 == 'BLACK':
        color = color2

    if color2 == 'WHITE' and (color1 == 'GRAY' or color1 == 'BLACK'):
        color = color1

    if color2 == 'GRAY' and color1 == 'BLACK':
        color = color1

    return (edges, distance, color)


iteration_rdd = create_starting_rdd()
iteration = 0

while True:
    print("Running BFS iteration# " + str(iteration+1))

    # Create new vertices as needed to darken or reduce distances in the
    # reduce stage. If we encounter the node we're looking for as a GRAY
    # node, increment our accumulator to signal that we're done.
    mapped = iteration_rdd.flatMap(bfs_map)

    # Note that mapped.count() action here forces the RDD to be evaluated, and
    # that's the only reason our accumulator is actually updated.
    print("Processing " + str(mapped.count()) + " values.")

    if hit_counter.value > 0:
        print("Hit the target character! From " + str(hit_counter.value) \
            + " different direction(s).")
        break

    # Reducer combines data for each character ID, preserving the darkest
    # color and shortest path.
    iteration_rdd = mapped.reduceByKey(bfs_reduce)
    iteration = iteration + 1


