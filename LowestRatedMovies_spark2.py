from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions
from pyspark import SparkContext



def Map_Id_to_Name():
    id_to_name = {}
    with open("../data/u.item") as f:
        for line in f:
            line = line.split("|")
            id_to_name[line[0]] = line[1]
    
    return id_to_name


def parseInput(line):
    line_arr = line.split("\t")
    return Row(movieID=line_arr[0], rating=line_arr[1])


def getWorst():
    id_to_name = Map_Id_to_Name()
    spark_session = SparkSession.builder.appName("PopularMovies").getOrCreate()
    lines = SparkContext.textFile("hdfs:///user/root/load_movie_data/u.data")
    movies = lines.map(parseInput)
    movieDataset = spark.createDataFrame(movies)
    get_avg = movieDataset.groupBy("movieID").avg("rating")
    get_count = movieDataset.groupBy("movieID").count()
    avg_and_count = get_avg.join(get_count, "movieID")
    top_ten = avg_and_count.orderBy("avg(rating").take(10)

    for movie in top_ten:
        print(id_to_name[movie[0], movie[1], movie[2]])



if __name__ == "__main__":
    getWorst()
