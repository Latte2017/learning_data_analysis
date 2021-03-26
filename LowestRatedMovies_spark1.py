#Give the lowest rated movies using spark

from pyspark import SparkConf, SparkContext
from collections import defaultdict
import sys

#Get a count of all the movies in line
def parseInput(line):
    fields = line.split()
    return (int(fields[1]), (float(fields[2]), 1.0))

#Parse the input and return list of movies
def LoadMovieNames():
    movie_id_to_name = {}
    with open("../data/u.item") as f:
        for line in f:
            line_arr = line.split("|")
            movie_id_to_name[int(line_arr[0])] = line_arr[1]
    return movie_id_to_name

if __name__ == "__main__":
    #Load up movie ID --> movie name
    movie_id_to_name = LoadMovieNames()

    conf = SparkConf().setAppName("WorstMovies")
    sc = SparkContext(conf = conf)

    # Load up raw u.data file
    lines = sc.textFile("hdfs:///user/root/load_movie_data/u.data") ## Lines is Resilient Distributed Data(RDD)

    #Convert to (movieID, (ratings, 1.0))
    movie_ratings = lines.map(parseInput)

    #Reduce to (movieID, (sumOfRatings, totalRatings))
    '''
    Spark RDD reduceByKey() transformation is used to merge the values of each key using an associative reduce function.
    It is a wider transformation as it shuffles data across multiple partitions and it operates on pair RDD (key/value pair).
     example https://tinyurl.com/reduceByKey
    '''
    ratings_total_and_count = movie_ratings.reduceByKey(lambda movie1, movie2: (movie1[0] + movie2[0], movie1[1] + movie2[1]))

    #mapvalues just gets the values instead of key/value pair like map function
    #Calculate the average
    average_ratings = ratings_total_and_count.mapValues(lambda x: x[0]/x[1])

    #Sort by average
    sorted_movies = average_ratings.sortBy(lambda x:x[1])

    #Take top 10
    results = sorted_movies.take(10)

    print(results)

    for result in results:
        print(movie_id_to_name[result[0]], result[1])

