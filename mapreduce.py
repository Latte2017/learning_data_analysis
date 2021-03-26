#Write a Code to print the top 5 word - occurences
# python movie_ratings_count.py -r hadoop --hadoop-streaming-jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar ../data        /u.data

#Import Dependencies
from mrjob.job import MRJob
from mrjob.step import MRStep
import heapq

class MRWordCount(MRJob):

  def steps(self):
    return [MRStep(mapper=self.mapper,reducer=self.reducer),MRStep(reducer = self.secondreducer)]

  def mapper(self,_,lines):
    Uid, Mid, rating, timestamp = lines.split('\t')
    yield Mid, 1

  def reducer(self,key,values):
    yield None,('%04d'%int(sum(values)),key)
  

  def secondreducer(self, occ, word_list):
    self.h = []
    for wrd in word_list:
      self.h.append(wrd)

    while len(self.h) > 5:
      heapq.heappop(self.h)
 
    while self.h:
      ret_val = heapq.heappop(self.h)[1]
      yield ret_val, None
      
  

if __name__ == '__main__':
    MRWordCount.run()