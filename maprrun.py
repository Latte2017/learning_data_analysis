
from mrjob.job import MRJob
from mrjob.step import MRStep
from collections import defaultdict
import heapq

'''
class MRWordCounter(MRJob):

    def Steps(self):
      return [MRStep(mapper=self.mapper,reducer=self.reducer),MRStep(reducer=self.secondreducer)]

    def secondreducer(self,key,values):
      self.alist = []
      for value in values:
        self.alist.append(value)
      self.blist = []
      for i in range(5):
        self.blist.append(max(self.alist))
        self.alist.remove(max(self.alist))
      for i in range(5):
        yield self.blist[i]
    

    def secondreducer(self,values, key):
      self.alist = []
      for value in values:
        self.alist.append(value)
      self.blist = []
      for i in range(5):
        self.blist.append(max(self.alist))
        self.alist.remove(max(self.alist))
      for i in range(5):
        yield self.blist[i]
    

    def mapper(self, key, line):
        results = []
        wrd, ct, ct1, ct2, = line.split('\t')
        yield (wrd, 1)

    def reducer(self, word, occurrences):
        return_arr = []
        dix = defaultdict(int)
        dix[word] = sum(occurrences)
        yield None, (dix[word], int(word))


if __name__ == '__main__':
    MRWordCounter.run()
'''



class MRWordCount(MRJob):

  def steps(self):
    return [MRStep(mapper=self.mapper,reducer=self.reducer),MRStep(reducer = self.secondreducer)]

  def mapper(self,_,lines):
    uID, mID, rank, timestamp = lines.split('\t')
    yield mID.lower(),1

  def reducer(self,key,values):
    yield ('%04d'%int(sum(values)),key)


  def secondreducer(self,values, key):
    self.alist = []
    for value in values:
      self.alist.append(value)
    self.blist = []
    for i in range(5):
      self.blist.append(max(self.alist))
      self.alist.remove(max(self.alist))
    for i in range(5):
      yield self.blist[i]

  '''
  def secondreducer(self, occ, word_list):
      self.h = []
      for wrd in word_list:
        if len(self.h) < 5:
          heapq.heappush(self.h, (int(occ), wrd))
        else:
          heapq.heappushpop(self.h, (int(occ), wrd))
      
      while self.h:
        yield heapq.heappop(self.h), None
'''  


if __name__ == '__main__':
    MRWordCount.run()
