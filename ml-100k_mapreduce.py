# -*- coding: utf-8 -*-
"""
Created on Fri Jan  5 16:10:32 2018

@author: alex.hall
functions to count the number of each movie rating by using mapreduce
"""



from mrjob.job import MRJob
from mrjob.step import MRStep

class RatingsBreakdown(MRJob):
    
    #uses mrjob library to define steps of the mapreduce job
    def steps(self):
        return[
                MRStep(mapper=self.mapper_get_ratings,
                       reducer=self.reducer_count_ratings)]
                
            #function to map lines to key value pair (rating,1) to allow for ratings count
    def mapper_get_ratings(self, _, line):
        #split input line by tab delimiter
        (userID, movieID, rating, timeStamp)=line.split('\t')
        yield rating,1
    
    #counts the number of each rating
    def reducer_count_ratings(self, key, values):
        yield key , sum(values)
        
if __name__ == '__main__':
    RatingsBreakdown.run()