from mrjob.job import MRJob
from mrjob.step import MRStep

class RatingsBreakdown(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_ratings,
                   reducer=self.reducer_count_ratings),
            MRStep(reducer=self.reducer_sort_ratings)
        ]

    def mapper_get_ratings(self, _, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield int(rating), 1

    def reducer_count_ratings(self, key, values):
        yield None, (sum(values), key)
        
    def reducer_sort_ratings(self, _, count_ratings_pairs):
        for count, rating in sorted(count_ratings_pairs, reverse=True):
            yield rating, count
        
if __name__ == '__main__':
    RatingsBreakdown.run()
