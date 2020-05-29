#!/usr/bin/env python
# coding: utf-8

from mrjob.job import MRJob, MRStep

class MRTask(MRJob):
    def mapper1(self, _, data):
        yield (data.split(',')[3],data.split(',')[0]), float(data.split(',')[4])       

    def reducer1(self, ID, cost):
        yield ID, sum(cost)

    def mapper2(self, IDs, costs):
        yield IDs[0], (costs,1)

    def reducer2(self, key, values):
        record = list(zip(*list(values)))
        yield key, [sum(record[1]), sum(record[0])]

    def steps(self):
        return [
            MRStep(mapper=self.mapper1,
                  reducer=self.reducer1),
            MRStep(mapper=self.mapper2,
                  reducer=self.reducer2)
        ]

if __name__ == '__main__':

    MRTask.run()
        