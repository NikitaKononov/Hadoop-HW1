
from mrjob.job import MRJob
from mrjob.step import MRStep

class BoltunAwards(MRJob):
    def mapper_init(self):
        self.replics = {}
        
    def mapper(self, _, line):
        splitted_line = line.strip().split('" "')
        
        if len(splitted_line) > 2:
            _, character, _ = splitted_line
            
            if character not in self.replics:
                self.replics[character] = 1
            else:
                self.replics[character] = self.replics[character] + 1
            
    def mapper_final(self):
        for character, count in self.replics.items():
            yield character, count

    def reducer_init(self):
        self.top_boltunov = []

    def reducer(self, key, values):
        total = sum(values)
        self.top_boltunov.append((key, total))

    def reducer_final(self):
        self.top_boltunov.sort(key=lambda x: x[1], reverse=True)
        for character, count in self.top_boltunov[:20]:
            yield character, count

    def steps(self):
        return [
            MRStep(mapper_init=self.mapper_init,
                   mapper=self.mapper,
                   mapper_final=self.mapper_final,
                   reducer_init=self.reducer_init,
                   reducer=self.reducer,
                   reducer_final=self.reducer_final)
        ]

if __name__ == "__main__":
    BoltunAwards.run()
