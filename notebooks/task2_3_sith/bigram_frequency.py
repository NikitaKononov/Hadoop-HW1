
import mrjob
from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
import logging

logger = logging.getLogger(__name__)

class BigramFrequency(MRJob):
    
    def mapper_init(self):
        # чтобы не валило логами
        nltk.download('punkt')
        nltk.download('stopwords')
        self.stop_words = set(stopwords.words('english'))

        self.bigrams = {}

    def mapper(self, _, line):
        splitted_line = line.strip().split('" "')
        if len(splitted_line) == 3:
            _, _, line = splitted_line
            line = re.sub(r'[^\w\s]', '', line).lower()
            
            words = word_tokenize(line)
            words = [word for word in words if word not in self.stop_words]
            
            bigrams = [words[i] + " " + words[i + 1] for i in range(len(words) - 1)]
            
            for bigram in bigrams:
                if bigram not in self.bigrams:
                    self.bigrams[bigram] = 1
                else:
                    self.bigrams[bigram] = self.bigrams[bigram] + 1

    def mapper_final(self):
        # logger.info(f"MAPPER FINAL LEN {len(self.bigrams.items())}")
        for bigram, count in self.bigrams.items():
            yield bigram, count
    
    def reducer(self, key, values):
        yield None, (key, sum(values))

    def mapper2(self, _, line):
        yield "onekey", line
    
    def reducer2(self, key, values):
        bigrams = []
        for value in values:
            bigrams.append(value)

        bigrams.sort(key=lambda x: x[1], reverse=True)

        for bigram, count in bigrams[:20]:
            yield bigram, count

    def steps(self):
        return [
            MRStep(mapper_init=self.mapper_init,
                   mapper=self.mapper,
                   mapper_final=self.mapper_final,
                   reducer=self.reducer),
            MRStep(mapper=self.mapper2,
                  reducer=self.reducer2)
        ]

if __name__ == '__main__':
    BigramFrequency.run()
