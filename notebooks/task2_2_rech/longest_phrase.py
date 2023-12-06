
from mrjob.job import MRJob
from mrjob.step import MRStep
import logging

logger = logging.getLogger(__name__)

class LongestPhrase(MRJob):    
    def mapper_init(self):
        self.replics = {}
        
    def mapper(self, _, line):
        splitted_line = line.strip().split('" "')
        
        if len(splitted_line) == 3:
            _, character, phrase = splitted_line
            
            if character not in self.replics:
                self.replics[character] = phrase
            else:
                if len(phrase) > len(self.replics[character]):
                    self.replics[character] = phrase
            
    def mapper_final(self):
        # logger.info(f"MAPPER FINAL LEN {len(self.replics.items())}")
        for character, phrase in self.replics.items():
            yield character, phrase

    def reducer_init(self):
        self.phrases = []

    def reducer(self, key, values):
        longest_phrase = ""
        for phrase in values:
            if len(phrase) > len(longest_phrase):
                longest_phrase = phrase
        
        self.phrases.append((key, longest_phrase))

    def reducer_final(self):
        sorted_phrases = sorted(self.phrases, key=lambda x: len(x[1]), reverse=True)
        # logger.info(f"REDUCER FINAL LEN {len(sorted_phrases)}")
        for character, phrase in sorted_phrases:
            yield None, (character, phrase)

    def mapper2(self, _, line):
        yield "onekey", line
    
    def reducer2(self, key, values):
        phrases = []
        for value in values:
            phrases.append(value)

        phrases.sort(key=lambda x: len(x[1]), reverse=True)

        for character, phrase in phrases:
            yield character, phrase
    
    def steps(self):
        return [
            MRStep(mapper_init=self.mapper_init,
                   mapper=self.mapper,
                   mapper_final=self.mapper_final,
                   reducer_init=self.reducer_init,
                   reducer=self.reducer,
                   reducer_final=self.reducer_final),
            MRStep(mapper=self.mapper2,
                  reducer=self.reducer2)
        ]

if __name__ == "__main__":
    LongestPhrase.run()
