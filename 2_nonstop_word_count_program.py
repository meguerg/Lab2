# -*- coding: utf-8 -*-
"""
Created on Thur Sep 21 06:32:17 2023

@author: meguerg
"""

from mrjob.job import MRJob
import re

# List of the stopwords
stopwords = set(["the", "and", "of", "a", "to", "in", "is", "it"])

# Regular expression to split words by non-alphabetic characters
WORD_RE = re.compile(r"\b\w+\b")

class NonStopWordCount(MRJob):

    def mapper(self, _, line):
        # Split the line into different words
        words = re.findall(WORD_RE, line.lower())
        # List each non-stop word as a key with a value of 1
        for word in words:
            if word not in stopwords:
                yield (word, 1)

    def reducer(self, word, counts):
        # total counts for each word
        yield (word, sum(counts))

if __name__ == '__main__':
    NonStopWordCount.run()
