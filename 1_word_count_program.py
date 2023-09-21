# -*- coding: utf-8 -*-
"""
Created on Thur Sep 21 05:22:12 2023

@author: meguerg
"""

from mrjob.job import MRJob
import re

# Regular expression to split words by non-alphabetic characters
word_re = re.compile(r"\b\w+\b")

class WordCount(MRJob):

    def mapper(self, _, line):
        # Split the line into words using the regular expression
        words = re.findall(word_re, line.lower())
        # Emit each word as a key with a value of 1
        for word in words:
            yield (word, 1)

    def reducer(self, word, counts):
        # Sum the counts for each word
        yield (word, sum(counts))

if __name__ == '__main__':
    WordCount.run()
