# -*- coding: utf-8 -*-
"""
Created on Thur Sep 21 8:51:27 2023

@author: meguerg
"""

from mrjob.job import MRJob

class BigramCount(MRJob):

    def configure_args(self):
        super(BigramCount, self).configure_args()
        self.add_passthru_arg('--stopwords', type=str, help='Comma-separated list of stopwords to exclude')

    def mapper(self, _, line):
        # Separate the line into words
        word = line.split()

        # List the stopwords from the command line argument
        stop_words = set(self.options.stopwords.split(','))

        # Generate bigrams and emit them as key-value pairs
        for i in range(len(word) - 1):
            word1 = word[i].lower()
            word2 = word[i + 1].lower()

            # Exclude bigrams with stopwords
            if word1 not in stop_words and word2 not in stop_words:
                yield f"{word1},{word2}", 1

    def reducer(self, bigram, counts):
        # Total the counts for each bigram
        yield bigram, sum(counts)

if __name__ == '__main__':
    BigramCount.run()
