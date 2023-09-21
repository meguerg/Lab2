# -*- coding: utf-8 -*-
"""
Created on Thur Sep 21 11:41:28 2023

@author: meguerg
"""

from mrjob.job import MRJob
import re

# Splitting words by non-alphabetic characters
WORD_RE = re.compile(r"\b\w+\b")

class InvertedIndex(MRJob):

    def mapper(self, _, line):
    # Check if the line has the expected format
     if ":" in line:
        document_number, text = line.split(':', 1)
        words = re.findall(WORD_RE, text.lower())
        for word in words:
            yield (word, document_number.strip())
     else:
        # Handle lines with unexpected format (e.g., empty lines)
        pass
        

    def reducer(self, word, documents):
        # Create a set to store unique document IDs
        unique_document = set()

        # Add document ID to the set
        for document in documents:
            unique_document.add(document)

        # List the word and the list of documents where it appears
        yield (word, sorted(list(unique_document)))

if __name__ == '__main__':
    InvertedIndex.run()
