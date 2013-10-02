#!/usr/bin/env python
import mincemeat
import glob
import os
import sys

all_files = glob.glob('files/*')

def file_contents(file_name):
    f = open(file_name)
    try:
        return f.read()
    finally:
        f.close()

# The data source can be any dictionary-like object
datasource = dict((file_name, file_contents(file_name))
                  for file_name in all_files)

def mapfn(k, v):
    #contents= []
    #dict={}
    import stopwords
    for line in v.splitlines():
        wordlist=line.split(':::')
        authorlist=wordlist[1].split('::')
        title =wordlist[2].rstrip('.')
        title =title.split(' ')
        for author in authorlist:
            for term in title:
                term = term.lower()
                if term not in stopwords.allStopWords and len(term) > 1:
                        yield (author,term), 1




def reducefn(k, vs):
    result = sum(vs)
    return result

s = mincemeat.Server()
s.datasource = datasource
s.mapfn = mapfn
s.reducefn = reducefn

results = s.run_server(password="changeme")
print results
