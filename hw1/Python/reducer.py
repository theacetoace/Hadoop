#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
from itertools import groupby
from operator import itemgetter
import math

def unique(year, group, separator):
    keys = []
    for country, g in groupby(list(group)):
        if country not in keys:
            keys.append(country)
    print "%s%s%d" % (year, separator, len(keys))
    pass

def mean(lst):
    return float(sum(lst)) / len(lst)

def std(lst):
    average = mean(lst)
    return math.sqrt(float(sum((val - average) ** 2 for val in lst)) / len(lst)) 

def hist(year, group, separator):
    keys = {}
    for country, g in groupby(group):
        if country not in keys:
            keys[country] = 0
        keys[country] += len(list(g))
    vals = sorted([val for val in keys.values()])
    print "%s%s%d%s%d%s%f%s%d%s%f%s%f" % (year, separator,\
            len(keys), separator,\
            vals[0], separator,\
            vals[len(vals) / 2], separator,\
            vals[-1], separator,\
            mean(vals), separator,\
            std(vals))


commands = {
    'UniqueValueCount': unique,
    'ValueHistogram': hist,
}

def read_mapper_output(file, separator='\t'):
    res = []
    command = None
    for line in file:
        key = line.lstrip().split(':', 1)
        if not command:
            command = key[0]
        #print key
        res.append(key[1].rstrip().split(separator, 1))
    return res, command

def main(separator='\t'):
    data, command = read_mapper_output(sys.stdin)
    #print data
    for year, group in groupby(data, itemgetter(0)):
        try:
            if not command or year.startswith('"'):
                continue
            commands[command](year, [x[1] for x in list(group)], separator)
        except ValueError:
            pass

if __name__ == "__main__":
	main()
