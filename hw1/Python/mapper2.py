#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys

index1 = int(sys.argv[1])
index2 = int(sys.argv[2])
for line in sys.stdin:
    if line.startswith('"'): continue
    fields = line.split(',')
#    print "UniqValueCount:%s\t%s" % (fields[index1],fields[index2])
    print "ValueHistogram:%s\t%s" % (fields[index1],fields[index2])
'''
1 Количество уникальных
2 Минимальное значение
3 Медиана
4 Максимальное значение
5 Среднее значение
6 Стандартное откланение
'''

