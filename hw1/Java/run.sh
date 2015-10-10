#!/bin/bash
hadoop fs -rm -r ./java_citation

javac -d classes/ Citation.java
jar -cvf citation.jar -C classes/ ./
hadoop jar citation.jar org.myorg.Citation /data/patents/apat63_99.txt java_citation
