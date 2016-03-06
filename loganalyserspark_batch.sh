#!/bin/bash

hadoop fs -rm -r /user/cloudera/loganalyzer/input

hadoop fs -mkdir -p /user/cloudera/loganalyzer/input

hadoop fs -put access_log /user/cloudera/loganalyzer/input/

spark-submit --class edu.mum.cs522.logs.LogAnalyzer --master local[1] LogAnalyzer-0.0.1-SNAPSHOT.jar /user/cloudera/loganalyzer/input/access_log /user/cloudera/loganalyzer/rescoderesult /user/cloudera/loganalyzer/topurlresult /user/cloudera/loganalyzer/ressizeresult /user/cloudera/loganalyzer/resavgsizeresult



