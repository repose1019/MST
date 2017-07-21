# -*-coding:utf-8-*- 
# @author: 'Repose' 
# @date: 2017/6/21

import pyspark
import sys
from operator import add

from pyspark import SparkContext


if __name__ == "__main__":
    sc = SparkContext(appName="PythonWordCount")
    lines = sc.textFile('words.txt')
    counts = lines.flatMap(lambda x: x.split(' ')) \
                  .map(lambda x: (x, 1)) \
                  .reduceByKey(add)
    output = counts.collect()
    for (word, count) in output:
        print( "%s: %i" % (word, count))

    sc.stop()

    # '''
    # D:\spark-2.0.0-bin-hadoop2.6\bin;
    # C:\ProgramData\Oracle\Java\javapath;
    # D:\ProgramData\Anaconda3;
    # D:\ProgramData\Anaconda3\Library\mingw-w64\bin;
    # D:\ProgramData\Anaconda3\Library\usr\bin;
    # D:\ProgramData\Anaconda3\Library\bin;
    # D:\ProgramData\Anaconda3\Scripts;
    # %SystemRoot%\system32;
    # %SystemRoot%;
    # %SystemRoot%\System32\Wbem;
    # %SYSTEMROOT%\System32\WindowsPowerShell\v1.0\;
    # C:\Program Files (x86)\Windows Kits\8.1\Windows Performance Toolkit\;
    # C:\Program Files\Java\jdk1.8.0_131\bin
    #
    #
    #     '''