# CIS4307
# HW1
# Student: Huajing Lin
# TU ID: 915660847
# Spring 2020

# input file: hw1_in.txt

import re
import sys
import time
import threading
from threading import Thread

def count_occurrence(lines_list, start_line, thread_index, segments, segment_lines, dict1):
    if(thread_index < segments):
        lines = lines_list[start_line : start_line + segment_lines];
    else:
        lines = lines_list[start_line:];

    #count lines
    line_count = len(lines);

    #count frequency occurrence of word in each line
    regex = r'\w+'
    for line in lines:
        words = re.findall(regex, line);
        for word in words:
            if(dict1.get(word) == None):
                dict1[word] = 1;
            else:
                val = dict1.get(word) + 1;
                dict1[word] = val;
    # output result
    print("thead %i: total line:%i, dictionary size: %i"%(thread_index,line_count, len(dict1)))
    
def main():
    # count the arguments
    arguments = len(sys.argv);
    #print ("the script is called with %i arguments" % (arguments))
    if(arguments != 3):
        print("usage: ~ hw1_in.txt segment_number\n");
        return;

    # print arguments
    #position = 1;
    #while (arguments > position):
    #    print ("parameter %i: %s" % (position, sys.argv[position]));
    #    position = position + 1;
    file_name = sys.argv[1];
    segments = int(sys.argv[2]);
    file_in = open(file_name,"r");
    
    #read all lines of text file
    lines_list = file_in.readlines();
    lines_total = len(lines_list);
    file_in.close();
    
    #dictionary for counting word frequency
    dict_results = {};
    dict_list = [{}] * segments;
    threads = [None] * segments;
    
    #line number of each segment
    segment_lines = lines_total // segments;
    print("file lines: %i, segment: %i,segment lines: %i"
          % (lines_total, segments, segment_lines));

    start_time = time.time()
    
    current_line = 0;
    for i in range(0, segments):
        threads[i] = Thread(target=count_occurrence,
                            args=(lines_list, current_line, i+1, segments, segment_lines, dict_list[i]) );
        threads[i].start();
        current_line += segment_lines;
        print("thread %i start"%(i))

    # to wait all threads finish
    for i in range(0, segments):
        threads[i].join();

    # merge dictionaries (using Python 3.5 syntax)
    for i in dict_list:
        dict_results = {**dict_results, **i}
    print("dictionary result size:%i"%(len(dict_results)))

    # write dictionary to file (total: 12053)
    file_out = open("hw1_out.txt","w");
    for key, value in sorted(dict_results.items()):
        file_out.write('%s:%s\n' % (key, value))
    file_out.close();
    print("--- %s seconds ---" % (time.time() - start_time))
    
main();
