package com.hadooptutorial.wordcount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
* Here the key LongWritable represents the offset location of
* the current line being read from the Input Split of the given input file.
* Where the Text represents the actual current line itself.
*/
public class WCMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        StringTokenizer st = new StringTokenizer(line, " ");

        while(st.hasMoreTokens()){
            word.set(st.nextToken());
            context.write(word, one);
        }
    }
}
