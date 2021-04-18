package com.hadooptutorial.wordcount;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class WCMain {

    public static void main(String[] args) throws Exception {
        System.out.println("WordCount with Hadoop...");

        if (args.length != 2){
            System.err.println("Usage: WCMain needs two arguments, input and output files");
            System.exit(-1);
        }

        Job job = new Job();
        job.setJarByClass(WCMain.class);
        job.setJobName("WordCounter");

        FileInputFormat.addInputPath(job, new Path("/Users/mli/Desktop/Projects/java/small_dataset_auth_log.txt"));
        FileOutputFormat.setOutputPath(job, new Path("wcoutput"));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(WCMapper.class);
        job.setReducerClass(WCReducer.class);

        int exitCode = job.waitForCompletion(true) ? 0: 1;

        if (job.isSuccessful()){
            System.out.println("Job was successful");
        } else {
            System.out.println("Job was not successful");
        }

        System.exit(exitCode);
    }
}
