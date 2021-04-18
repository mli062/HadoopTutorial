package com.hadooptutorial.logpreprocessing;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class LPMain {

    public static void main(String[] args) throws Exception {
        System.out.println("Log Preprocessing with Hadoop...");

        if (args.length != 2){
            System.err.println("Usage: LPMain needs two arguments, input and output files");
            System.exit(-1);
        }

        Job job = new Job();
        job.setJarByClass(LPMain.class);
        job.setJobName("IpCounter");

        FileInputFormat.addInputPath(job, new Path("auth_log_test_dataset.txt"));
        FileOutputFormat.setOutputPath(job, new Path("result/lpoutput"));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(LPMapper.class);
        job.setReducerClass(LPReducer.class);

        int exitCode = job.waitForCompletion(true) ? 0: 1;

        if (job.isSuccessful()){
            System.out.println("Job was successful");
        } else {
            System.out.println("Job was not successful");
        }

        System.exit(exitCode);
    }
}
