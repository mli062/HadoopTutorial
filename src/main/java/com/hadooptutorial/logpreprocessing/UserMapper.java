package com.hadooptutorial.logpreprocessing;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class UserMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    public static final Log log = LogFactory.getLog(IpMapper.class);
    private final static IntWritable one = new IntWritable(1);
    private Text user = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        if (line.contains("Failed password for")) {
            Pattern usernamePattern = Pattern.compile("for invalid user (\\w+)");
            Matcher usernameMatcher = usernamePattern.matcher(line);
            String username = usernameMatcher.find() ? usernameMatcher.group().split(" ")[3] : "root";

            user.set(username);
            //log.info("Preprocess ip: " + extract_ip);
            context.write(user, one);
        }
    }
}