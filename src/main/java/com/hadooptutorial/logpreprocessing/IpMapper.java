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


public class IpMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    public static final Log log = LogFactory.getLog(IpMapper.class);
    private final static IntWritable one = new IntWritable(1);
    private Text ip = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        if (line.contains("Failed password for")) {
            Pattern ipPattern = Pattern.compile("\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}");
            Matcher ipMatcher = ipPattern.matcher(line);
            String extract_ip = ipMatcher.find() ? ipMatcher.group() : null;

            ip.set(extract_ip);
            //log.info("Preprocess ip: " + extract_ip);
            context.write(ip, one);
        }
    }

    /*private static String extractData(String s) throws ParseException {
        // Extract Date from string
        String dateString = s.substring(0, 15);
        SimpleDateFormat formatter = new SimpleDateFormat("MMM dd HH:mm:ss", Locale.ENGLISH);
        Date date = formatter.parse(dateString);

        // Extract Username from string
        Pattern usernamePattern = Pattern.compile("for invalid user (\\w+)");
        Matcher usernameMatcher = usernamePattern.matcher(s);
        String username = usernameMatcher.find() ? usernameMatcher.group().split(" ")[3] : "root";

        // Extract IP from string
        Pattern ipPattern = Pattern.compile("\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}");
        Matcher ipMatcher = ipPattern.matcher(s);
        String ip = ipMatcher.find() ? ipMatcher.group() : null;

        // Extract
        Pattern portPattern = Pattern.compile("port (\\d{5})");
        Matcher portMatcher = portPattern.matcher(s);
        String port = portMatcher.find() ? portMatcher.group().split(" ")[1] : null;
        return date.toString() + "," + username + "," + ip + "," + port;
    }*/
}