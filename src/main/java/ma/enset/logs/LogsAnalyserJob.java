package ma.enset.logs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;

public class LogsAnalyserJob {
    private static final Logger logger = Logger.getLogger(LogsAnalyserJob.class);

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        logger.info("Starting LogsAnalyserJob");
        logger.info("input file: " + args[0]);
        logger.info("cherche logs pour IpAddress: " + args[2]);

        Configuration conf = new Configuration();
        conf.set("ipAdress", args[2]);
        // create MapReduce Job and setting name and jar class:
        Job logsAnalyserJob = Job.getInstance(conf, "LogsAnalyserJob");
        logsAnalyserJob.setJarByClass(LogsAnalyserJob.class);

        // set the input type which will be a text file:
        logsAnalyserJob.setInputFormatClass(TextInputFormat.class);

        // setting the appropriate mapper and reducer:
        logsAnalyserJob.setMapperClass(LogsMapper.class);
        logsAnalyserJob.setReducerClass(LogsReducer.class);

        // describe the output key and value types for mapping stage:
        logsAnalyserJob.setMapOutputKeyClass(Text.class);
        logsAnalyserJob.setMapOutputValueClass(LongWritable.class);

        // describe the output key and value types for reduce stage:
        logsAnalyserJob.setOutputKeyClass(Text.class);
        logsAnalyserJob.setOutputValueClass(LongWritable.class);

        // setting the input and output files:
        FileInputFormat.addInputPath(logsAnalyserJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(logsAnalyserJob, new Path(args[1]));

        // wait job to finish:
        logsAnalyserJob.waitForCompletion(true);
        logger.info("LogsAnalyserJob completed");
        logger.info("Output file: " + args[1]);
    }


}
