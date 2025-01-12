package ma.enset;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;

public class VentesByVillesJob {

    private static final Logger logger = Logger.getLogger(VentesByVillesJob.class);

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        logger.info("Starting VentesByVillesJob");
        logger.info("input file: " + args[0]);

        // create MapReduce Job and setting name and jar class:
        Job ventesByVillesJob = Job.getInstance(new Configuration(), "VentesByVillesJob");
        ventesByVillesJob.setJarByClass(VentesByVillesJob.class);

        // set the input type which will be a text file:
        ventesByVillesJob.setInputFormatClass(TextInputFormat.class);

        // setting the appropriate mapper and reducer:
        ventesByVillesJob.setMapperClass(VentesVillesMapper.class);
        ventesByVillesJob.setReducerClass(VentesVillesReducer.class);

        // describe the output key and value types for mapping stage:
        ventesByVillesJob.setMapOutputKeyClass(Text.class);
        ventesByVillesJob.setMapOutputValueClass(DoubleWritable.class);

        // describe the output key and value types for reduce stage:
        ventesByVillesJob.setOutputKeyClass(Text.class);
        ventesByVillesJob.setOutputValueClass(DoubleWritable.class);

        // setting the input and output files:
        FileInputFormat.addInputPath(ventesByVillesJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(ventesByVillesJob, new Path(args[1]));

        // wait job to finish:
        ventesByVillesJob.waitForCompletion(false);
        logger.info("VentesByVillesJob completed");
        logger.info("Output file: " + args[1]);
    }
}
