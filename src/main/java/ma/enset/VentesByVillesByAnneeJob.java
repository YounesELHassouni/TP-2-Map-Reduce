package ma.enset;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;

import java.io.IOException;

public class VentesByVillesByAnneeJob {
    private static final Logger logger = org.slf4j.LoggerFactory.getLogger(VentesByVillesByAnneeJob.class);

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        logger.info("Starting VentesByVillesByAnneeJob");
        logger.info("input file: " + args[0]);
        logger.info("cherche ventes pour l'annee: " + args[2]);

        Configuration conf = new Configuration();
        conf.set("annee", args[2]);

        // create MapReduce Job and setting name and jar class:
        Job ventesByVillesByAnneeJob = Job.getInstance(conf, "VentesByVillesByAnneeJob");
        ventesByVillesByAnneeJob.setJarByClass(VentesByVillesByAnneeJob.class);

        // set the input type which will be a text file:
        ventesByVillesByAnneeJob.setInputFormatClass(TextInputFormat.class);

        // setting the appropriate mapper and reducer:
        ventesByVillesByAnneeJob.setMapperClass(VentesByVillesByAnneeMapper.class);
        ventesByVillesByAnneeJob.setReducerClass(VentesVillesReducer.class);

        // describe the output key and value types for mapping stage:
        ventesByVillesByAnneeJob.setMapOutputKeyClass(Text.class);
        ventesByVillesByAnneeJob.setMapOutputValueClass(DoubleWritable.class);

        // describe the output key and value types for reduce stage:
        ventesByVillesByAnneeJob.setOutputKeyClass(Text.class);
        ventesByVillesByAnneeJob.setOutputValueClass(DoubleWritable.class);

        // setting the input and output files:
        FileInputFormat.addInputPath(ventesByVillesByAnneeJob, new org.apache.hadoop.fs.Path(args[0]));
        FileOutputFormat.setOutputPath(ventesByVillesByAnneeJob, new org.apache.hadoop.fs.Path(args[1]));

        // wait job to finish:
        ventesByVillesByAnneeJob.waitForCompletion(false);
        logger.info("VentesByVillesByAnneeJob completed");
        logger.info("Output file: " + args[1]);


    }
}
