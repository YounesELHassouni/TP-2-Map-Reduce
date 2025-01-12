package ma.enset;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class VentesByVillesByAnneeMapper extends Mapper<LongWritable,Text, Text, DoubleWritable> {
    int annee;

    @Override
    protected void setup(Mapper<LongWritable, Text, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
        annee = context.getConfiguration().getInt("annee", 2020);
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(" ");
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd-MM-yyyy");
        LocalDate date = LocalDate.parse(tokens[0], formatter);
        String ville = tokens[1];
        double amount = Double.parseDouble(tokens[3]);
        if (date.getYear() != annee)
            return;

        double price = Double.parseDouble(tokens[3]);
        context.write(new Text(tokens[1]), new DoubleWritable(price));

    }
}
