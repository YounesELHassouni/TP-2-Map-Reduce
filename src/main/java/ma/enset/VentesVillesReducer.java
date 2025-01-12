package ma.enset;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.stream.StreamSupport;



public class VentesVillesReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {

        double sum = StreamSupport.stream(values.spliterator(), false)
                .mapToDouble(DoubleWritable::get)
                .sum();

        context.write(key, new DoubleWritable(sum));
    }
}
