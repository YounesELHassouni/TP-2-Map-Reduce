package ma.enset.logs;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.stream.StreamSupport;

public class LogsReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException, IOException {
        long sum = StreamSupport.stream(values.spliterator(), false)
                .mapToLong(LongWritable::get)
                .sum();

        context.write(key, new LongWritable(sum));
    }
}
