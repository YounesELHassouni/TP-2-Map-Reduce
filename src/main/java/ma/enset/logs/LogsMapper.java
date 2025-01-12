package ma.enset.logs;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LogsMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    private String ipAdress;

    @Override
    protected void setup(Mapper<LongWritable, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {
        this.ipAdress = context.getConfiguration().get("ipAdress");
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {
        context.write(new Text("total-request"), new LongWritable(1));

        String[] tokens = value.toString().split(" -- ");
        String ip = tokens[0];
        if (!ip.equals(this.ipAdress)) {
            return;
        }
        int status = Integer.parseInt(tokens[1].split(" ")[6]);
        if (status != 200)
            return;
        context.write(new Text(ip), new LongWritable(1));

    }
}
