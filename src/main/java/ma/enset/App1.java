package ma.enset;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

public class App1 {
    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://namenode:8020");

        FileSystem fs = FileSystem.get(conf);

        FSDataOutputStream fsout = fs.create(new Path("/ventes.txt"));
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fsout));
        bw.write("12/10/2024 Casablanca Product1 1000 \n");
        bw.write("13/10/2024 Mohammedia Product2 2000 \n");
        bw.write("20/10/2024 Mohammedia Product3 3000 \n");
        bw.write("22/09/2024 Rabat Product4 4500 \n");
        bw.write("22/08/2024 Casablanca Product5 500 \n");
        bw.close();


    }
}
