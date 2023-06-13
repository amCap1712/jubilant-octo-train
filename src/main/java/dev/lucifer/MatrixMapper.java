package dev.lucifer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MatrixMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
            throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();

        int m = Integer.parseInt(conf.get("m"));
		int p = Integer.parseInt(conf.get("p"));

        String line = value.toString();
        String[] tokens = line.split(",");

        Text outputKey = new Text();
        Text outputValue = new Text();

        if (tokens[0].equals("A")) {
            // (A, i, j, Aij)
            for (int k = 1; k <= p; k++) {
                outputKey.set(tokens[1] + "," + k);  // (i, k)
                outputValue.set("A" + "," + tokens[2] + "," + tokens[3]);  // (A, j, Aij)
                context.write(outputKey, outputValue);
            }
        } else {
            // (B, j, k, Bjk)
			for (int i = 1; i <= m; i++) {
				outputKey.set(i + "," + tokens[2]);  // (i, k)
				outputValue.set("B" + "," + tokens[1] + "," + tokens[3]);  // (B, j, Bjk)
				context.write(outputKey, outputValue);
			}
        }
    }
}