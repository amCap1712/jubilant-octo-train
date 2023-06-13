package dev.lucifer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MatrixReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
            throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();

        int n = Integer.parseInt(conf.get("n"));
        // we are indexing matrix from 1 but java arrays are indexed from 0
        int[] A = new int[n + 1];
        int[] B = new int[n + 1];

        for (Text value: values) {
            String[] tokens = value.toString().split(",");
            int j = Integer.parseInt(tokens[1]);
            int number = Integer.parseInt(tokens[2]);
            if (tokens[0].equals("A")) {
                A[j] = number;
            } else {
                B[j] = number;
            }
        }

        int c_ik = 0;
        for (int j = 1; j <= n; j++) {
            c_ik += A[j] * B[j];
        }
        context.write(null, new Text("C," + key.toString() + "," + c_ik));
    }
}