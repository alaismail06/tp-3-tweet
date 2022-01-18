package tn.isima.tp4;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SumPriceReducer
        extends Reducer<Text,IntWritable,IntWritable,Text,IntWritable,IntWritable> {

    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Iterable<IntWritable> values2, Context context) throws IOException, InterruptedException {
        integer sum = 0;
        for (IntWritable val : values) {
            System.out.println("value: "+val.get());
            sum += val.get();
        }
        System.out.println("--> somme de like de tous les infléuenseurs = "+sum);
        result.set(sum);
        context.write(key, result);
    }
}