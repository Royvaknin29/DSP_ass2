package reducers;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import writable.WordsInDecadeWritable;

public class WordCountReducer extends Reducer<WordsInDecadeWritable, IntWritable, WordsInDecadeWritable, LongWritable> {

    public void reduce(WordsInDecadeWritable key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {
        System.out.println("Reducing: " + key.toString());
    	long sum = 0;
        for (LongWritable value : values) {
            sum += value.get();
        }
        context.write(key, new LongWritable(sum));
    }
}
