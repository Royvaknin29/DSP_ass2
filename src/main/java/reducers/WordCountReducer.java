package reducers;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer<KEY> extends Reducer<KEY, MapWritable, KEY, MapWritable> {

    private MapWritable mapResult = new MapWritable();
    private IntWritable decade = new IntWritable();
    private LongWritable count = new LongWritable();

    public void reduce(KEY key, Iterable<MapWritable> values, Context context)
            throws IOException, InterruptedException {
        int i;
        long currentCount;
        long countToAdd;
        long sum;
        for (i = 1900; i <= 2010; i += 10) {
            decade.set(i);
            count.set(0);
            mapResult.put(decade, count);
        }
        for (MapWritable val : values) {
            for (i = 1900; i <= 2010; i += 10) {
                decade.set(i);
                if (val.containsKey(decade)) {
                    currentCount = ((LongWritable) mapResult.get(decade)).get();
                    countToAdd = ((LongWritable) val.get(decade)).get();
                    sum = currentCount + countToAdd;
                    count.set(sum);
                    mapResult.put(decade, count);
                }
            }
        }
        context.write(key, mapResult);
    }
}
