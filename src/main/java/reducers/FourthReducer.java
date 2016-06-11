package reducers;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import writable.FinalKeyByDecade;

public class FourthReducer extends Reducer<FinalKeyByDecade, Text, FinalKeyByDecade, Text> {
	// TODO: get from main calss parameter K, write out the first K entries for
	// each decade...
	public void reduce(FinalKeyByDecade key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		System.out.println("Reducing: " + key.toString());
		// long sum = 0;
		// for (LongWritable value : values) {
		// sum += value.get();
		// }
		for (Text value : values) {
			context.write(key, value);
		}
	}
}
