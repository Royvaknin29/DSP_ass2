package reducers;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import writable.FinalKeyByDecade;

public class FourthReducer extends Reducer<FinalKeyByDecade, Text, FinalKeyByDecade, Text> {

	private int currentDecade = 0;
	private int i = 0;

	public void reduce(FinalKeyByDecade key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		if (currentDecade != key.decade) {
			currentDecade = key.decade;
			i = 0;
		}
		int k = context.getConfiguration().getInt("k", 10);

		for (Text value : values) {
			if (i < k) {
				i++;
				context.write(key, value);
			} else {
				break;
			}
		}
	}
}
