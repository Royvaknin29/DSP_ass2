package mappers;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import writable.FinalKeyByDecade;

import java.io.IOException;
import java.text.DecimalFormat;

public class FifthMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {

	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// <key, value> format: < [word, decade], pmi>
		System.out.println("Mapping 5th time..");
		String[] primarySplit = value.toString().split("\t");
		DoubleWritable newKey = null;
		Text newValue = new Text();
		String[] rawWordInDecade = primarySplit[0].split(" ");
		if (Integer.parseInt(rawWordInDecade[2]) != 2000) {
			return;
		} else {
			for (double pmi = 1; pmi < 21; pmi += 1.25) {
				newKey = new DoubleWritable(pmi);
				newValue.set(primarySplit[1] + " " + rawWordInDecade[0] + " " + rawWordInDecade[1]);
				context.write(newKey, newValue);
			}
		}
	}

}