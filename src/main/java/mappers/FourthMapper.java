package mappers;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import writable.FinalKeyByDecade;

public class FourthMapper extends Mapper<LongWritable, Text, FinalKeyByDecade, Text> {

	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// <key, value> format: < [word, decade], pmi>
		System.out.println("Mapping 4th time..");
		String[] primarySplit = value.toString().split("\t");
		FinalKeyByDecade newKey = null;
		Text newValue = new Text();
		String[] rawWordInDecade = primarySplit[0].split(" ");
		newKey = new FinalKeyByDecade(Integer.parseInt(rawWordInDecade[2]), Double.parseDouble(primarySplit[1]));
		newValue.set(rawWordInDecade[0] + " " + rawWordInDecade[1]);
		context.write(newKey, newValue);
	}

}