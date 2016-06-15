package mappers;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import drivers.WordCountTest;
import writable.SecondReduceOutput;
import writable.WordsInDecadeWritable;

public class ThirdMapper extends Mapper<LongWritable, Text, WordsInDecadeWritable, SecondReduceOutput> {
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// <key, value> format: < [word, decade], [word1, count, (word1, word2,
		// count)] / count>
		System.out.println("Mapping 3rd time: " + value.toString());
		String[] primarySplit = value.toString().split("\t");
		WordsInDecadeWritable newKey;
		SecondReduceOutput newValue;
		if (primarySplit.length > 1) {
			String[] rawWordInDecade = primarySplit[0].split(" ");
			String[] rawSecondReduceOutput = primarySplit[1].split(" ");
			if (rawSecondReduceOutput.length > 1) {
				System.out.println("Multi values..word: " + rawWordInDecade[0] + "decade: " + rawWordInDecade[1]
						+ "value " + rawSecondReduceOutput[0]);

				newKey = new WordsInDecadeWritable(rawWordInDecade[0], Integer.parseInt(rawWordInDecade[1]));
				newValue = new SecondReduceOutput(rawSecondReduceOutput[0].replaceAll("[^\\w\\s]", ""),
						Long.parseLong(rawSecondReduceOutput[1].replaceAll("[^\\w\\s]", "")),
						rawSecondReduceOutput[3].replaceAll("[^\\w\\s]", ""),
						Long.parseLong(rawSecondReduceOutput[4].replaceAll("[^\\w\\s]", "")));
			} else {
				System.out.println("Single value..word: " + rawWordInDecade[0] + "decade: " + rawWordInDecade[1]
						+ "value " + rawSecondReduceOutput[0]);
				newKey = new WordsInDecadeWritable(rawWordInDecade[0], Integer.parseInt(rawWordInDecade[1]));
				newValue = new SecondReduceOutput(Long.parseLong(rawSecondReduceOutput[0]));
				if (rawWordInDecade[0].equals("TotalWordsInDecade$")) {
					// context.getConfiguration().setLong(rawWordInDecade[0] +
					// rawWordInDecade[1],
					// Long.parseLong(rawSecondReduceOutput[0]));
					context.getCounter(WordCountTest.DecadeCounters.valueOf("DECADE" + rawWordInDecade[1]))
							.increment(Long.parseLong(rawSecondReduceOutput[0]));
				}
			}

		} else {
			return;
		}
		context.write(newKey, newValue);
	}

}
