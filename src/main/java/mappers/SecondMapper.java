package mappers;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import writable.SeconderySortWritable;
import writable.WordsInDecadeWritable;

public class SecondMapper extends Mapper<LongWritable, Text, WordsInDecadeWritable, SeconderySortWritable> {

	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// <key, value> format: < [word1, word2*, decade], count>
		String[] primarySplit = value.toString().split("\t");
		WordsInDecadeWritable words = null;
		Long count = null;
		if (primarySplit.length > 1) {
			count = Long.valueOf(primarySplit[1]);
			String[] rawWordInDecade = primarySplit[0].split(" ");
			if (rawWordInDecade.length > 2) {
				words = new WordsInDecadeWritable(rawWordInDecade[0], rawWordInDecade[1],
						Integer.valueOf(rawWordInDecade[2]));
			} else if (rawWordInDecade.length == 2) {
				words = new WordsInDecadeWritable(rawWordInDecade[0], Integer.valueOf(rawWordInDecade[1]));
			} else {
				return;
			}
		} else {
			return;
		}
		System.out.println("Mapped: " + value);
		WordsInDecadeWritable keyToWrite = null;
		SeconderySortWritable valueToWrite = null;
		if (!words.isCouple) {
			keyToWrite = new WordsInDecadeWritable(words.word1 + '$', words.decade);
			valueToWrite = new SeconderySortWritable(count);
		} else {
			keyToWrite = new WordsInDecadeWritable(words.word1, words.decade);
			valueToWrite = new SeconderySortWritable(words.word2, count);
		}
		context.write(keyToWrite, valueToWrite);
	}

}
