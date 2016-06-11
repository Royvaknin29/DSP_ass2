package reducers;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;

import writable.SecondReduceOutput;
import writable.SeconderySortWritable;
import writable.WordsInDecadeWritable;

public class SecondReducer
		extends Reducer<WordsInDecadeWritable, SeconderySortWritable, WordsInDecadeWritable, SecondReduceOutput> {

	public void reduce(WordsInDecadeWritable key, Iterable<SeconderySortWritable> values, Context context)
			throws IOException, InterruptedException {
		WordsInDecadeWritable currentKeyWord = null;
		long currentCount = 0;
		System.out.println("Reducing: " + key.toString());
		for (SeconderySortWritable value : values) {
			WordsInDecadeWritable keyOut;
			SecondReduceOutput valueOut;
			WordsInDecadeWritable tempDollarKey;
			if (!value.hasWord()) {
				System.out.println("replacing currWord. was:" + currentKeyWord + " now:" + key.toString());
				valueOut = new SecondReduceOutput(value.getCount());
				keyOut = new WordsInDecadeWritable(key.word1, key.decade);
				currentKeyWord = key;
				currentCount = value.getCount();
			} else { // got a couple of words..
				System.out.println("current SeconderySortWritable is:" + value.toString());
				tempDollarKey = new WordsInDecadeWritable(key.word1 + '$', key.decade);
				if (!tempDollarKey.equals(currentKeyWord) || currentCount == 0) {
					// throw new IOException("Second Reduce Error - key does not
					// match!");
					System.out.println("Didn't write: " + key.word1 + " / " + currentKeyWord);
					continue;
				}
				valueOut = new SecondReduceOutput(currentKeyWord.word1, currentCount, value.getWord(),
						value.getCount());
				keyOut = new WordsInDecadeWritable(value.getWord(), key.decade);
			}
			context.write(keyOut, valueOut);
		}
	}
}
