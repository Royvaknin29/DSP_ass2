package reducers;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;

import writable.SecondReduceOutput;
import writable.SeconderySortWritable;
import writable.WordsInDecadeWritable;

public class SecondReducer
		extends Reducer<WordsInDecadeWritable, SeconderySortWritable, WordsInDecadeWritable, SecondReduceOutput> {
	private WordsInDecadeWritable currentKeyWord;
	private long currentCount;

	public void reduce(WordsInDecadeWritable key, Iterable<SeconderySortWritable> values, Context context)
			throws IOException, InterruptedException {
		System.out.println("Reducing: " + key.toString());
		for (SeconderySortWritable value : values) {
			WordsInDecadeWritable keyOut = null;
			SecondReduceOutput valueOut = null;
			if (!value.hasWord()) {
				System.out.println("replacing currWord. was:" + currentKeyWord + " now:" + key.word1);
				valueOut = new SecondReduceOutput(value.getCount());
				keyOut = new WordsInDecadeWritable(key.word1, key.decade);
				this.currentKeyWord = key;
				this.currentCount = value.getCount();
			} else { // got a couple of words..
				System.out.println("current SeconderySortWritable is:" + value.toString());
				if (!key.equals(currentKeyWord) || currentCount == 0) {
					// throw new IOException("Second Reduce Error - key does not
					// match!");
					System.out.println("Didn't write: " + key.word1 + " / " + currentKeyWord);
					continue;
				}
				valueOut = new SecondReduceOutput(this.currentKeyWord.word1, this.currentCount, value.getWord(),
						value.getCount());
				keyOut = new WordsInDecadeWritable(value.getWord(), key.decade);
			}
			context.write(keyOut, valueOut);
		}
	}
}
