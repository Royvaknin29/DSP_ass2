package reducers;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import writable.SecondReduceOutput;
import writable.SeconderySortWritable;
import writable.WordsInDecadeWritable;

import java.io.IOException;

public class ThirdReducer
		extends Reducer<WordsInDecadeWritable, SecondReduceOutput, WordsInDecadeWritable, DoubleWritable> {

	public void reduce(WordsInDecadeWritable key, Iterable<SecondReduceOutput> values, Context context)
			throws IOException, InterruptedException {
		System.out.println("Reducing: " + key.toString());
		WordsInDecadeWritable currentKeyWord = null;
		long currentCount = 0;
		for (SecondReduceOutput value : values) {
			WordsInDecadeWritable keyOut = null;
			DoubleWritable valueOut = null;
			WordsInDecadeWritable tempDollarKey;
			if (!value.hasSecondWord) {
				System.out.println("replacing currWord. was:" + currentKeyWord + " now:" + key);
				currentKeyWord = key;
				currentCount = value.keyWordCount;
			} else { // got a couple of words..
				System.out.println("current SeconderySortWritable is:" + value.toString());
				tempDollarKey = new WordsInDecadeWritable(key.word1 + '$', key.decade);
				if (!tempDollarKey.equals(currentKeyWord) || currentCount == 0) {
					// throw new IOException("Second Reduce Error - key does not
					// match!");
					System.out.println("Didn't write: " + key.word1 + " / " + currentKeyWord);
					continue;
				}
				valueOut.set(calcPMI(value.keyWordCount, currentCount, value.coupleCount, 1000));
				keyOut = new WordsInDecadeWritable(value.keyWord, key.word1, key.decade);
				context.write(keyOut, valueOut);
			}
		}
	}

	private double calcPMI(long a, long b, long ab, long n) {
		return  Math.log(ab) + Math.log(n) - Math.log(a) - Math.log(b);
	}
}
