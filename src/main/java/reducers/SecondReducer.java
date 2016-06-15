package reducers;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;

import writable.SecondReduceOutput;
import writable.SeconderySortWritable;
import writable.WordsInDecadeWritable;

public class SecondReducer
		extends Reducer<WordsInDecadeWritable, SeconderySortWritable, WordsInDecadeWritable, SecondReduceOutput> {
	private DollarWordCountInDecade dollarCountByDecade;

	public void reduce(WordsInDecadeWritable key, Iterable<SeconderySortWritable> values, Context context)
			throws IOException, InterruptedException {

		System.out.println("Reducing: " + key.toString());
		for (SeconderySortWritable value : values) {
			WordsInDecadeWritable keyOut = null;
			SecondReduceOutput valueOut = null;
			WordsInDecadeWritable tempDollarKey;
			if (!value.hasWord()
					&& (dollarCountByDecade == null || !key.word1.equals(dollarCountByDecade.getWordWithDollar()))) {

				if (dollarCountByDecade != null) {
				}
				dollarCountByDecade = new DollarWordCountInDecade(key.word1);
				dollarCountByDecade.getCountByDecade().put(key.decade, value.getCount());
				valueOut = new SecondReduceOutput(value.getCount());
				keyOut = new WordsInDecadeWritable(key.word1, key.decade);
			} else if (!value.hasWord()) {
				dollarCountByDecade.getCountByDecade().put(key.decade, value.getCount());
				valueOut = new SecondReduceOutput(value.getCount());
				keyOut = new WordsInDecadeWritable(key.word1, key.decade);
			} else { // got a couple of words..
				tempDollarKey = new WordsInDecadeWritable(key.word1 + '$', key.decade);
				if (!tempDollarKey.word1.equals(dollarCountByDecade.getWordWithDollar())
						|| dollarCountByDecade.getCountByDecade().get(key.decade) == 0) {
					// throw new IOException("Second Reduce Error - key does not
					// match!");
					continue;
				}
				keyOut = new WordsInDecadeWritable(value.getWord(), key.decade);
				valueOut = new SecondReduceOutput(key.word1, dollarCountByDecade.getCountByDecade().get(key.decade),
						value.getWord(), value.getCount());
			}
			context.write(keyOut, valueOut);
		}
	}
}
