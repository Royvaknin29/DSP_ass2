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
					System.out.println("replacing dollarCountByDecade. was:" + dollarCountByDecade.getWordWithDollar()
							+ " now:" + key.toString());
				}
				dollarCountByDecade = new DollarWordCountInDecade(key.word1);
				dollarCountByDecade.getCountByDecade().put(key.decade, value.getCount());
				valueOut = new SecondReduceOutput(value.getCount());
				keyOut = new WordsInDecadeWritable(key.word1, key.decade);
			} else if (!value.hasWord()) {
				System.out.println("inserted into decade:" + key.decade + "count:" + value.getCount());
				dollarCountByDecade.getCountByDecade().put(key.decade, value.getCount());
				valueOut = new SecondReduceOutput(value.getCount());
				keyOut = new WordsInDecadeWritable(key.word1, key.decade);
			} else { // got a couple of words..
				System.out.println("current SeconderySortWritable is:" + value.toString());
				tempDollarKey = new WordsInDecadeWritable(key.word1 + '$', key.decade);
				System.out.println("map is:" + dollarCountByDecade.getCountByDecade().toString());
				if (!tempDollarKey.word1.equals(dollarCountByDecade.getWordWithDollar())
						|| dollarCountByDecade.getCountByDecade().get(key.decade) == 0) {
					// throw new IOException("Second Reduce Error - key does not
					// match!");
					System.out.println(
							"Didn't write: " + tempDollarKey.word1 + " / " + dollarCountByDecade.getWordWithDollar());
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
