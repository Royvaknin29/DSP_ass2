package reducers;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;

import drivers.WordCountTest;
import writable.SecondReduceOutput;
import writable.WordsInDecadeWritable;

public class ThirdReducer
		extends Reducer<WordsInDecadeWritable, SecondReduceOutput, WordsInDecadeWritable, DoubleWritable> {
	private DollarWordCountInDecade dollarCountByDecade;
	private Counters counters;

	public void reduce(WordsInDecadeWritable key, Iterable<SecondReduceOutput> values, Context context)
			throws IOException, InterruptedException {
		System.out.println("Reducing: " + key.toString());
		for (SecondReduceOutput value : values) {
			WordsInDecadeWritable keyOut = null;
			DoubleWritable valueOut = new DoubleWritable();
			WordsInDecadeWritable tempDollarKey;
			if (!value.isHasSecondWord()
					&& (dollarCountByDecade == null || !key.word1.equals(dollarCountByDecade.getWordWithDollar()))) {

				if (dollarCountByDecade != null) {
					System.out.println("replacing dollarCountByDecade. was:" + dollarCountByDecade.getWordWithDollar()
							+ " now:" + key.toString());
				}
				dollarCountByDecade = new DollarWordCountInDecade(key.word1);
				dollarCountByDecade.getCountByDecade().put(key.decade, value.getKeyWordCount());

			} else if (!value.isHasSecondWord()) {
				System.out.println("inserted into decade:" + key.decade + "count:" + value.getKeyWordCount());
				dollarCountByDecade.getCountByDecade().put(key.decade, value.getKeyWordCount());

			} else { // got a couple of words..
				System.out.println("current SeconderySortWritable is:" + value.toString());
				tempDollarKey = new WordsInDecadeWritable(key.word1 + '$', key.decade);
				if (!tempDollarKey.word1.equals(dollarCountByDecade.getWordWithDollar())
						|| dollarCountByDecade.getCountByDecade().get(key.decade) == 0) {
					System.out.println(
							"Didn't write: " + tempDollarKey.word1 + " / " + dollarCountByDecade.getWordWithDollar());
					continue;
				}

				System.out.println("Counter Value of " + "DECADE" + key.decade.toString() + "is: " + context
						.getCounter(WordCountTest.DecadeCounters.valueOf("DECADE" + key.decade.toString())).getValue());
				valueOut.set(
						calcPMI(value.getKeyWordCount(), dollarCountByDecade.getCountByDecade().get(key.decade),
								value.getCoupleCount(),
								// context.getConfiguration().getLong("TotalWordsInDecade$"
								// + key.decade.toString(), 100)));
								counters.findCounter(
										WordCountTest.DecadeCounters.valueOf("DECADE" + key.decade.toString()))
										.getValue()));
				keyOut = new WordsInDecadeWritable(value.getKeyWord(), key.word1, key.decade);
				context.write(keyOut, valueOut);
			}
		}
	}

	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		Cluster cluster = new Cluster(conf);
		Job currentJob = cluster.getJob(context.getJobID());
		counters = currentJob.getCounters();
	}

	private double calcPMI(long a, long b, long ab, long n) {

		return Math.log(ab) + Math.log(n) - Math.log(a) - Math.log(b);
	}
}
