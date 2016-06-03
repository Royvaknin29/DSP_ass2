package mappers;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
	//private static int numOfMapping = 0;
//	private static Set<String> STOP_WORDS = initializeStopWords();
	private LongWritable count = new LongWritable();

	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		// value format: n-gram, year, occurrences, pages, books
//		System.out.println("Mapping Line num: " + ++numOfMapping);
		System.out.println("Mapping..");
		String[] split = value.toString().split("\t");
		if (split.length > 4) {
			int year = Integer.parseInt(split[1]);
			if (year >= 1900) {
				count.set(Long.parseLong(split[2]));
				String[] ngram = split[0].split(" ");
				if (ngram.length > 1) {
					if (ngram.length == 2) {
						if (!isStopWord(ngram[0])) {
							context.write(new Text(ngram[0]), count);
						}
						if (!isStopWord(ngram[1])) {
							context.write(new Text(ngram[1]), count);
						}
						if (!isStopWord(ngram[0]) && !isStopWord(ngram[1])) {
							context.write(new Text(ngram[1]), count);
						}
					} else {
						int middleIdx = (ngram.length / 2);
						String middleWord = ngram[middleIdx];
						if (!isStopWord(middleWord)) {
							context.write(new Text(middleWord), count);
							
							for (String word : ngram) {
								if (!word.equals(middleWord) && !isStopWord(word)) {
									context.write(new Text(middleWord), count);
									context.write(new Text(middleWord), count);
								}
							}
						}
					}
				} else {
					return;
				}
			} else {
				return;
			}
		} else {
			return;
		}
	}

//	private static Set<String> initializeStopWords() {
//		System.out.println("Initializing Stop-Words Set!");
//		Set<String> stopWords = Sets.newHashSet();
//		Scanner in = null;
//		try {
//			in = new Scanner(new FileReader("stopWords.txt"));
//			while (in.hasNext()) {
//				stopWords.add(in.next());
//			}
//		} catch (FileNotFoundException e) {
//			e.printStackTrace();
//		} finally {
//			in.close();
//		}
//		System.out.println("Finished Initializing Stop-Words Set!");
//		return stopWords;
//	}

	protected Boolean isStopWord(String word) {
		return false;
		//		return STOP_WORDS.contains(word);
	}
}
