package mappers;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;

import com.google.common.collect.Lists;

import writable.WordsInDecadeWritable;

public class WordCountMapper extends Mapper<LongWritable, Text, WordsInDecadeWritable, LongWritable> {
	// private static int numOfMapping = 0;

	public static final String HDFS_STOPWORD_LIST = "/data/stopWords.txt";
	private Set<String> stopWords;
	private LongWritable count = new LongWritable();

	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		// value format: n-gram, year, occurrences, pages, books
		// System.out.println("Mapping Line num: " + ++numOfMapping);
		System.out.println("Mapping..");
		String[] split = value.toString().split("\t");
		if (split.length > 4) {
			int year = Integer.parseInt(split[1]);
			if (year >= 1900) {
				count.set(Long.parseLong(split[2]));
				String[] ngram = split[0].split(" ");
				List<String> validWords = Lists.newArrayList();
				for (int i = 0; i < ngram.length; i++) {
					ngram[i] = ngram[i].toLowerCase().replaceAll("[^\\w\\s]", "");
					if (ngram[i].length() > 0) {
						validWords.add(ngram[i]);
					}
				}
				if (validWords.size() > 1) {
					context.write(new WordsInDecadeWritable("TotalWordsInDecade", year),
							new LongWritable(count.get() * Long.valueOf(validWords.size())));
					if (validWords.size() == 2) {
						if (!isStopWord(validWords.get(0))) {
							context.write(new WordsInDecadeWritable(validWords.get(0), year), count);
						}
						if (!isStopWord(validWords.get(1))) {
							context.write(new WordsInDecadeWritable(validWords.get(1), year), count);
						}
						if (!isStopWord(validWords.get(0)) && !isStopWord(validWords.get(1))) {
							context.write(new WordsInDecadeWritable(validWords.get(0), validWords.get(1), year), count);
						}
					} else {
						int middleIdx = (validWords.size() / 2);
						String middleWord = validWords.get(middleIdx);
						if (!isStopWord(middleWord)) {
							context.write(new WordsInDecadeWritable(middleWord, year), count);
							for (String word : validWords) {
								if (!word.equals(middleWord) && !isStopWord(word)) {
									context.write(new WordsInDecadeWritable(middleWord, year), count);
									context.write(new WordsInDecadeWritable(word, middleWord, year), count);
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

	@Override
	protected void setup(Mapper<LongWritable, Text, WordsInDecadeWritable, LongWritable>.Context context)
			throws IOException, InterruptedException {
		System.out.println("STARTING CONFIGURE!!!");
		try {
			String stopwordCacheName = new Path(HDFS_STOPWORD_LIST).getName();
			Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			if (null != cacheFiles && cacheFiles.length > 0) {
				for (Path cachePath : cacheFiles) {
					if (cachePath.getName().equals(stopwordCacheName)) {
						System.out.println("CALLING LOAD!!!");
						loadStopWords(cachePath);
						break;
					}
				}
			}
		} catch (IOException ioe) {
			System.err.println("IOException reading from distributed cache");
			System.err.println(ioe.toString());
		}
	}

	void loadStopWords(Path cachePath) throws IOException {
		// note use of regular java.io methods here - this is a local file now
		BufferedReader wordReader = new BufferedReader(new FileReader(cachePath.toString()));
		try {
			String line;
			this.stopWords = new HashSet<String>();
			while ((line = wordReader.readLine()) != null) {
				this.stopWords.add(line);
			}
		} finally {
			wordReader.close();
		}
		System.out.println("FINISHED LOAD!!!");

	}

	protected Boolean isStopWord(String word) {
		return stopWords.contains(word);
	}
}
