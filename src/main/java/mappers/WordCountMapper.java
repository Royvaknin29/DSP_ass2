package mappers;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import writable.WordsInDecadeWritable;
import java.io.IOException;

public class WordCountMapper extends Mapper<LongWritable, Text, WordsInDecadeWritable, LongWritable> {

    private LongWritable count = new LongWritable();

    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        // value format: n-gram, year, occurrences, pages, books

        String[] split = value.toString().split("\t");
        if (split.length > 4) {
            int year = Integer.parseInt(split[1]);
            if (year >= 1900) {
                count.set(Long.parseLong(split[2]));
                String[] ngram = split[0].split(" ");
                if (ngram.length > 1) {
                    if (ngram.length == 2) {
                        if (!isStopWord(ngram[0])) {
                            context.write(new WordsInDecadeWritable(ngram[0], year), count);
                        }
                        if (!isStopWord(ngram[1])) {
                            context.write(new WordsInDecadeWritable(ngram[1], year), count);
                        }
                        if (!isStopWord(ngram[0]) && !isStopWord(ngram[1])) {
                            context.write(new WordsInDecadeWritable(ngram[0], ngram[1], year), count);
                        }
                    } else {
                        int middleIdx = (ngram.length / 2) + 1;
                        String middleWord = ngram[middleIdx];
                        if (!isStopWord(middleWord)) {
                            context.write(new WordsInDecadeWritable(middleWord, year), count);
                            for (String word : ngram) {
                                if (!isStopWord(word)) {
                                    context.write(new WordsInDecadeWritable(word, year), count);
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

    protected Boolean isStopWord(String word) {
        return false;
    }
}
