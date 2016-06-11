package writable;

import org.apache.hadoop.mapreduce.Partitioner;


public class SecondPartitioner extends Partitioner<WordsInDecadeWritable, SeconderySortWritable>{

    @Override
    public int getPartition(WordsInDecadeWritable key, SeconderySortWritable value, int numReduceTasks) {

        String cleanKeyWord = key.word1.toLowerCase().replaceAll("[^\\w\\s]", "");

        if (numReduceTasks == 0) {
            return 0;
        } else {
            return (cleanKeyWord.hashCode() + key.decade.hashCode()) % numReduceTasks;
        }

    }
}
