package drivers;

/**
 * Created by aaronv on 21/05/2016.
 */

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import mappers.WordCountMapper;
import reducers.WordCountReducer;

public class WordCount extends Configured implements Tool {

    private final String CORPUS_PATH = "";
    private final String OUTPUT_PATH = "";

    public int run(String[] args) throws Exception {
        JobConf conf = new JobConf(getConf(), WordCount.class);
        conf.setJobName("WordCount");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(WordCountMapper.class);
        conf.setReducerClass(WordCountReducer.class);

        FileInputFormat.addInputPath(conf, new Path(CORPUS_PATH));
        FileOutputFormat.setOutputPath(conf, new Path(OUTPUT_PATH));

        JobClient.runJob(conf);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new WordCount(),args);
        System.exit(res);
    }
}