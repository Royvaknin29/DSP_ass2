package drivers;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import mappers.WordCountMapper;
import reducers.WordCountReducer;

public class WordCountTest {

    public static void main(String[] args) throws Exception {
        System.out.println("Running Word Count Test v8 !");
        Configuration conf = new Configuration();
//        conf.set("mapred.map.tasks","10");
//        conf.set("mapred.reduce.tasks","2");
        Job job = new Job(conf, "Word Count");
        System.out.println("Created new job!");
        job.setJarByClass(WordCountTest.class);
        System.out.println("JarByClass is set!");
        job.setMapperClass(WordCountMapper.class);
        System.out.println("MapperClass is set!");
        // job.setPartitionerClass(PartitionerClass.class);
        job.setCombinerClass(WordCountReducer.class);
        System.out.println("CombinerClass is set!");
        job.setReducerClass(WordCountReducer.class);
        System.out.println("ReducerClass is set!");
        job.setOutputKeyClass(Text.class);
        System.out.println("OutputKeyClass is set!");
        job.setOutputValueClass(LongWritable.class);
        System.out.println("OutputValueClass is set!");
        job.setInputFormatClass(SequenceFileInputFormat.class);
        //System.out.println("InputFormatClass is set!");
        //job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}