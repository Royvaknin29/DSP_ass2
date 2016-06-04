package drivers;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;

import mappers.WordCountMapper;
import writable.WordsInDecadeWritable;

public class WordCountTest {
	public static final String HDFS_STOPWORD_LIST = "/data/stopWords.txt";
	public static final String STOPWORD_LIST = "stopWords.txt";

	public static void main(String[] args) throws Exception {
		System.out.println("Running Word Count Test v8 !");
		Configuration conf = new Configuration();
		System.out.println("uploading stopwords txt..");
		FileSystem fs = FileSystem.get(conf);
		Path hdfsPath = new Path(HDFS_STOPWORD_LIST);
		fs.copyFromLocalFile(false, true, new Path(STOPWORD_LIST), hdfsPath);
		DistributedCache.addCacheFile(hdfsPath.toUri(), conf);
		// conf.set("mapred.map.tasks","10");
		// conf.set("mapred.reduce.tasks","2");
		System.out.println("FINISHED uploading stopwords txt..");
		Job job = new Job(conf, "Word Count");
		System.out.println("Created new job!");
		job.setJarByClass(WordCountTest.class);
		System.out.println("JarByClass is set!");
		job.setMapperClass(WordCountMapper.class);
		System.out.println("MapperClass is set!");
		// job.setPartitionerClass(PartitionerClass.class);
		job.setCombinerClass(LongSumReducer.class);
		// System.out.println("CombinerClass is set!");
		job.setReducerClass(LongSumReducer.class);
		System.out.println("ReducerClass is set!");
		job.setMapOutputKeyClass(WordsInDecadeWritable.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(WordsInDecadeWritable.class);
		// System.out.println("OutputKeyClass is set!");
		job.setOutputValueClass(LongWritable.class);
		// System.out.println("OutputValueClass is set!");
		job.setInputFormatClass(SequenceFileInputFormat.class);
		// System.out.println("InputFormatClass is set!");
		// job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}