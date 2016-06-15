package drivers;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;

import mappers.FourthMapper;
import mappers.SecondMapper;
import mappers.ThirdMapper;
import mappers.WordCountMapper;
import reducers.FourthReducer;
import reducers.SecondReducer;
import reducers.ThirdReducer;
import writable.FinalKeyByDecade;
import writable.SecondReduceOutput;
import writable.SecondSortComperator;
import writable.SeconderySortWritable;
import writable.WordsInDecadeWritable;

public class WordCountTest {
	public static enum DecadeCounters {
		DECADE1900, DECADE1910, DECADE1920, DECADE1930, DECADE1940, DECADE1950, DECADE1960, DECADE1970, DECADE1980, DECADE1990, DECADE2000
	}

	public static void main(String[] args) throws Exception {
		System.out.println("hi! =]");
		Job firstJob = initFirstJob(args[0], args[1] + "tmp1");
		firstJob.waitForCompletion(true);
		Job secondJob = initSecondJob(args[1] + "tmp1", args[1] + "tmp2");
		secondJob.waitForCompletion(true);
		Job thirdJob = initThirdJob(args[1] + "tmp2", args[1] + "tmp3");
		thirdJob.waitForCompletion(true);
		Job fourthJob = initFourthJob(args[1] + "tmp3", args[1], Integer.parseInt(args[2]));
		fourthJob.waitForCompletion(true);
		Map<Double, Double> fMeasures = FMeasureUtils.getFMeasure(args[1] + "tmp3");
		System.out.println("Fmeasures are: " + fMeasures.toString());
		System.exit(0);

	}

	public static Job initFifth(String in, String out, int k) throws IllegalArgumentException, IOException {
		System.out.println("initializing fifth job..");
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Word Count2");
		job.setJarByClass(WordCountTest.class);
		job.setMapperClass(FourthMapper.class);
		// job.setCombinerClass(SecondReducer.class);
		// job.setPartitionerClass(SecondPartitioner.class);
		job.setReducerClass(FifthReducer.class);
		job.setMapOutputKeyClass(FinalKeyByDecade.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(FinalKeyByDecade.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(in));
		FileOutputFormat.setOutputPath(job, new Path(out));

		return job;
	}

	public static Job initFourthJob(String in, String out, int k) throws IllegalArgumentException, IOException {
		System.out.println("initializing fourth job..");
		Configuration conf = new Configuration();
		conf.setInt("k", k);
		Job job = new Job(conf, "Word Count2");
		job.setJarByClass(WordCountTest.class);
		job.setMapperClass(FourthMapper.class);
		// job.setCombinerClass(SecondReducer.class);
		// job.setPartitionerClass(SecondPartitioner.class);
		job.setReducerClass(FourthReducer.class);
		job.setMapOutputKeyClass(FinalKeyByDecade.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(FinalKeyByDecade.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(in));
		FileOutputFormat.setOutputPath(job, new Path(out));
		System.out.println("fourth job created!");
		return job;
	}

	public static Job initThirdJob(String in, String out) throws IllegalArgumentException, IOException {
		System.out.println("initializing third job..");
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Word Count2");
		job.setJarByClass(WordCountTest.class);
		job.setMapperClass(ThirdMapper.class);
		// job.setCombinerClass(SecondReducer.class);
		// job.setPartitionerClass(SecondPartitioner.class);
		job.setReducerClass(ThirdReducer.class);
		job.setMapOutputKeyClass(WordsInDecadeWritable.class);
		job.setMapOutputValueClass(SecondReduceOutput.class);
		job.setSortComparatorClass(SecondSortComperator.class);
		job.setOutputKeyClass(WordsInDecadeWritable.class);
		job.setOutputValueClass(DoubleWritable.class);

		FileInputFormat.addInputPath(job, new Path(in));
		FileOutputFormat.setOutputPath(job, new Path(out));
		System.out.println("third job created!");

		return job;
	}

	public static Job initSecondJob(String in, String out) throws IllegalArgumentException, IOException {
		System.out.println("initializing second job..");
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Job job = new Job(conf, "Word Count2");
		job.setJarByClass(WordCountTest.class);
		job.setMapperClass(SecondMapper.class);
		// job.setCombinerClass(SecondReducer.class);
		// job.setPartitionerClass(SecondPartitioner.class);
		job.setReducerClass(SecondReducer.class);
		job.setMapOutputKeyClass(WordsInDecadeWritable.class);
		job.setMapOutputValueClass(SeconderySortWritable.class);
		job.setSortComparatorClass(SecondSortComperator.class);
		job.setOutputKeyClass(WordsInDecadeWritable.class);
		job.setOutputValueClass(SecondReduceOutput.class);

		FileInputFormat.addInputPath(job, new Path(in));
		FileOutputFormat.setOutputPath(job, new Path(out));
		System.out.println("second job created!");
		return job;
	}

	public static Job initFirstJob(String in, String out) throws IllegalArgumentException, IOException {
		System.out.println("initializing first job..");
		Configuration conf = new Configuration();
		Job job = new Job(conf, "job1");
		job.setJarByClass(WordCountTest.class);
		job.setMapperClass(WordCountMapper.class);
		job.setCombinerClass(LongSumReducer.class);
		job.setReducerClass(LongSumReducer.class);
		job.setMapOutputKeyClass(WordsInDecadeWritable.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(WordsInDecadeWritable.class);
		job.setOutputValueClass(LongWritable.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(in));
		FileOutputFormat.setOutputPath(job, new Path(out));
		System.out.println("first job created!");
		return job;
	}
}