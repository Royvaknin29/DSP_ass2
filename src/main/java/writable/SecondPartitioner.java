package writable;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Partitioner;

public class SecondPartitioner<K2, V2> extends Partitioner<K2, V2> {

	public void configure(JobConf job) {
	}

	/** Use {@link Object#hashCode()} to partition. */
	public int getPartition(K2 key, V2 value, int numReduceTasks) {
		return 1;
	}
}
