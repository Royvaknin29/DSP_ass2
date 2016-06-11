package writable;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Partitioner;

public class SecondPartitioner<K2, V2> extends Partitioner<K2, V2> {

	public void configure(JobConf job) {
	}

	@Override
	public int getPartition(K2 key, V2 value, int numPartitions) {
		return 1 % numPartitions;
	}

	/** Use {@link Object#hashCode()} to partition. */

}
