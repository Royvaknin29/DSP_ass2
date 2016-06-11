package writable;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SecondSortComperator extends WritableComparator {

	protected SecondSortComperator() {
		super(WordsInDecadeWritable.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		return super.compare(a, b);
	}

}
