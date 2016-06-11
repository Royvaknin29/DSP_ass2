
package writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class FinalKeyByDecade implements WritableComparable<FinalKeyByDecade> {
	public Integer decade;
	public Double PMI;

	public FinalKeyByDecade() {

	}

	public FinalKeyByDecade(Integer decade, Double PMI) {
		this.decade = decade;
		this.PMI = PMI;
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(decade);
		out.writeDouble(PMI);
	}

	public void readFields(DataInput in) throws IOException {
		decade = in.readInt();
		PMI = in.readDouble();
	}

	public String toString() {
		String res = "";
		res += Integer.toString(decade);
		res += " " + Double.toString(PMI);
		return res;
	}

	public int compareTo(FinalKeyByDecade o) {
		int res = 0;
		res = this.decade.compareTo(o.decade);
		if (res == 0) {
			return this.PMI.compareTo(o.PMI);
		} else {
			return res;
		}
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((PMI == null) ? 0 : PMI.hashCode());
		result = prime * result + ((decade == null) ? 0 : decade.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		FinalKeyByDecade other = (FinalKeyByDecade) obj;
		if (PMI == null) {
			if (other.PMI != null)
				return false;
		} else if (!PMI.equals(other.PMI))
			return false;
		if (decade == null) {
			if (other.decade != null)
				return false;
		} else if (!decade.equals(other.decade))
			return false;
		return true;
	}

}