
package writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class WordsInDecadeWritable implements WritableComparable<WordsInDecadeWritable> {
	public String word1;
	public String word2;
	public Integer decade;
	public Boolean isCouple;

	public WordsInDecadeWritable() {

	}

	public WordsInDecadeWritable(String word1, String word2, Integer decade) {
		if (word1.compareTo(word2) < 0) {
			this.word1 = word1;
			this.word2 = word2;
		} else {
			this.word1 = word2;
			this.word2 = word1;
		}
		this.decade = (int) Math.floor(decade / 10) * 10;
		this.isCouple = true;
	}

	public WordsInDecadeWritable(String word1, Integer decade) {
		this.word1 = word1;
		this.word2 = null;
		this.decade = (int) Math.floor(decade / 10) * 10;
		this.isCouple = false;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((decade == null) ? 0 : decade.hashCode());
		result = prime * result + ((isCouple == null) ? 0 : isCouple.hashCode());
		result = prime * result + ((word1 == null) ? 0 : word1.hashCode());
		result = prime * result + ((word2 == null) ? 0 : word2.hashCode());
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
		WordsInDecadeWritable other = (WordsInDecadeWritable) obj;
		if (decade == null) {
			if (other.decade != null)
				return false;
		} else if (!decade.equals(other.decade))
			return false;
		if (isCouple == null) {
			if (other.isCouple != null)
				return false;
		} else if (!isCouple.equals(other.isCouple))
			return false;
		if (word1 == null) {
			if (other.word1 != null)
				return false;
		} else if (!word1.equals(other.word1))
			return false;
		if (word2 == null) {
			if (other.word2 != null)
				return false;
		} else if (!word2.equals(other.word2))
			return false;
		return true;
	}

	public void write(DataOutput out) throws IOException {
		out.writeBoolean(isCouple);
		out.writeUTF(word1);
		if (isCouple) {
			out.writeUTF(word2);
		}
		out.writeInt(decade);
	}

	public void readFields(DataInput in) throws IOException {
		isCouple = in.readBoolean();
		word1 = in.readUTF();
		if (isCouple) {
			word2 = in.readUTF();
		}
		decade = in.readInt();
	}

	public String toString() {
		String res = "";
		res += word1 + " ";
		if (isCouple) {
			res += word2 + " ";
		}
		res += Integer.toString(decade);
		return res;
	}

	public int compareTo(WordsInDecadeWritable o) {
		int res = word1.compareTo(o.word1);
		if (res == 0) {
			if (word2 != null && o.word2 != null) {
				res = word2.compareTo(o.word2);
			} else if (word2 != null) {
				return 1;
			}
			if (res == 0) {
				return decade.compareTo(o.decade);
			}
			return res;
		}
		return res;
	}

}