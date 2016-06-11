package writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class SeconderySortWritable implements WritableComparable<SeconderySortWritable> {

	private String word;
	private Long count;
	private boolean hasWord;

	public SeconderySortWritable() {

	}

	public SeconderySortWritable(String word, Long count) {
		this.count = count;
		this.hasWord = true;
		this.word = word;
	}

	public SeconderySortWritable(Long count) {
		this.count = count;
		this.word = null;
		this.hasWord = false;
	}

	public String getWord() {
		return word;
	}

	public boolean hasWord() {
		return hasWord;
	}

	public void setWord(String word) {
		this.word = word;
	}

	public Long getCount() {
		return count;
	}

	public void setCount(Long count) {
		this.count = count;
	}

	public String toString() {
		String res = "";
		if (hasWord) {
			res += word + " ";
		}
		res += Long.toString(count);
		return res;
	}

	public void write(DataOutput out) throws IOException {
		out.writeBoolean(hasWord);
		if (hasWord) {
			out.writeUTF(word);
		}
		out.writeLong(count);

	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((count == null) ? 0 : count.hashCode());
		result = prime * result + (hasWord ? 1231 : 1237);
		result = prime * result + ((word == null) ? 0 : word.hashCode());
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
		SeconderySortWritable other = (SeconderySortWritable) obj;
		if (count == null) {
			if (other.count != null)
				return false;
		} else if (!count.equals(other.count))
			return false;
		if (hasWord != other.hasWord)
			return false;
		if (word == null) {
			if (other.word != null)
				return false;
		} else if (!word.equals(other.word))
			return false;
		return true;
	}

	public void readFields(DataInput in) throws IOException {
		hasWord = in.readBoolean();
		if (hasWord) {
			word = in.readUTF();
		}
		count = in.readLong();
	}

	public int compareTo(SeconderySortWritable o) {
		int res = 0;
		if (this.hasWord && o.hasWord) { // both have 2 words.
			res = this.word.compareTo(o.word);
			if (res == 0) {
				return this.count.compareTo(o.getCount());
			} else {
				return res;
			}
		} else if (this.hasWord && !o.hasWord) {
			return 1;
		} else if (!this.hasWord && o.hasWord) {
			return -1;
		} else { // both of us dont have word(Shouldn't happen..);
			return this.count.compareTo(o.getCount());
		}
	}

}
