package writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class SecondReduceOutput implements WritableComparable<SecondReduceOutput> {

	private String keyWord;
	private Long keyWordCount;
	private boolean hasSecondWord;
	private String secondWord;
	private Long coupleCount;

	public SecondReduceOutput() {

	}

	public SecondReduceOutput(String keyWord, Long keyWordCount, String secondWord, Long coupleCount) {
		this.keyWord = keyWord;
		this.keyWordCount = keyWordCount;
		this.secondWord = secondWord;
		this.coupleCount = coupleCount;
		this.hasSecondWord = true;
	}

	public SecondReduceOutput(Long keyWordCount) {
		this.keyWordCount = keyWordCount;
		this.hasSecondWord = false;
	}

	public String toString() {
		String res = "";
		if (hasSecondWord) {
			res += "(" + keyWord + ", " + keyWordCount.toString() + ") ";
			res += "[(" + keyWord + ", " + secondWord + ")" + ", " + Long.toString(coupleCount) + "]";
		} else {
			res = keyWordCount.toString();
		}
		return res;
	}

	public void write(DataOutput out) throws IOException {
		out.writeBoolean(hasSecondWord);
		out.writeLong(keyWordCount);
		if (hasSecondWord) {
			out.writeUTF(keyWord);
			out.writeUTF(secondWord);
			out.writeLong(coupleCount);
		}
	}

	public void readFields(DataInput in) throws IOException {
		hasSecondWord = in.readBoolean();
		keyWordCount = in.readLong();
		if (hasSecondWord) {
			keyWord = in.readUTF();
			secondWord = in.readUTF();
			coupleCount = in.readLong();
		}
	}

	public int compareTo(SecondReduceOutput o) {
		int res = 0;
		if (this.hasSecondWord && o.hasSecondWord) { // both have 2 words.
			res = this.keyWord.compareTo(o.keyWord);
			if (res == 0) {
				res = this.secondWord.compareTo(o.secondWord);
				if (res == 0) {
					return this.coupleCount.compareTo(o.coupleCount);
				} else {
					return res;
				}
			} else {
				return res;
			}
		} else if (this.hasSecondWord && !o.hasSecondWord) {
			return 1;
		} else if (!this.hasSecondWord && o.hasSecondWord) {
			return -1;
		} else {
			return this.keyWordCount.compareTo(o.keyWordCount);
		}
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((coupleCount == null) ? 0 : coupleCount.hashCode());
		result = prime * result + (hasSecondWord ? 1231 : 1237);
		result = prime * result + ((keyWord == null) ? 0 : keyWord.hashCode());
		result = prime * result + ((keyWordCount == null) ? 0 : keyWordCount.hashCode());
		result = prime * result + ((secondWord == null) ? 0 : secondWord.hashCode());
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
		SecondReduceOutput other = (SecondReduceOutput) obj;
		if (coupleCount == null) {
			if (other.coupleCount != null)
				return false;
		} else if (!coupleCount.equals(other.coupleCount))
			return false;
		if (hasSecondWord != other.hasSecondWord)
			return false;
		if (keyWord == null) {
			if (other.keyWord != null)
				return false;
		} else if (!keyWord.equals(other.keyWord))
			return false;
		if (keyWordCount == null) {
			if (other.keyWordCount != null)
				return false;
		} else if (!keyWordCount.equals(other.keyWordCount))
			return false;
		if (secondWord == null) {
			if (other.secondWord != null)
				return false;
		} else if (!secondWord.equals(other.secondWord))
			return false;
		return true;
	}

	public String getKeyWord() {
		return keyWord;
	}

	public void setKeyWord(String keyWord) {
		this.keyWord = keyWord;
	}

	public Long getKeyWordCount() {
		return keyWordCount;
	}

	public void setKeyWordCount(Long keyWordCount) {
		this.keyWordCount = keyWordCount;
	}

	public boolean isHasSecondWord() {
		return hasSecondWord;
	}

	public void setHasSecondWord(boolean hasSecondWord) {
		this.hasSecondWord = hasSecondWord;
	}

	public String getSecondWord() {
		return secondWord;
	}

	public void setSecondWord(String secondWord) {
		this.secondWord = secondWord;
	}

	public Long getCoupleCount() {
		return coupleCount;
	}

	public void setCoupleCount(Long coupleCount) {
		this.coupleCount = coupleCount;
	}

}
