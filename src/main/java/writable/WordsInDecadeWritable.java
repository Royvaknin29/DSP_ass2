package writable;

import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WordsInDecadeWritable implements Writable {
    public String word1;
    public String word2;
    public Integer decade;
    public Boolean isCouple;

    public WordsInDecadeWritable(String word1, String word2, Integer decade) {
        word1 = word1.toLowerCase().replaceAll("[^\\w\\s]","");
        word2 = word2.toLowerCase().replaceAll("[^\\w\\s]","");
        if (word1.compareTo(word2) < 0) {
            this.word1 = word1;
            this.word2 = word2;
        } else {
            this.word1 = word2;
            this.word2 = word1;
        }
        this.decade = (int) Math.floor(decade/10)*10;
        this.isCouple = true;
    }

    public WordsInDecadeWritable(String word1, Integer decade) {
        word1 = word1.toLowerCase().replaceAll("[^\\w\\s]","");
        this.word1 = word1;
        this.word2 = null;
        this.decade = (int) Math.floor(decade/10)*10;
        this.isCouple = false;
    }

    public void write(DataOutput out) throws IOException {
        out.writeBoolean(isCouple);
        out.writeChars(word1);
        if (isCouple) {
            out.writeChars(word2);
        }
        out.writeInt(decade);
    }

    public void readFields(DataInput in) throws IOException {
        isCouple = in.readBoolean();
        word1 = in.readLine();
        if (isCouple) {
            word2 = in.readLine();
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
}
