package reducers;

import java.util.Map;

import com.google.common.collect.Maps;

public class DollarWordCountInDecade {

	private String wordWithDollar;
	private Map<Integer, Long> countByDecade;

	public DollarWordCountInDecade(String word) {
		this.wordWithDollar = word;
		this.countByDecade = Maps.newHashMap();
	}

	public String getWordWithDollar() {
		return wordWithDollar;
	}

	public void setWordWithDollar(String wordWithDollar) {
		this.wordWithDollar = wordWithDollar;
	}

	public Map<Integer, Long> getCountByDecade() {
		return countByDecade;
	}

	public void setCountByDecade(Map<Integer, Long> countByDecade) {
		this.countByDecade = countByDecade;
	}

}
