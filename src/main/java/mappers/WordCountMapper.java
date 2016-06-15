package mappers;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.google.common.collect.Lists;

import writable.WordsInDecadeWritable;

public class WordCountMapper extends Mapper<LongWritable, Text, WordsInDecadeWritable, LongWritable> {
	// private static int numOfMapping = 0;

	private Set<String> stopWords;
	private LongWritable count = new LongWritable();

	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		// value format: n-gram, year, occurrences, pages, books
		String[] split = value.toString().split("\t");
		if (split.length > 4) {
			int year = Integer.parseInt(split[1]);
			if (year >= 1900) {
				count.set(Long.parseLong(split[2]));
				String[] ngram = split[0].split(" ");
				List<String> validWords = Lists.newArrayList();
				for (int i = 0; i < ngram.length; i++) {
					ngram[i] = ngram[i].toLowerCase().replaceAll("[^\\w\\s]", "");
					if (ngram[i].length() > 1 && isOnlyLetters(ngram[i])) {
						validWords.add(ngram[i]);
					}
				}
				if (validWords.size() > 1) {
					context.write(new WordsInDecadeWritable("TotalWordsInDecade", year),
							new LongWritable(count.get() * Long.valueOf(validWords.size())));
					if (validWords.size() == 2) {
						if (!isStopWord(validWords.get(0))) {
							context.write(new WordsInDecadeWritable(validWords.get(0), year), count);
						}
						if (!isStopWord(validWords.get(1))) {
							context.write(new WordsInDecadeWritable(validWords.get(1), year), count);
						}
						if (!isStopWord(validWords.get(0)) && !isStopWord(validWords.get(1))) {
							context.write(new WordsInDecadeWritable(validWords.get(0), validWords.get(1), year), count);
						}
					} else {
						int middleIdx = (validWords.size() / 2);
						String middleWord = validWords.get(middleIdx);
						if (!isStopWord(middleWord)) {
							context.write(new WordsInDecadeWritable(middleWord, year), count);
							for (String word : validWords) {
								if (!word.equals(middleWord) && !isStopWord(word)) {
									context.write(new WordsInDecadeWritable(word, year), count);
									context.write(new WordsInDecadeWritable(word, middleWord, year), count);
								}
							}
						}
					}
				} else {
					return;
				}
			} else {
				return;
			}
		} else {
			return;
		}
	}

	private boolean isOnlyLetters(String name) {
		char[] chars = name.toCharArray();
		for (char c : chars) {
			if (!Character.isLetter(c)) {
				return false;
			}
		}

		return true;
	}

	@Override
	protected void setup(Mapper<LongWritable, Text, WordsInDecadeWritable, LongWritable>.Context context)
			throws IOException, InterruptedException {
		this.stopWords = new HashSet<String>(Arrays.asList("", "a", "able", "about", "above", "abst", "accordance",
				"according", "accordingly", "across", "act", "actually", "added", "adj", "affected", "affecting",
				"affects", "after", "afterwards", "again", "against", "ah", "all", "almost", "alone", "along",
				"already", "also", "although", "always", "am", "among", "amongst", "an", "and", "announce", "another",
				"any", "anybody", "anyhow", "anymore", "anyone", "anything", "anyway", "anyways", "anywhere",
				"apparently", "approximately", "are", "aren", "arent", "arise", "around", "as", "aside", "ask",
				"asking", "at", "auth", "available", "away", "awfully", "b", "back", "be", "became", "because",
				"become", "becomes", "becoming", "been", "before", "beforehand", "begin", "beginning", "beginnings",
				"begins", "behind", "being", "believe", "below", "beside", "besides", "between", "beyond", "biol",
				"both", "brief", "briefly", "but", "by", "c", "ca", "came", "can", "cannot", "can't", "cause", "causes",
				"certain", "certainly", "co", "com", "come", "comes", "contain", "containing", "contains", "could",
				"couldnt", "d", "date", "did", "didn't", "different", "do", "does", "doesn't", "doing", "done", "don't",
				"down", "downwards", "due", "during", "e", "each", "ed", "edu", "effect", "eg", "eight", "eighty",
				"either", "else", "elsewhere", "end", "ending", "enough", "especially", "et", "et-al", "etc", "even",
				"ever", "every", "everybody", "everyone", "everything", "everywhere", "ex", "except", "f", "far", "few",
				"ff", "fifth", "first", "five", "fix", "followed", "following", "follows", "for", "former", "formerly",
				"forth", "found", "four", "from", "further", "furthermore", "g", "gave", "get", "gets", "getting",
				"give", "given", "gives", "giving", "go", "goes", "gone", "got", "gotten", "h", "had", "happens",
				"hardly", "has", "hasn't", "have", "haven't", "having", "he", "hed", "hence", "her", "here",
				"hereafter", "hereby", "herein", "heres", "hereupon", "hers", "herself", "hes", "hi", "hid", "him",
				"himself", "his", "hither", "home", "how", "howbeit", "however", "hundred", "i", "id", "ie", "if",
				"i'll", "im", "immediate", "immediately", "importance", "important", "in", "inc", "indeed", "index",
				"information", "instead", "into", "invention", "inward", "is", "isn't", "it", "itd", "it'll", "its",
				"itself", "i've", "j", "just", "k", "keep	keeps", "kept", "kg", "km", "know", "known", "knows", "l",
				"largely", "last", "lately", "later", "latter", "latterly", "least", "less", "lest", "let", "lets",
				"like", "liked", "likely", "line", "little", "'ll", "look", "looking", "looks", "ltd", "m", "made",
				"mainly", "make", "makes", "many", "may", "maybe", "me", "mean", "means", "meantime", "meanwhile",
				"merely", "mg", "might", "million", "miss", "ml", "more", "moreover", "most", "mostly", "mr", "mrs",
				"much", "mug", "must", "my", "myself", "n", "na", "name", "namely", "nay", "nd", "near", "nearly",
				"necessarily", "necessary", "need", "needs", "neither", "never", "nevertheless", "new", "next", "nine",
				"ninety", "no", "nobody", "non", "none", "nonetheless", "noone", "nor", "normally", "nos", "not",
				"noted", "nothing", "now", "nowhere", "o", "obtain", "obtained", "obviously", "of", "off", "often",
				"oh", "ok", "okay", "old", "omitted", "on", "once", "one", "ones", "only", "onto", "or", "ord", "other",
				"others", "otherwise", "ought", "our", "ours", "ourselves", "out", "outside", "over", "overall",
				"owing", "own", "p", "page", "pages", "part", "particular", "particularly", "past", "per", "perhaps",
				"placed", "please", "plus", "poorly", "possible", "possibly", "potentially", "pp", "predominantly",
				"present", "previously", "primarily", "probably", "promptly", "proud", "provides", "put", "q", "que",
				"quickly", "quite", "qv", "r", "ran", "rather", "rd", "re", "readily", "really", "recent", "recently",
				"ref", "refs", "regarding", "regardless", "regards", "related", "relatively", "research",
				"respectively", "resulted", "resulting", "results", "right", "run", "s", "said", "same", "saw", "say",
				"saying", "says", "sec", "section", "see", "seeing", "seem", "seemed", "seeming", "seems", "seen",
				"self", "selves", "sent", "seven", "several", "shall", "she", "shed", "she'll", "shes", "should",
				"shouldn't", "show", "showed", "shown", "showns", "shows", "significant", "significantly", "similar",
				"similarly", "since", "six", "slightly", "so", "some", "somebody", "somehow", "someone", "somethan",
				"something", "sometime", "sometimes", "somewhat", "somewhere", "soon", "sorry", "specifically",
				"specified", "specify", "specifying", "still", "stop", "strongly", "sub", "substantially",
				"successfully", "such", "sufficiently", "suggest", "sup", "sure	t", "take", "taken", "taking", "tell",
				"tends", "th", "than", "thank", "thanks", "thanx", "that", "that'll", "thats", "that've", "the",
				"their", "theirs", "them", "themselves", "then", "thence", "there", "thereafter", "thereby", "thered",
				"therefore", "therein", "there'll", "thereof", "therere", "theres", "thereto", "thereupon", "there've",
				"these", "they", "theyd", "they'll", "theyre", "they've", "think", "this", "those", "thou", "though",
				"thoughh", "thousand", "throug", "through", "throughout", "thru", "thus", "til", "tip", "to",
				"together", "too", "took", "toward", "towards", "tried", "tries", "truly", "try", "trying", "ts",
				"twice", "two", "u", "un", "under", "unfortunately", "unless", "unlike", "unlikely", "until", "unto",
				"up", "upon", "ups", "us", "use", "used", "useful", "usefully", "usefulness", "uses", "using",
				"usually", "v", "value", "various", "'ve", "very", "via", "viz", "vol", "vols", "vs", "w", "want",
				"wants", "was", "wasnt", "way", "we", "wed", "welcome", "we'll", "went", "were", "werent", "we've",
				"what", "whatever", "what'll", "whats", "when", "whence", "whenever", "where", "whereafter", "whereas",
				"whereby", "wherein", "wheres", "whereupon", "wherever", "whether", "which", "while", "whim", "whither",
				"who", "whod", "whoever", "whole", "who'll", "whom", "whomever", "whos", "whose", "why", "widely",
				"willing", "wish", "with", "within", "without", "wont", "words", "world", "would", "wouldnt", "www",
				"x", "y", "yes", "yet", "you", "youd", "you'll", "your", "youre", "yours", "yourself", "yourselves",
				"you've", "z", "zero"));

	}

	protected Boolean isStopWord(String word) {
		return stopWords.contains(word);
	}
}
