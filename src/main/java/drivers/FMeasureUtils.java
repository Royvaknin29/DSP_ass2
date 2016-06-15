package drivers;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Maps;

import writable.WordsInDecadeWritable;

public class FMeasureUtils {

	public static Set<String> related = new HashSet<String>(Arrays.asList("tiger,jaguar", "tiger,feline",
			"closet,clothes", "planet,sun", "hotel,reservation", "planet,constellation", "credit,card", "stock,market",
			"psychology,psychiatry", "planet,moon", "planet,galaxy", "bank,money", "physics,proton", "vodka,brandy",
			"war,troops", "Harvard,Yale", "news,report", "psychology,Freud", "money,wealth", "man,woman",
			"FBI,investigation", "network,hardware", "nature,environment", "seafood,food", "weather,forecast",
			"championship,tournament", "law,lawyer", "money,dollar", "calculation,computation", "planet,star",
			"Jerusalem,Israel", "vodka,gin", "money,bank", "computer,software", "murder,manslaughter", "king,queen",
			"OPEC,oil", "Maradona,football", "mile,kilometer", "seafood,lobster", "furnace,stove",
			"environment,ecology", "boy,lad", "asylum,madhouse", "street,avenue", "car,automobile", "gem,jewel",
			"type,kind", "magician,wizard", "football,soccer", "money,currency", "money,cash", "coast,shore",
			"money,cash", "dollar,buck", "journey,voyage", "midday,noon", "tiger,tiger"));
	public static Set<String> unrelated = new HashSet<String>(Arrays.asList("king,cabbage", "professor,cucumber",
			"chord,smile", "noon,string", "rooster,voyage", "sugar,approach", "stock,jaguar", "stock,life",
			"monk,slave", "lad,wizard", "delay,racism", "stock,CD", "drink,ear", "stock,phone", "holy,sex",
			"production,hike", "precedent,group", "stock,egg", "energy,secretary", "month,hotel", "forest,graveyard",
			"cup,substance", "possibility,girl", "cemetery,woodland", "glass,magician", "cup,entity", "Wednesday,news",
			"direction,combination", "reason,hypertension", "sign,recess", "problem,airport", "cup,article",
			"Arafat,Jackson", "precedent,collection", "volunteer,motto", "listing,proximity", "opera,industry",
			"drink,mother", "crane,implement", "line,insurance", "announcement,effort", "precedent,cognition",
			"media,gain", "cup,artifact", "Mars,water", "peace,insurance", "viewer,serial", "president,medal",
			"prejudice,recognition", "drink,car", "shore,woodland", "coast,forest", "century,nation",
			"practice,institution", "governor,interview", "money,operation", "delay,news", "morality,importance",
			"announcement,production", "five,month", "school,center", "experience,music", "seven,series", "report,gain",
			"music,project", "cup,object", "atmosphere,landscape", "minority,peace", "peace,atmosphere",
			"morality,marriage", "stock,live", "population,development", "architecture,century",
			"precedent,information", "situation,isolation", "media,trading", "profit,warning", "chance,credibility",
			"theater,history", "day,summer", "development,issue"));
	public static Charset charset = Charset.forName("ISO-8859-1");

	public static void init(String relatedPath, String unrelatedPath) {

		Path relatedFile = Paths.get(relatedPath);
		Path unrelatedFile = Paths.get(unrelatedPath);

		try {
			List<String> relatedList = Files.readAllLines(relatedFile, charset);
			List<String> unrelatedList = Files.readAllLines(unrelatedFile, charset);

			for (String relatedWords : relatedList) {
				related.add(relatedWords);
			}

			for (String unrelatedWords : unrelatedList) {
				unrelated.add(unrelatedWords);
			}

		} catch (IOException e) {
			System.out.println(e);
		}
	}

	public static Map<Double, Double> getFMeasure(String path) {

		long truePositive = 0;
		long falsePositive = 0;
		long falseNegative = 0;
		Path resultsFile = Paths.get(path);
		Map<WordsInDecadeWritable, Double> resultsMap = Maps.newHashMap();
		Map<Double, Double> fMeasureMap = Maps.newHashMap();

		try {
			List<String> results = Files.readAllLines(resultsFile, charset);

			for (String result : results) {
				if (!result.contains("2000")) {
					continue;
				}
				String[] rawResults = result.split("\t");
				if (rawResults.length < 2) {
					continue;
				} else {

				}
				String[] rawWordsInDecade = rawResults[0].split(" ");
				resultsMap.put(new WordsInDecadeWritable(rawWordsInDecade[0], rawWordsInDecade[1],
						Integer.parseInt(rawWordsInDecade[2])), Double.parseDouble(rawResults[1]));
			}
		} catch (IOException e) {
			System.out.println(e);
		}

		for (double pmi = 0.1; pmi < 1; pmi += 0.1) {

			for (Map.Entry<WordsInDecadeWritable, Double> result : resultsMap.entrySet()) {
				String words = result.getKey().word1 + "," + result.getKey().word2;
				if (result.getValue() > pmi) {
					if (related.contains(words)) {
						truePositive++;
					} else if (unrelated.contains(words)) {
						falsePositive++;
					}
				} else {
					if (related.contains(words)) {
						falseNegative++;
					}
				}
			}
			fMeasureMap.put(pmi, getFValue(truePositive, falsePositive, falseNegative));
		}

		return fMeasureMap;
	}

	private static double getFValue(long truePositive, long falsePositive, long falseNegative) {
		if (truePositive + falsePositive == 0 || truePositive + falseNegative == 0) {
			return 0;
		}
		double precision = truePositive / (truePositive + falsePositive);
		double recall = truePositive / (truePositive + falseNegative);

		if (precision + recall == 0) {
			return 0;
		} else {
			return 2 * ((precision * recall) / (precision + recall));
		}
	}

}
