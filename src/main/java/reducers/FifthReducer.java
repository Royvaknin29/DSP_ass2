package reducers;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import writable.FinalKeyByDecade;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class FifthReducer extends Reducer<FinalKeyByDecade, Text, DoubleWritable, DoubleWritable> {

	private Set<String> related = new HashSet<String>(Arrays.asList("tiger,jaguar", "tiger,feline",
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

	private Set<String> unrelated = new HashSet<String>(Arrays.asList("king,cabbage", "professor,cucumber",
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

	public void reduce(DoubleWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		System.out.println("Reducing: " + key.toString());

		long truePositive = 0;
		long falsePositive = 0;
		long falseNegative = 0;

		for (Text value : values) {
			String[] rawPmiWithWords = value.toString().split(" ");
			String words = rawPmiWithWords[1] + "," + rawPmiWithWords[2];
			double wordsPmi = Double.parseDouble(rawPmiWithWords[0]);
			double pmi = key.get();

			if (wordsPmi > pmi) {
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
		context.write(new DoubleWritable(key.get()), new DoubleWritable(getFValue(truePositive, falsePositive, falseNegative)));
	}

	private double getFValue(long truePositive, long falsePositive, long falseNegative) {
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
