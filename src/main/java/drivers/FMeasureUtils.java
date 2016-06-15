package drivers;

import writable.WordsInDecadeWritable;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class FMeasureUtils {

    public static Set<String> related = new HashSet<String>();
    public static Set<String> unrelated = new HashSet<String>();
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

    public static Map getFMeasure(String path) {

        long truePositive = 0;
        long falsePositive = 0;
        long falseNegative = 0;
        Path resultsFile = Paths.get(path);
        Map<WordsInDecadeWritable, Double> resultsMap = new HashMap();
        Map<Double, Double> fMeasureMap = new HashMap();

        try {
            List<String> results = Files.readAllLines(resultsFile, charset);

            for (String result : results) {
                if ( !result.contains("2000")) {
                    continue;
                }
                String[] rawResults = result.split("\t");
                if (rawResults.length < 2) {
                    continue;
                } else {

                }
                String[] rawWordsInDecade = rawResults[0].split(" ");
                resultsMap.put(new WordsInDecadeWritable(
                        rawWordsInDecade[0],
                        rawWordsInDecade[1],
                        Integer.parseInt(rawWordsInDecade[2])),
                        Double.parseDouble(rawResults[1])
                );
            }
        } catch (IOException e) {
            System.out.println(e);
        }

        for (double pmi = 0.1; pmi < 1; pmi += 0.1) {

            for (Map.Entry<WordsInDecadeWritable, Double> result : resultsMap.entrySet()) {
                String words = result.getKey().word1 + "\t" + result.getKey().word2;
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
            fMeasureMap.put(pmi,
                    getFValue(truePositive, falsePositive, falseNegative));
        }

        return fMeasureMap;
    }

    private static double getFValue(long truePositive, long falsePositive, long falseNegative) {

        double precision =  truePositive / (truePositive + falsePositive);
        double recall = truePositive / (truePositive + falseNegative);

        if (truePositive + falsePositive == 0 || truePositive + falseNegative == 0) {
            return 0;
        } else if (precision + recall == 0){
            return 0;
        } else {
            return 2 * ((precision * recall) / (precision + recall));
        }
    }

}
