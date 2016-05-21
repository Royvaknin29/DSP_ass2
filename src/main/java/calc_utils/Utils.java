package calc_utils;

/**
 * Created by aaronv on 21/05/2016.
 */

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Scanner;

public class Utils {

    private final String CORPUS_PATH = "";

    public static Double getPMI(String word1, String word2) {
        HashMap<String, Integer> map = getWordsMap();
        Integer word1Count = map.get(word1);
        Integer word2Count = map.get(word2);
        Integer totalWordsCount = map.get("totalWordsCount");

        Double pmi = Math.log(word1Count + word2Count) + Math.log(totalWordsCount) - Math.log(word1Count) - Math.log(word2Count);
        return pmi;
    }

    private static HashMap<String, Integer> getWordsMap() {
        Scanner file = null;
        try {
            file = new Scanner(new File(CORPUS_PATH)).useDelimiter("[^a-zA-Z]+");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        HashMap<String, Integer> map = new HashMap<String, Integer>();
        Integer totalWordsCount = 0;

        while (file.hasNext()){
            String word = file.next().toLowerCase();
            totalWordsCount++;
            if (map.containsKey(word)) {
                map.put(word, map.get(word) + 1);
            } else {
                map.put(word, 0);
            }
        }

        map.put("totalWordsCount", totalWordsCount);
        return map;
    }
}
