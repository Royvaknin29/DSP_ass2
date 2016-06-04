package mappers;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Scanner;
import java.util.Set;

import com.google.common.collect.Sets;

public class ScannerTest {
	public static Set<String> STOP_WORDS = initializeStopWords();

	
    private static Set<String> initializeStopWords() {
    	Set<String> stopWords =Sets.newHashSet();
    	try {
			Scanner in = new Scanner(new FileReader("stopWords.txt"));
			while(in.hasNext()){
				System.out.println("new Word!");
				stopWords.add(in.next());
			}
    	} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
    	
		return stopWords;
	}	
    public static void main(String[] args) {
		ScannerTest test = new ScannerTest();
		System.out.println(ScannerTest.STOP_WORDS.size());
	}
}
