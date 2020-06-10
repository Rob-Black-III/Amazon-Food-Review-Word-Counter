package edu.rit.cs.basic_word_count;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class WordCount_Seq {
    public static final String AMAZON_FINE_FOOD_REVIEWS_file="amazon-fine-food-reviews/Reviews.csv";
    //public static final String AMAZON_FINE_FOOD_REVIEWS_file="~/IdeaProjects/CoPaDS/basic_word_count/amazon-fine-food-reviews/Reviews.csv";

    public static List<AmazonFineFoodReview> read_reviews(String dataset_file) {
        List<AmazonFineFoodReview> allReviews = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(dataset_file))){
            String reviewLine = null;
            // read the header line
            reviewLine = br.readLine();

            //read the subsequent lines
            while ((reviewLine = br.readLine()) != null) {
                allReviews.add(new AmazonFineFoodReview(reviewLine));
            }
        } catch(Exception e) {
            e.printStackTrace();
        }
        return allReviews;
    }


    public static void print_word_count( Map<String, Integer> wordcount){
        for(String word : wordcount.keySet()){
            System.out.println(word + " : " + wordcount.get(word));
        }
    }

    public static void main(String[] args) {
        List<AmazonFineFoodReview> allReviews = read_reviews(AMAZON_FINE_FOOD_REVIEWS_file);

        /* For debug purpose */
//        for(AmazonFineFoodReview review : allReviews){
//            System.out.println(review.get_Text());
//        }

        /* Tokenize words */
        List<String> words = new ArrayList<String>();
        for(AmazonFineFoodReview review : allReviews) {
            Pattern pattern = Pattern.compile("([a-zA-Z]+)");
            Matcher matcher = pattern.matcher(review.get_Summary());

            while(matcher.find())
                words.add(matcher.group().toLowerCase());
        }

        MyTimer myTimer = new MyTimer("wordCount");
        myTimer.start_timer();

//        /* Count words */
        Map<String, Integer> wordcount = new HashMap<>();
        for(String word : words) {
            if(!wordcount.containsKey(word)) {
                wordcount.put(word, 1);
            } else{
                int init_value = wordcount.get(word);
                wordcount.replace(word, init_value, init_value+1);
            }
        }
        myTimer.stop_timer();

        print_word_count(wordcount);

        myTimer.print_elapsed_time();
    }

}
