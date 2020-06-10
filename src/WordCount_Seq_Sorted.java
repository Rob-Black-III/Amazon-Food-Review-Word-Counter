/**
 * Filename:        WordCount_Seq_Sorted.java
 * Date:            9/22/2019
 * Author:          Rob Black
 * CS Account:      rdb5063@rit.edu
 */
package edu.rit.cs.basic_word_count;

import java.util.*;

/**
 * Sorted implementation of provided word/frequency code
 * Major changes include hashmap -> treemap and compartmentalizing
 * the code into functions in the Utilities.java class.
 */
public class WordCount_Seq_Sorted {

    /**
     * Sequential implementation
     * Same core logic as unsorted
     * @param args none
     */
    public static void main(String[] args) {
        //Read the Reviews
        List<AmazonFineFoodReview> allReviews = Utilities.read_reviews(Utilities.AMAZON_FINE_FOOD_REVIEWS_file);

        //Tokenize the Reviews into Words
        List<String> words = Utilities.tokenizeAmazonFineFoodReviews(allReviews);

        //Create and start a timer
        MyTimer myTimer = new MyTimer("wordCount");
        myTimer.start_timer();

        //Make the map (sorted)
        Map<String, Integer> finalMap = Utilities.makeWordCountMap(words);

        //Stop the timer
        myTimer.stop_timer();

        //Print the word count
        Utilities.print_word_count(finalMap);

        //Print the elapsed time
        myTimer.print_elapsed_time();
    }

}
