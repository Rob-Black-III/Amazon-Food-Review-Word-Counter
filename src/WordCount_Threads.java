/**
 * Filename:        WordCount_Threads.java
 * Date:            9/22/2019
 * Author:          Rob Black
 * CS Account:      rdb5063@rit.edu
 */
package edu.rit.cs.basic_word_count;

import java.util.*;
import java.util.concurrent.*;

/**
 * Threaded implementation of sorting words and frequencies.
 * Takes a list of words, partitions, and distributes it
 * across multiple threads.
 */
public class WordCount_Threads{

    /**
     * Main.
     * @param args - none
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        //Read the Reviews
        List<AmazonFineFoodReview> allReviews = Utilities.read_reviews(Utilities.AMAZON_FINE_FOOD_REVIEWS_file);

        //Tokenize the Reviews into Words
        List<String> words = Utilities.tokenizeAmazonFineFoodReviews(allReviews);

        /*
        Multithreaded Part
        */

        MyTimer myTimer = new MyTimer("making and merging sorted maps");
        myTimer.start_timer();

        //Create an executor with the previously specified maximum threads to reuse.
        ExecutorService executor = Executors.newFixedThreadPool(Utilities.MAX_THREADS);

        //Place to hold all the eventual return Maps
        List<Future> listOfThreadComputations = new ArrayList<>();

        //Partition the list into maxThreads pieces.
        List<List<String>> partitions = Utilities.partitionList(words, Utilities.MAX_THREADS);

        //Dispatch the partitions to the threads for processing
        for(int i = 0; i<Utilities.MAX_THREADS; i++){
            //Submit a new thread to the executor with the sublist
            Future unmergedMap = executor.submit(new SortMapCallable(partitions.get(i)));

            //Add the solution (possibly not completed) to a list to merge later.
            listOfThreadComputations.add(unmergedMap);
        }

        // Merge The Thread Results
        Map<String, Integer> wordCount = Utilities.mergeThreadedMapComputations(listOfThreadComputations);

        //Shutdown the thread pool properly
        Utilities.shutdownAndAwaitTermination(executor);

        myTimer.stop_timer();

        Utilities.print_word_count(wordCount);

        myTimer.print_elapsed_time();
    }
}

/**
 * Threaded Class that implements Callable.
 */
class SortMapCallable implements Callable<Map<String,Integer>> {
    private List<String> wordsToSort;
    private Map<String, Integer> sortedMap;

    /**
     * Constructor
     * @param wordsToSort words to sort into a map
     */
    public SortMapCallable(List<String> wordsToSort) {
        this.wordsToSort = wordsToSort;
        this.sortedMap = new TreeMap<>();
    }

    /**
     * Where the magic happens
     * @return a sorted map containing the words and their frequencies.
     * @throws Exception
     */
    @Override
    public Map<String, Integer> call() throws Exception {
        return sortMap(this.wordsToSort);
    }

    /**
     * Logic to make the map and frequencies. Stolen from sequential code.
     * @param wordsToSort words to sort into a map
     * @return a sorted map containing the words and their frequencies.
     * @throws InterruptedException
     */
    private Map<String, Integer> sortMap(List<String> wordsToSort) throws InterruptedException {

        //DEBUG
        //System.out.println(Thread.currentThread() + " has " + wordsToSort.size() + " words to sort. ");

        for (String word : wordsToSort) {
            if (!sortedMap.containsKey(word)) {
                sortedMap.put(word, 1);
            } else {
                int init_value = sortedMap.get(word);
                sortedMap.replace(word, init_value, init_value + 1);
            }
        }

        return sortedMap;
    }
}