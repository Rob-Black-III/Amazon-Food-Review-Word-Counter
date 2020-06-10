/**
 * Filename:        Utilities.java
 * Date:            9/22/2019
 * Author:          Rob Black
 * CS Account:      rdb5063@rit.edu
 */

package edu.rit.cs.basic_word_count;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Common functions used between multiple parts of the problem.
 * Rather than copy and paste, I placed their shared implementation here.
 * Also acts as a configuration file.
 */
public class Utilities {

    /*
    Config Information
    -----------------------------------------
    */

    //Review file location [ALL TASKS]
    public static final String AMAZON_FINE_FOOD_REVIEWS_file="amazon-fine-food-reviews/Reviews.csv";

    //Number of Threads on the System [SUBTASK 2]
    public static final int MAX_THREADS = Runtime.getRuntime().availableProcessors();

    //Default Configuration [SUBTASK 3]
    public static int NUMBER_OF_WORKERS = 5;
    public static int SERVER_PORT = 5678;

    /*
    Shared Functions
    -----------------------------------------
    */

    /**
     * Prints the word count of the index map. Implementation Provided
     * @param wordcount
     */
    public static void print_word_count( Map<String, Integer> wordcount){
        for(String word : wordcount.keySet()){
            System.out.println(word + " : " + wordcount.get(word));
        }
    }

    /**
     * Reads the reviews into a List<AmazonFineFoodReviews>.
     * @param dataset_file - file to read reviews from
     * @return List of Amazon Food reviews
     */
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

    /**
     * Tokenizes Amazon Reviews into words for later partitioning
     * @param allReviews
     * @return
     */
    public static List<String> tokenizeAmazonFineFoodReviews(List<AmazonFineFoodReview> allReviews){
        ArrayList<String> words = new ArrayList<String>();
        for(AmazonFineFoodReview review : allReviews) {
            Pattern pattern = Pattern.compile("([a-zA-Z]+)");
            Matcher matcher = pattern.matcher(review.get_Summary());

            while(matcher.find())
                words.add(matcher.group().toLowerCase());
        }
        return words;
    }

    /**
     * Partitions a Generic List into a List of those Lists with relatively equal parts
     * Uses integer divisions so parts are not all equal but are close.
     * Uses an Internal ArrayList
     * @param sourceList list to partition
     * @param numPartitions list of new partitions
     * @param <T> Generic List Type
     * @return list of lists that contain all of the sourceList
     */
    public static <T> List<List<T>> partitionList(List<T> sourceList, int numPartitions) {
        // Variables for partitioning
        int remainingWords = sourceList.size();
        int remainingPartitions = numPartitions;
        int partitionIndex = 0;

        //Create the Partitions
        List<List<T>> listOfPartitions = new ArrayList<>();

        for(int i=0;i<numPartitions;i++){

            //Calculate PartitionSize
            int partitionSize = remainingWords / remainingPartitions;

            //DEBUG
            //int upperBound = partitionIndex + partitionSize;
            //System.out.println("Sublist of Words[" + partitionIndex + "," + upperBound + "]");

            //Partition the words ArrayList into a sublist.
            List<T> temp = sourceList.subList(partitionIndex, partitionIndex + partitionSize);

            //Make a new list for the partition with a Copy Constructor
            List<T> partition = new ArrayList<>(temp);

            //Add the partition to the main list to return
            listOfPartitions.add(partition);

            //Increment the Partition
            partitionIndex = partitionIndex + partitionSize;

            //Reduce the Remaining Words by the Partition Size
            remainingWords = remainingWords - partitionSize;

            //Reduce Number of Remaining Partitions
            remainingPartitions = remainingPartitions - 1;
        }
        return listOfPartitions;
    }

    /**
     * Creates a new map given a list of words. Implementation Provided. Wrapper function created.
     * @param words list of Strings
     * @return new Sorted Map of words and their frequencies.
     */
    public static Map<String, Integer> makeWordCountMap(List<String> words){
        Map<String, Integer> wordcount = new TreeMap<>();
        for(String word : words) {
            if(!wordcount.containsKey(word)) {
                wordcount.put(word, 1);
            } else{
                int init_value = wordcount.get(word);
                wordcount.replace(word, init_value, init_value+1);
            }
        }
        return wordcount;
    }

    /**
     * Recursively Merges any number of word/frequency maps using the mergeTwoMaps helper function
     * Produces a single map with the contents of all the other maps
     * @param listOfMaps all the maps to merge
     * @return singular merged map
     */
    public static Map<String, Integer> mergeManyMaps(List<Map<String, Integer>> listOfMaps){
        if(listOfMaps.size() == 1){
            return listOfMaps.get(0);
        }
        else{
            //Merge the first two maps
            Map<String, Integer> temp = mergeTwoMaps(listOfMaps.get(0),listOfMaps.get(1));

            //Remove both maps
            listOfMaps.remove(0);
            listOfMaps.remove(0);

            //Add merged map to front
            listOfMaps.add(0,temp);

            return mergeManyMaps(listOfMaps);
        }
    }

    /**
     * Merges two maps together by reading the smaller one and adding it to the larger one.
     * @param map1 A map of words/frequencies
     * @param map2 A map of words/frequencies
     * @return A merged map
     */
    private static Map<String, Integer> mergeTwoMaps(Map<String, Integer> map1, Map<String, Integer> map2){
        if(map1.size() >= map2.size()){
            //Merge Smaller Map with Bigger Map
            for(String word : map2.keySet()) {
                if(!map1.containsKey(word)) {
                    map1.put(word, map2.get(word));
                } else{
                    int init_value = map1.get(word);
                    map1.replace(word, init_value, init_value + map2.get(word));
                }
            }
            return map1;
        }
        else{
            //Merge Smaller Map with Bigger Map
            for(String word : map1.keySet()) {
                if(!map2.containsKey(word)) {
                    map2.put(word, map1.get(word));
                } else{
                    int init_value = map2.get(word);
                    map2.replace(word, init_value, init_value + map1.get(word));
                }
            }
            return map2;
        }

    }

    /**
     * Merges threaded Futures of type Map<String, Integer> into a single Map<String, Integer> as they complete.
     * @param listOfThreadComputations - list of threads that return Futures of Map<String,Integer>
     * @return An entire map of a words and occurrences in ascending order
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public static Map<String, Integer> mergeThreadedMapComputations(List<Future> listOfThreadComputations) throws ExecutionException, InterruptedException {

        //Create the total map containing all results.
        Map<String,Integer> mergedResult = new TreeMap<>();

        //Loop until all threads finish computation
        while(!listOfThreadComputations.isEmpty()){

            //Create a new map to store the returned Future Map.
            //Map<String,Integer> completedMap = new TreeMap<>();
            //int removalIndex = 0;

            // Loop through all the futures until we get one that isDone()
            // If just called temp.get(), would wait for completion.
            // This way preserves parallel behavior.
            for(int i=0;i<listOfThreadComputations.size();i++){

                //Get a thread result (Future)
                Future temp = listOfThreadComputations.get(i);

                //Check if it is done (has an answer)
                if(temp.isDone()){
                    Map<String, Integer> completedMap = new TreeMap<>((Map<String, Integer>)temp.get());
                    mergedResult = mergeTwoMaps(mergedResult, completedMap);
                    listOfThreadComputations.remove(i);
                    break;
                }
            }
        }
        return mergedResult;
    }

    /**
     * Code ripped right from the java doc
     * on how to stop the executor service thread pool properly.
     * https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/ExecutorService.html
     *
     * Could just use pool.shutdownNow() because I verify completion
     * of the threads in implementation, but doesn't matter.
     * @param pool - Thread pool
     */
    public static void shutdownAndAwaitTermination(ExecutorService pool) {
        pool.shutdown(); // Disable new tasks from being submitted
        try {
            // Wait a while for existing tasks to terminate
            if (!pool.awaitTermination(60, TimeUnit.SECONDS)) {
                pool.shutdownNow(); // Cancel currently executing tasks
                // Wait a while for tasks to respond to being cancelled
                if (!pool.awaitTermination(60, TimeUnit.SECONDS))
                    System.err.println("Pool did not terminate");
            }
        } catch (InterruptedException ie) {
            // (Re-)Cancel if current thread also interrupted
            pool.shutdownNow();
            // Preserve interrupt status
            Thread.currentThread().interrupt();
        }
    }
}