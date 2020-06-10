/**
 * Filename:        WordCount_Cluster_Master.java
 * Date:            9/22/2019
 * Author:          Rob Black
 * CS Account:      rdb5063@rit.edu
 */

package edu.rit.cs.basic_word_count;

import java.io.IOException;
import java.util.List;
import java.util.Map;


/**
 * Master Node. Responsible for reading, tokenizing, and partitioning
 * a list. Sends list partitions over TCP, receives list partitions over TCP.
 */
public class WordCount_Cluster_Master {

    /**
     * Main Master Logic
     * @param args - IP Addresses of Workers (Utilities.NUM_WORKERS)
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {

        //Parse Arguments
        if(args.length != Utilities.NUMBER_OF_WORKERS){
            System.out.println("Usage: WordCount_Cluster_Master <Worker IP> <Worker IP> <Worker IP> ...");
            System.exit(1);
        }

        //Read the review from a file.
        System.out.println("Reading Reviews From File...");
        List<AmazonFineFoodReview> allReviews = Utilities.read_reviews(Utilities.AMAZON_FINE_FOOD_REVIEWS_file);

        //Tokenize the reviews into a List<String> words.
        System.out.println("Tokenizing Reviews into Words List...");
        List<String> words = Utilities.tokenizeAmazonFineFoodReviews(allReviews);

        //Start Timer
        MyTimer myTimer = new MyTimer("wordCount");
        myTimer.start_timer();

        //Partition the words list for all the workers to use
        System.out.println("Partitioning Reviews for (" + Utilities.NUMBER_OF_WORKERS + ") worker(s).");
        List<List<String>> partitionedWords = Utilities.partitionList(words,Utilities.NUMBER_OF_WORKERS);

        //Send a partition to each of the workers
        for(int i=0;i<Utilities.NUMBER_OF_WORKERS;i++){

            //Register Client with the Server
            Client sendListClient = new Client(args[i],Utilities.SERVER_PORT);

            //Send the partition
            System.out.println("Sending Partition to Worker...");
            sendListClient.sendObject(partitionedWords.get(i));

            //Kill the connection
            System.out.println("Closing Connection to Worker...");
            sendListClient.kill();
        }

        //Create a Listen Server to Listen for the Incoming Connections
        System.out.println("Creating Listen Server for the Maps from Workers...");
        Server receiveMapServer = new Server(Utilities.SERVER_PORT);

        // Read the objects
        System.out.println("Listening for Maps...");
        List<Object> myMapsAsObjects = receiveMapServer.listen(Utilities.NUMBER_OF_WORKERS);

        //Wildcard Cast BS
        List<Map<String, Integer>> myMaps = (List<Map<String,Integer>>)(List<?>) myMapsAsObjects;

        //Merge the map
        Map<String,Integer> finalMap = Utilities.mergeManyMaps(myMaps);

        myTimer.stop_timer();

        //Print the map
        Utilities.print_word_count(finalMap);

        myTimer.print_elapsed_time();
    }
}
