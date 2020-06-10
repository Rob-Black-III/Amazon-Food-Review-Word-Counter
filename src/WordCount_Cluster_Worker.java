/**
 * Filename:        WordCount_Cluster_Worker.java
 * Date:            9/22/2019
 * Author:          Rob Black
 * CS Account:      rdb5063@rit.edu
 */

package edu.rit.cs.basic_word_count;

import java.io.*;
import java.util.List;
import java.util.Map;

/**
 * Worker Node. Reads is a list partition over TCP and makes
 * a sorted Map. Sends the map to a Master Node over TCP.
 */
public class WordCount_Cluster_Worker {

    public static String MASTER_IP;

    /**
     * Main worker. Reads words, sends a map
     * @param args - IP of the MasterNode (1)
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {

        //Parse Arguments
        if(args.length != 1){
            System.out.println("Usage: WordCount_Cluster_Worker <MasterWorker IP>");
            System.exit(1);
        }
        MASTER_IP = args[0];

        //Setup a listen server
        System.out.println("Creating Listen Server for the word list from Master");
        Server wordListServer = new Server(Utilities.SERVER_PORT);

        //Listen for data (word list)
        System.out.println("Listening for Wordlist...");
        List<Object> wordsAsObjectList = wordListServer.listen(1);
        System.out.println("Wordlist Recieved");
        List<String> words = (List<String>)(wordsAsObjectList.get(0));

        //Close the connection
        System.out.println("Closing Listen Server...");
        wordListServer.shutdownServer();

        //Process Data
        System.out.println("Making Maps from Wordlist");
        Map<String, Integer> workerMap = Utilities.makeWordCountMap(words);

        //Meanwhile, the Master created a listen server to listen for incoming maps.

        //Send the map to the master node
        System.out.println("Sending Maps to Listen Server (Master)");
        Client workerClient = new Client(MASTER_IP, Utilities.SERVER_PORT);
        workerClient.sendObject(workerMap);

        System.out.println("Map Sent");
        System.out.println("Worker Done");
        workerClient.kill();
    }
}