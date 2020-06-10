/**
 * Filename:        Client.java
 * Date:            9/22/2019
 * Author:          Rob Black
 * CS Account:      rdb5063@rit.edu
 */

package edu.rit.cs.basic_word_count;

import java.net.*;
import java.io.*;

/**
 * TCP Client implementation that can send and receive objects.
 * Fully compartmentalized, has nothing to do with WordCount.
 */
public class Client {
    private String serverAddress;
    private Socket connectionSocket;
    private ObjectInputStream in;
    private ObjectOutputStream out;

    /**
     * Client constructor, takes in a server address and port to connect to
     * @param serverAddress Address of the server
     * @param serverPort Port of the server
     * @throws IOException
     */
    public Client(String serverAddress, int serverPort) throws IOException {
        System.out.println("[CLIENT] Constructor Called");
        this.serverAddress = serverAddress;

        System.out.println("[CLIENT] Connecting to server...");
        while (this.connectionSocket == null) {
            try {
                this.connectionSocket = new Socket(this.serverAddress, serverPort);
            }catch (Exception e){
                continue;
            }
        }
        System.out.println("[CLIENT] Server Connected: " + this.connectionSocket.isConnected());

        //Assign Output Stream first - Blocking operations
        //https://stackoverflow.com/questions/8186135/java-sockets-program-stops-at-socket-getinputstream-w-o-error
        System.out.println("[CLIENT] Assigning OUT Stream...");
        this.out = new ObjectOutputStream(this.connectionSocket.getOutputStream());
        System.out.println("[CLIENT] Assigning IN Stream...");
        this.in = new ObjectInputStream(this.connectionSocket.getInputStream());

        System.out.println("[CLIENT] Created");

    }

    /**
     * Writes and object to the server
     * @param myObject - Object to write
     * @throws IOException
     */
    public void sendObject(Object myObject) throws IOException {
        out.writeObject(myObject);
    }

    /**
     * Reads an object from the server
     * @return Object read from server
     * @throws IOException
     * @throws ClassNotFoundException
     */
    public Object receiveObject() throws IOException, ClassNotFoundException {
        return in.readObject();
    }

    /**
     * Kills the Client (closes socket)
     * @return true on kill success, false on fail
     */
    public boolean kill() {
        try {
            System.out.println("[CLIENT] Killing Client...");
            this.connectionSocket.close();
            System.out.println("[CLIENT] Killed");
            return true;
        } catch (IOException e) {
            System.out.println("Connection Close Failure:" + e.getMessage());
            return false;
        }
    }
}
