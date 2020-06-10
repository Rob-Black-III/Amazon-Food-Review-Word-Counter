/**
 * Filename:        Server.java
 * Date:            9/22/2019
 * Author:          Rob Black
 * CS Account:      rdb5063@rit.edu
 */

package edu.rit.cs.basic_word_count;

import java.io.*;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * TCP Server implementation that can send and receive objects.
 * Fully compartmentalized, has nothing to do with WordCount.
 */
public class Server {
    ServerSocket listenSocket;
    ExecutorService executorService;
    int port;

    /**
     * Server constructor
     * @param port - Make a new server accepting connections on this port.
     */
    public Server(int port) {
        this.port = port;

        //Try to bind to port until successful (only halts on success)
        while (listenSocket == null) {
            try {
                this.listenSocket = new ServerSocket(port);
            } catch (BindException b) {
                //Wait for port to free up. Thus, never quits if existing connection never closes.
                continue;
            } catch (IOException e) {
                e.printStackTrace();
            }
            System.out.println("[SERVER] Created");
        }
    }

    /**
     * The main functionality of the server. Listens for N clients,
     * then returns a list of objects of size N
     *
     * Fully multithreaded using a Handler Callable class.
     * @param numClients - number of clients to expect
     * @return List<Object> received over TCP from Clients
     */
    public List<Object> listen(int numClients) {

        //Create an executor with the previously specified maximum threads to reuse.
        this.executorService = Executors.newFixedThreadPool(numClients);

        //Place to hold all the eventual return Maps
        List<Future> listOfClientData = new ArrayList<>();

        //Handle all clients
        while (numClients != 0) {
            //Accept Client
            Socket clientSocket = null;
            try {
                clientSocket = listenSocket.accept();
                System.out.println("[SERVER] Client Connected " + clientSocket.getInetAddress() + " over port " + clientSocket.getLocalPort());

                //Submit a new thread to the executor with the sublist
                System.out.println("[SERVER] Dispatching client to Handler Callable through Executor");
                Future objectFromClientAsFuture = executorService.submit(new Handler(clientSocket));

                //Add the solution (possibly not completed) to a list to merge later.
                System.out.println("[SERVER] Adding Future to return list");
                listOfClientData.add(objectFromClientAsFuture);
            } catch (IOException e) {
                e.printStackTrace();
                e.printStackTrace();
                System.out.println("[Server] ERROR: Connection Problem");
                System.exit(1);
            }
            numClients = numClients - 1;
        }

        //The Blocking part. Forces the threads to finish to get objects back
        List<Object> returnList = getObjectsFromFutures(listOfClientData);

        //Stop the server
        shutdownServer();

        return returnList;
    }

    /**
     * Converts Futures into their objects
     * "Merges" the threaded computations
     * @param myFutureList - list of futures that may or may not be complete
     * @return List<Object> list of objects of any type
     */
    private static List<Object> getObjectsFromFutures(List<Future> myFutureList) {
        //Create the list of objects to return;
        List<Object> listOfObjects = new ArrayList<>();

        //Loop until all threads finish computation
        while (!myFutureList.isEmpty()) {

            for (int i = 0; i < myFutureList.size(); i++) {

                //Get a thread result (Future)
                Future temp = myFutureList.get(i);

                //Check if it is done (has an answer)
                if (temp.isDone()) {
                    try {
                        listOfObjects.add((Object) temp.get());
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (ExecutionException e) {
                        e.printStackTrace();
                    }
                    myFutureList.remove(i);
                    break;
                }
            }
        }
        return listOfObjects;
    }

    /**
     * Shutdown the server and close the socket.
     * Close the executor service.
     * @return true if shutdown, false if not
     */
    public boolean shutdownServer() {
        Utilities.shutdownAndAwaitTermination(executorService);
        try {
            System.out.println("[SERVER] Killing Server...");
            this.listenSocket.close();
            System.out.println("[SERVER] Killed");
            return true;
        } catch (IOException e) {
            System.out.println("[SERVER] Connection Close Failure:" + e.getMessage());
            return false;
        }
    }
}

/**
 * Threaded Class that implements Callable.
 * Handles all connections to the server. Self-contained
 */
class Handler implements Callable<Object> {
    private Socket clientSocket;
    private ObjectInputStream in;
    private ObjectOutputStream out;

    /**
     * Handler constructor. Takes a socket in as params.
     * @param clientSocket Socket for the client connected to the server
     * @throws IOException
     */
    public Handler(Socket clientSocket) throws IOException {
        System.out.println("[HANDLER] Constructor called.");
        this.clientSocket = clientSocket;

        //Assign Output Stream first - Blocking operations
        //https://stackoverflow.com/questions/8186135/java-sockets-program-stops-at-socket-getinputstream-w-o-error
        System.out.println("[HANDLER] Assigning OUT Stream...");
        this.out = new ObjectOutputStream(clientSocket.getOutputStream());
        System.out.println("[HANDLER] Assigning IN Stream...");
        this.in = new ObjectInputStream(clientSocket.getInputStream());

        System.out.println("[HANDLER] Handler created for " + clientSocket.getInetAddress() + " over port " + clientSocket.getLocalPort());
    }

    /**
     * Override call. Only reads and object.
     * @return Object returned from client
     * @throws Exception
     */
    @Override
    public Object call() throws Exception {
        //Read the Data From the Client
        Object myObject = in.readObject();

        //Close the connection
        kill();

        return myObject;
    }

    /**
     * Closes the server/client connection socket
     * @return true for kill success, false for fail
     */
    public boolean kill() {
        try {
            this.clientSocket.close();
            return true;
        } catch (IOException e) {
            System.out.println("Connection Close Failure:" + e.getMessage());
            return false;
        }
    }
}