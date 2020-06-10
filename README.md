# JAVA-Amazon-Food-Review-Word-Counter
Counts the word in Amazon food reviews and places them into a (word,count) mapping. The map is distributed across LAN stations and threads, then merged at the end. Scalable.

## Usage
Need to build a .jar file. 

### Worker Command
java -cp target/basic_word_count-1.0-SNAPSHOT.jar basic_word_count.WordCount_Cluster_Worker <MASTER_IP>

### Host Command
java -cp target/basic_word_count-1.0-SNAPSHOT.jar basic_word_count.WordCount_Cluster_Master <WORKER_IP_1> <WORKER_IP_2> ...

## Additional Notes
Previously configured as part of edu.rit package, which has been lost in order to condense the project. The code was copied over and I have not refactored to work without the package yet. This is mainly to demonstrate the quality of my code. One would need to configure a LAN or Docker container in order to run it, and I have not provided any documentation to do so. This is here to demonstrate my knowledge with distributed systems and multithreading, and not neccesarily a demonstration of the entire software development pipeline. 
