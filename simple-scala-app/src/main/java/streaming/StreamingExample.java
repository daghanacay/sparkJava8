package streaming;

import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

import com.google.common.collect.Lists;

/**
 * Counts words in UTF8 encoded, '\n' delimited text received from the network every second.
 *
 * Usage: JavaNetworkWordCount <hostname> <port> <hostname> and <port> describe the TCP server that Spark Streaming
 * would connect to receive data.
 *
 * To run this on your local machine, you need to first run a Netcat server `$ nc -lk 9999` and then run the example `$
 * bin/run-example org.apache.spark.examples.streaming.JavaNetworkWordCount localhost 9999`
 */
public final class StreamingExample {
  private static final Pattern SPACE = Pattern.compile(" ");

  public static void main(final String[] args) {
    if (args.length < 2) {
      System.err.println("Usage: JavaNetworkWordCount <hostname> <port>");
      System.exit(1);
    }

    StreamingExamples.setStreamingLogLevels();

    // Create the context with a 1 second batch size
    final SparkConf sparkConf = new SparkConf().setAppName("JavaNetworkWordCount");
    final JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));

    // Create a JavaReceiverInputDStream on target ip:port and count the
    // words in input stream of \n delimited text (eg. generated by 'nc')
    // Note that no duplication in storage level only for running locally.
    // Replication necessary in distributed scenario for fault tolerance.
    final JavaReceiverInputDStream<String> lines = ssc.socketTextStream(args[0], Integer.parseInt(args[1]),
        StorageLevels.MEMORY_AND_DISK_SER);
    final JavaDStream<String> words = lines.flatMap(x -> Lists.newArrayList(SPACE.split(x)));
    final JavaPairDStream<String, Integer> wordCounts = words.mapToPair(s -> new Tuple2<String, Integer>(s, 1))
        .reduceByKey((i1, i2) -> i1 + i2);

    wordCounts.print();
    ssc.start();
    ssc.awaitTermination();
  }
}