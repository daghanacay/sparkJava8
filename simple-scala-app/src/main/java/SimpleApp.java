import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SimpleApp {
  public static void main(final String[] args) {
    final String logFile = "/home/daghan/Applications/spark-1.2.1/README.md"; // Should be some file on your system
    final SparkConf conf = new SparkConf().setAppName("Simple Application");
    final JavaSparkContext sc = new JavaSparkContext(conf);
    final JavaRDD<String> logData = sc.textFile(logFile).cache();

    final long numAs = logData.filter(s-> s.contains("a")).count();
    final long numBs = logData.filter(s->s.contains("b")).count();

    System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
  }
}