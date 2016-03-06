package edu.mum.cs522.logs;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import scala.Tuple2;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class LogAnalyzer {
	private static Function2<Long, Long, Long> SUM_REDUCER = (a, b) -> a + b;

	public static void main(String[] args) {
		// Create a Spark Context.
		SparkConf conf = new SparkConf().setAppName("Log Analyzer");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Load the text file into Spark.
		if (args.length == 0) {
			System.out.println("Must specify an access logs file.");
			System.exit(-1);
		}
		String logFile = args[0];
		JavaRDD<String> logLines = sc.textFile(logFile);

		// Convert the text log lines to ApacheAccessLog objects and cache them
		// since multiple transformations and actions will be called on that
		// data.
		JavaRDD<AccessLog> accessLogs = logLines.map(AccessLog::parseFromLogLine).cache();

		// Compute Response Code to Count.
		List<Tuple2<Integer, Long>> responseCodeToCount = accessLogs
				.mapToPair(log -> new Tuple2<>(log.getResponseCode(), 1L))
				.reduceByKey(SUM_REDUCER).take(100);

		JavaPairRDD<Integer, Long> resCount = sc.parallelizePairs(responseCodeToCount);
		resCount.saveAsTextFile(args[1]);

		// List of 10 most popular URL’s in the Apache log.
		List<Tuple2<String, Long>> urlCount = accessLogs
				.mapToPair(log -> new Tuple2<>(log.getMethod() + " "+ log.getEndpoint() + " " + log.getProtocol(),1L))
				.reduceByKey(SUM_REDUCER)
				.mapToPair(x -> x.swap()).sortByKey(false)
				.mapToPair(x -> x.swap()).take(10);

		JavaPairRDD<String, Long> urlCount1 = sc.parallelizePairs(urlCount);
		urlCount1.saveAsTextFile(args[2]);

		// The content size of responses returned from the server to any host..
		List<Tuple2<String, Long>> sizeSumToIP = accessLogs
				.mapToPair(log -> new Tuple2<>(log.getIpAddress(), log.getContentSize()))
				.reduceByKey(SUM_REDUCER)
				.take(100);

		JavaPairRDD<String, Long> sizeSum = sc.parallelizePairs(sizeSumToIP);
		sizeSum.saveAsTextFile(args[3]);

		// The average, min, and max content size of responses returned from the
		// server.
		JavaRDD<Long> contentSize = accessLogs.map(AccessLog::getContentSize)
				.cache();
		List<String> contentSizes = new ArrayList<String>();
		contentSizes.add("Average of response size = "
				+ contentSize.reduce(SUM_REDUCER) / contentSize.count());
		contentSizes.add("Maximum of response size = "
				+ contentSize.max(Comparator.naturalOrder()));
		contentSizes.add("Minimum of response size = "
				+ contentSize.min(Comparator.naturalOrder()));
		JavaRDD<String> maxminavg = sc.parallelize(contentSizes);
		maxminavg.saveAsTextFile(args[4]);

		// Stop the Spark Context before exiting.
		sc.stop();
	}
}
