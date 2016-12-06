/**
 * Illustrates a simple map then filter in Java
 */
package com.zkw.sparkdemo.helloworld;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.Duration;

public class StreamingLogInput {
  public static void main(String[] args) throws Exception {
		JavaSparkContext sc = new JavaSparkContext("local", "StreamingLogInput");
    List<String> list = new ArrayList<String>();
    list.add("sdfasdf");
    list.add("1");
    list.add("2");
    list.add("rr");
    sc.parallelize(list);
    // Create a StreamingContext with a 1 second batch size
    JavaStreamingContext jssc = new JavaStreamingContext(sc, new Duration(1000));
    // Create a DStream from all the input on port 7777
//    JavaDStream<String> lines = jssc.socketTextStream("localhost", 7777);
    // Filter our DStream for lines with "error"
    JavaDStream<String> lines = jssc.textFileStream("");
    JavaDStream<String> errorLines = lines.filter(new Function<String, Boolean>() {
        public Boolean call(String line) {
          return line.contains("error");
        }});
    // Print out the lines with errors, which causes this DStream to be evaluated
    errorLines.print();
    // start our streaming context and wait for it to "finish"
    jssc.start();
    // Wait for 10 seconds then exit. To run forever call without a timeout
    jssc.awaitTermination();
    // Stop the streaming context
    jssc.stop();
	}
}
