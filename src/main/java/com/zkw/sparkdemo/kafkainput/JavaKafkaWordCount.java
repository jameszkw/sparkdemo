package com.zkw.sparkdemo.kafkainput;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;
import java.util.regex.Pattern;

import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

/**
 * Created by Administrator on 2016/12/1 0001.
 */
public class JavaKafkaWordCount {
    private static final Pattern SPACE = Pattern.compile(" ");
    private static final  Logger logger = LoggerFactory.getLogger(JavaKafkaWordCount.class);

    private JavaKafkaWordCount() {
    }

    public static void main(String[] args) {
        if (args.length < 4) {
            System.err.println("Usage: JavaKafkaWordCount <zkQuorum> <group> <topics> <numThreads>");
        }
        String zkQuorum = "192.168.31.73:2181";
        String group = "test";
        String topicStr = "test";
        int args3 = 1;

//        StreamingExamples.setStreamingLogLevels();
        JavaSparkContext jsc = new JavaSparkContext("local", "StreamingLogInput");
//        LogManager.getRootLogger().setLevel(Level.ERROR);
        // Create the context with 2 seconds batch size
        JavaStreamingContext jssc = new JavaStreamingContext(jsc, new Duration(2000));
        int numThreads = args3;
        Map<String, Integer> topicMap = new HashMap<String, Integer>();
        String[] topics = topicStr.split(",");
        for (String topic: topics) {
            topicMap.put(topic, numThreads);
        }
        logger.warn("dddddddddddddddddddddddddddd");
        logger.warn("----------------------->:"+topicMap.toString());
        JavaPairReceiverInputDStream<String, String> messages =
                KafkaUtils.createStream(jssc, zkQuorum, group, topicMap);


        JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
            public String call(Tuple2<String, String> tuple2) {
                System.out.println(tuple2);
                return tuple2._2();
            }
        });


        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String x) {
                return Arrays.asList(SPACE.split(x)).iterator();
            }
        });

        JavaPairDStream<String, Integer> wordCounts = words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    public Tuple2<String, Integer> call(String s) {
                        return new Tuple2<String, Integer>(s, 1);
                    }
                }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer i1, Integer i2) {
                return i1 + i2;
            }
        });

        wordCounts.print();
        logger.info("map result... ...");
        lines.print();
        jssc.start();//开始接受数据

        try {
            jssc.awaitTermination();//等待流计算完成，防止应用退出
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


    }
}
