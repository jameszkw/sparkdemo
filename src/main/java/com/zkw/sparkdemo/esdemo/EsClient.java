package com.zkw.sparkdemo.esdemo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.spark_project.guava.collect.ImmutableList;
import org.elasticsearch.spark.streaming.api.java.JavaEsSparkStreaming;
import java.util.LinkedList;
import java.util.Queue;

/**
 * Created by Administrator on 2016/12/5 0005.
 */
public class EsClient {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        conf.set("es.index.auto.create", "true");
        conf.set("es.nodes", "192.168.31.73");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaStreamingContext jssc = new JavaStreamingContext(jsc, new Duration(2000));
        TripBean upcoming = new TripBean("OTP", "SFO");
        TripBean lastWeek = new TripBean("MUC", "OTP");

        JavaRDD<TripBean> javaRDD = jsc.parallelize(
                ImmutableList.of(upcoming, lastWeek));
        Queue<JavaRDD<TripBean>> microbatches = new LinkedList<JavaRDD<TripBean>>();
        microbatches.add(javaRDD);
        JavaDStream<TripBean> javaDStream = jssc.queueStream(microbatches);

        JavaEsSparkStreaming.saveToEs(javaDStream , "spark/docs");

        jssc.start();
    }
}
