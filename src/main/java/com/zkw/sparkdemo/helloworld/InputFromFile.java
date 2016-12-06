package com.zkw.sparkdemo.helloworld;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Created by Administrator on 2016/11/27 0027.
 */
public class InputFromFile {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("error number");
        JavaSparkContext sc = new JavaSparkContext(conf);
        //读取数据
        System.setProperty("hadoop.home.dir", "D:\\programFile\\hadoop");
        JavaRDD<String> inputRDD = sc.textFile("D:\\workspacemine\\sparkdemo\\src\\main\\resources\\bytebuffer.txt");
        //切分为单词
       JavaRDD<String> words = inputRDD.flatMap(new FlatMapFunction<String, String>() {
           public Iterator<String> call(String s) throws Exception {
               System.out.println(s);
               return Arrays.asList(SPACE.split(s)).iterator();
           }
       });

        List<String> list = words.collect();
        for (String str: list){
            System.out.println(str);
        }
        System.out.println(words.count());
    }
}


/**
 * new FlatMapFunction<String, String>() {
 public Iterator<String> call(String s) throws Exception {
 return null;
 }
 }
 */
