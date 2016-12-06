package com.zkw.sparkdemo.sparkdemo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;

/**
 * Created by Administrator on 2016/12/6 0006.
 */
public class InputFromXX {

    private void inputJson(){

    }

    /**
     * 创建的方式输入
     */
    private static void input4Create(){
        JavaSparkContext jsc = new JavaSparkContext("local", "createFromParallelize");
        JavaRDD<String> lines = jsc.parallelize(Arrays.asList("hello","kitty"));

        JavaRDD<String> m = lines.map(new Function<String, String>() {
            public String call(String s) throws Exception {
                System.out.println("----------------------------------"+s);
                return null;
            }
        });
        m.count();
    }

    public static void main(String[] args) {
        input4Create();
    }
}
