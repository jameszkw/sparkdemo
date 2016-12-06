package com.zkw.sparkdemo.sparkdemo;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * RDD 转化操作方法demo
 */
public class TranslationMethodRDD {
    private static JavaRDD<String> createData(){
        JavaSparkContext jsc = new JavaSparkContext("local", "createFromParallelize");
        JavaRDD<String> lines = jsc.parallelize(Arrays.asList("hello nina","kitty","jay zhou","james","jay1"));
        return lines;
    }

    /**
     * map()方法的使用
     */
    private static void mapDemo(){
        final JavaRDD<String> lines = createData();
        JavaRDD<List> errorRDD = lines.map(new Function<String, List>() {
            public List call(String s) throws Exception {
                System.out.println("-----------------------"+s);
                List list = new ArrayList();
                list.add(s);
                return list;
            }
        });
        /*for (String str:errorRDD.collect()){
            System.out.println(str);
        }*/

//        System.out.println(StringUtils.join(errorRDD.collect(),"_"));
//        System.out.println(errorRDD.take(3));
        System.out.println(errorRDD.collect());
//        System.out.println(errorRDD.count());

    }

    /**
     * flatMap()方法的使用
     */
    private static void flatMapDemo(){
        JavaRDD<String> lines = createData();
        JavaRDD<String> errorRDD = lines.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String s) throws Exception {
                System.out.println("-----------------------"+s);
                List list = new ArrayList();
                list.add(s);

                return list.iterator();
            }
        });

        System.out.println(errorRDD.collect());

    }

    /**
     * 过滤方法filter()的使用
     */
    private static void filterDemo(){
        JavaRDD<String> lines = createData();
        JavaRDD<String> errorRDD = lines.filter(new Function<String, Boolean>() {
            public Boolean call(String s) throws Exception {
                System.out.println("------------------------"+s);
                return s.contains("l");
            }
        });
        for (String str:errorRDD.collect()){
            System.out.println(str);
        }
    }

    /**
     * 方法union()的使用----组合两个RDD
     * 方法take()
     */
    private static void unionDemo(){
        JavaRDD<String> lines = createData();
        JavaRDD<String> james = lines.filter(new Function<String, Boolean>() {
            public Boolean call(String s) throws Exception {
                return s.contains("james");
            }
        });
        JavaRDD<String> jay = lines.filter(new Function<String, Boolean>() {
            public Boolean call(String s) throws Exception {
                return s.contains("jay");
            }
        });

        JavaRDD<String> unionRDD = james.union(jay);
        for (String str:unionRDD.collect()){
            System.out.println("collect:"+str);
        }

        for (String str:unionRDD.take(2)){
            System.out.println("take:"+str);
        }
    }

    public static void main(String[] args) {
//        unionDemo();
//        mapDemo();
        flatMapDemo();
    }
}
