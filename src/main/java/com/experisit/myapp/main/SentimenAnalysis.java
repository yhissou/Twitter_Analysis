/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.experisit.myapp.main;

import com.experisit.myapp.functions.NegativeScoreFunction;
import com.experisit.myapp.functions.PositiveScoreFunction;
import com.experisit.myapp.functions.ScoreTweetsFunction;
import com.experisit.myapp.functions.TextFilterFunction;
import com.experisit.myapp.functions.TwitterFilterFunction;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;

/**
 * @author youssef.hissou
 */
public class SentimenAnalysis implements Serializable {

    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws InterruptedException {
        String zk = "sandbox.hortonworks.com:2181";
        String topics = "tweets";

        SparkConf conf = new SparkConf().setAppName("Twitter Spark Streaming").setMaster("local[*]");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10));
        Map<String, Integer> map = new HashMap<>();
        map.put(topics, 1);

        // Create direct kafka stream with brokers and topics
        JavaPairInputDStream<String, String> messages = KafkaUtils.createStream(jssc, zk, "yhissou34", map);
        JavaDStream<String> json = messages.map(
            new Function<Tuple2<String, String>, String>() {
                private static final long serialVersionUID = 42l;
                @Override
                public String call(Tuple2<String, String> message) {
                    return message._2();
                }
            }
        );
//        json.dstream().saveAsTextFiles("hdfs://sandbox.hortonworks.com:8020/user/yhissou/tweets/", "tweets");
//        json.count().print();

        JavaPairDStream<Long, Tuple2<String,String>> tweets = json.mapToPair(
            new TwitterFilterFunction());
        
//        tweets.saveAsHadoopFiles("hdfs://sandbox.hortonworks.com:8020/user/yhissou/tesdsdst/", "tweets", Text.class, Text.class, TextOutputFormat.class); 
        
        JavaPairDStream<Long, Tuple2<String,String>> filtered = tweets.filter(
            new Function<Tuple2<Long, Tuple2<String, String>>, Boolean>() {
            @Override
            public Boolean call(Tuple2<Long, Tuple2<String, String>> tweet) throws Exception {
               return tweet != null;
            }
        });
        
        JavaPairDStream<Long, Tuple2<String,String>> tweetsFiltered = filtered.mapToPair(new TextFilterFunction());
        
        JavaPairDStream<Tuple2<Long, Tuple2<String,String>>, Float> positiveTweets =
            tweetsFiltered.mapToPair(new PositiveScoreFunction());
////        
        JavaPairDStream<Tuple2<Long, Tuple2<String,String>>, Float> negativeTweets =
            tweetsFiltered.mapToPair(new NegativeScoreFunction());      
//        
        JavaPairDStream<Tuple2<Long, Tuple2<String,String>>, Tuple2<Float, Float>> joined =
            positiveTweets.join(negativeTweets);
////
        JavaDStream<Tuple4<Long, Tuple2<String,String>, Float, Float>> scoredTweets =
            joined.map(new Function<Tuple2<Tuple2<Long, Tuple2<String,String>>,
                                           Tuple2<Float, Float>>,
                                    Tuple4<Long, Tuple2<String,String>, Float, Float>>() {
            private static final long serialVersionUID = 42l;
            @Override
            public Tuple4<Long, Tuple2<String,String>, Float, Float> call(
                Tuple2<Tuple2<Long, Tuple2<String,String>>, Tuple2<Float, Float>> tweet)
            {
                return new Tuple4<Long, Tuple2<String,String>, Float, Float>(
                    tweet._1()._1(),
                    tweet._1()._2(),
                    tweet._2()._1(),
                    tweet._2()._2());
            }
        });
////
        
        JavaDStream<Tuple3<String,String, String>> result =
            scoredTweets.map(new ScoreTweetsFunction());
//        
        result.dstream().saveAsTextFiles("hdfs://sandbox.hortonworks.com:8020/user/yhissou/tweets_score/", "tweets");
        result.count().print();

        jssc.start();
        jssc.awaitTermination();
       
    }
}
