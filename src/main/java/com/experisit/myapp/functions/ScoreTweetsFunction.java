package com.experisit.myapp.functions;

import org.apache.spark.api.java.function.*;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import scala.Tuple5;

public class ScoreTweetsFunction
    implements Function<Tuple4<Long, Tuple2<String, String>, Float, Float>, Tuple3<String,String, String>>
{
    private static final long serialVersionUID = 42l;

    @Override
    public Tuple3<String,String, String> call(
        Tuple4<Long, Tuple2<String,String>, Float, Float> tweet)
    {
        String score;
        if (tweet._3() >= tweet._4())
            score = "positive";
        else
            score = "negative";
        return new Tuple3<String,String,String>(
            tweet._2()._1(),
            tweet._2()._2(),
            score);
    }
}
