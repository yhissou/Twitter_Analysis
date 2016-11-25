package com.experisit.myapp.functions;

import org.apache.spark.api.java.function.*;
import scala.Tuple2;

public class TextFilterFunction
    implements PairFunction<Tuple2<Long, Tuple2<String, String>>, Long, Tuple2<String,String>>
{
    private static final long serialVersionUID = 42l;

    @Override
    public Tuple2<Long, Tuple2<String,String>> call(Tuple2<Long, Tuple2<String, String>> tweet) throws Exception {
        String text = tweet._2()._1;
        if (text != null){
            text = text.replaceAll("[^a-zA-Z\\s]", "").trim().toLowerCase();
        }
        return new Tuple2<Long, Tuple2<String,String>>(tweet._1(), new Tuple2(text,tweet._2()._2));
    }
}
