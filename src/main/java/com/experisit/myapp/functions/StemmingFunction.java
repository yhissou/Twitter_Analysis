package com.experisit.myapp.functions;

import org.apache.spark.api.java.function.*;
import scala.Tuple2;
import com.experisit.myapp.words.StopWords;


import java.util.List;

public class StemmingFunction
    implements Function<Tuple2<Long, String>, Tuple2<Long, String>>
{
    private static final long serialVersionUID = 42l;

    @Override
    public Tuple2<Long, String> call(Tuple2<Long, String> tweet)
    {
        String text = tweet._2();
        List<String> stopWords = StopWords.getWords();
        for (String word : stopWords)
        {
            if (text != null){
                text = text.replaceAll("\\b" + word + "\\b", "");
            }
        }
        return new Tuple2<Long, String>(tweet._1(), text);
    }
}
