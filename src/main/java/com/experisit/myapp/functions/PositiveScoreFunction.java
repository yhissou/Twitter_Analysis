package com.experisit.myapp.functions;

import org.apache.spark.api.java.function.*;
import scala.Tuple2;
import com.experisit.myapp.words.PositiveWords;

import java.util.Set;

public class PositiveScoreFunction
    implements PairFunction<Tuple2<Long, Tuple2<String,String>>,
                            Tuple2<Long, Tuple2<String,String>>, Float>
    {
    private static final long serialVersionUID = 42l;

    @Override
    public Tuple2<Tuple2<Long, Tuple2<String,String>>, Float> call(Tuple2<Long, Tuple2<String,String>> tweet)
    {
        String text = tweet._2()._1();
        Set<String> posWords = PositiveWords.getWords();
        String[] words = text.split(" ");
        int numWords = words.length;
        int numPosWords = 0;
        for (String word : words)
        {
            if (posWords.contains(word))
                numPosWords++;
        }
        return new Tuple2<Tuple2<Long, Tuple2<String,String>>, Float>(
            new Tuple2<Long, Tuple2<String,String>>(tweet._1(), 
                    new Tuple2(tweet._2()._1(),tweet._2()._2())),
                        (float) numPosWords / numWords
        );
    }
}
