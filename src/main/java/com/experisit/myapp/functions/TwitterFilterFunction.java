package com.experisit.myapp.functions;

import java.io.IOException;

import org.apache.log4j.Logger;

import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import scala.Tuple3;

public class TwitterFilterFunction implements PairFunction<String, Long, Tuple2<String,String>> {
    private static final long serialVersionUID = 42l;
    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public Tuple2<Long, Tuple2<String,String>> call(String tweet)
    {
        try
        {
            JsonNode root = mapper.readValue(tweet, JsonNode.class);
            long id;
            String text;
            String location;
            if (root.get("lang") != null &&
                "en".equals(root.get("lang").textValue()))
            {
                if (root.get("id") != null && root.get("text") != null)
                {
                    id = root.get("id").longValue();
                    text = root.get("text").textValue();
                    if (root.get("location") != null){
                        location = "Paris";}
                    else{
                        location = "New York";}
                    return new Tuple2<Long, Tuple2<String,String>>(id, new Tuple2(text, location));
                }
                return null;
            }
            return null;
        }
        catch (IOException ex)
        {
            Logger LOG = Logger.getLogger(this.getClass());
            LOG.error("IO error while filtering tweets", ex);
            LOG.trace(null, ex);
        }
        return null;
    }
}
