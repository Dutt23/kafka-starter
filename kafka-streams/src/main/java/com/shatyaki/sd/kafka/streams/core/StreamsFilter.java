package com.shatyaki.sd.kafka.streams.core;

import java.util.Properties;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonParser;
import com.shatyaki.sd.kafka.streams.configs.StreamProperties;

public class StreamsFilter {

    private static final Logger logger = LoggerFactory.getLogger(StreamsFilter.class);

    public static void main(String... agrs) {

        StreamProperties streamConfig = new StreamProperties();
        Properties properties = streamConfig.getStreamConfig();
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> inputTopic = builder.stream("twitter-tweets");
        KStream<String, String> filteredStream = inputTopic.filter((k, jsonTweet) -> {
            Long followers = extractFollwersFromTweet(jsonTweet);

            return followers >= 1000L;
        });
        filteredStream.to("important-tweets");
        
        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), properties);
        
        kafkaStreams.start();
    }

    private static Long extractFollwersFromTweet(String tweet) {

        try {
            JsonParser jsonParser = new JsonParser();
            return jsonParser.parse(tweet).getAsJsonObject().get("user").getAsJsonObject().get("followers_count")
                    .getAsLong();
        } catch (Exception e) {
            logger.error("Exception occured : ", e);
            return 0L;
        }

    }
}
