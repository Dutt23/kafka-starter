package com.shatyaki.sd.kafka.streams.configs;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;

public class StreamProperties {

    
    public Properties getStreamConfig() {
        Properties properties = new Properties();
        String bootstrapServer = "localhost:9092";
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
//        Same as group id for kafka consumers
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "demo-kafka-streams");
//       Derialiser
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        
        
        return properties;
    }
}
