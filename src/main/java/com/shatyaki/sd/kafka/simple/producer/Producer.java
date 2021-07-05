package com.shatyaki.sd.kafka.simple.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.shatyaki.sd.kafka.simple.configs.KafkaProperties;

public class Producer {

    
    public static void main(String ...agrs) {
        KafkaProperties kafkaProperties = new KafkaProperties();
        
        Properties producerProperties = kafkaProperties.getProducerProperties();
//        String value
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(producerProperties);
        
//        Create a producer record
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("first-topic", "Hello world");
//        Async -, programe exists before sending
        producer.send(record);
//        Flush data
        producer.flush();
//        Flush and close
        producer.close();
    }
}
