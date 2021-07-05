package com.shatyaki.sd.kafka.simple.consumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.shatyaki.sd.kafka.simple.configs.KafkaProperties;
import com.shatyaki.sd.kafka.simple.producer.ProducerWithCallBack;

public class Consumer {

    public static void main(String ...args) {
        Logger logger = LoggerFactory.getLogger(ProducerWithCallBack.class);
        KafkaProperties kafkaProperties = new KafkaProperties();
        Properties consumerProperties = kafkaProperties.getConsumerProperties();
        String topic = "second-topic";
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(consumerProperties);
        consumer.subscribe(Collections.singleton(topic));
        
//        Poll for data
        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            
            for(ConsumerRecord<String, String> record : records) {
                logger.info("======");
                logger.info("Key "+ record.key());
                logger.info("Value "+ record.value());
                logger.info("Partition" + record.partition());
                logger.info("Offset" + record.offset());
            }
        }
        
    }
}
