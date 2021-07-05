package com.shatyaki.sd.kafka.simple.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.shatyaki.sd.kafka.simple.configs.KafkaProperties;

public class ProducerWithCallBack {

    
    public static void main(String ...agrs) {
        
        Logger logger = LoggerFactory.getLogger(ProducerWithCallBack.class);
        KafkaProperties kafkaProperties = new KafkaProperties();
        
        Properties producerProperties = kafkaProperties.getProducerProperties();
        Properties consumerProperties = kafkaProperties.getConsumerProperties();
//        String Key
//        String value
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(producerProperties);
        
//        Create a producer record
        
//        Async -, programe exists before sending
        for(int i = 0 ; i <100; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("second-topic", "Hello world" + String.valueOf(i));
            producer.send(record, (recordMeta, e) -> {
//              Executes everytime a record is successfully sent
             if(e == null) {
//                 Record was sent
                 logger.info("received metadata ");
                 logger.info("Topic :" + recordMeta.topic());
                 logger.info("Partition :" + recordMeta.partition());
                 logger.info("Offset :" + recordMeta.offset());
                 logger.info("Timestamp :" + recordMeta.timestamp());
                 
             }
             else {
                 logger.error("Error", e);
             }
          });
//          Flush data
            producer.flush();
        }
        

//        Flush and close
        producer.close();
    }
}
