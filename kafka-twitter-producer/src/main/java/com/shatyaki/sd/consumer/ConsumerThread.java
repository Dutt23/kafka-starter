package com.shatyaki.sd.consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.shatyaki.sd.producer.configs.KafkaProperties;

public class ConsumerThread implements Runnable {
    static Logger logger = LoggerFactory.getLogger(ConsumerThread.class);

    public static void main(String... args) {
        String topic = "partition-topic";
        CountDownLatch latch = new CountDownLatch(1);
        Runnable thread = new ConsumerThread(latch, topic);
        Thread consumerThread = new Thread(thread);
        
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.error("Caught shutdown hook");
            ((ConsumerThread) thread).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }));
        consumerThread.start();
        ((ConsumerThread) thread).shutdown();
       
        
        try {
            latch.await();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            logger.info("App interuppted");
            e.printStackTrace();
        }

        finally {
            logger.info("App closing");
        }

    }

    private CountDownLatch latch;
    private KafkaConsumer<String, String> consumer;

    private ConsumerThread(CountDownLatch latch, String... topics) {
        this.latch = latch;
        KafkaProperties kafkaProperties = new KafkaProperties();
        Properties consumerProperties = kafkaProperties.getConsumerProperties("my-tenth-group");
        this.consumer = new KafkaConsumer<String, String>(consumerProperties);

        consumer.subscribe(Arrays.asList(topics));
    }

    @Override
    public void run() {

//          Poll for data
        try {

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    logger.info("======");
                    logger.info("Key " + record.key());
                    logger.info("Value " + record.value());
                    logger.info("Partition" + record.partition());
                    logger.info("Offset" + record.offset());
                }
            }
        } catch (WakeupException e) {
            logger.info("Received shutdown signal");
        } finally {
            logger.info("Received shutdown signal");
            consumer.close();
//                Tell main code that we are done with consumer
            latch.countDown();
        }

    }

    public void shutdown() {
//            Method to stop poll()
//            Throws exception WakeUpException
        logger.info("Received WAKE signal");
        consumer.wakeup();
    }

}