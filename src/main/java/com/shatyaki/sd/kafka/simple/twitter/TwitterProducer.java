package com.shatyaki.sd.kafka.simple.twitter;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.shatyaki.sd.kafka.simple.configs.KafkaProperties;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class TwitterProducer {
    private static final Logger logger = LoggerFactory.getLogger(TwitterProducer.class);
    public TwitterProducer() {
    }
    
    public void run() {
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);

        Client client = getTwitterClient(msgQueue);
        client.connect();
        KafkaProducer<String, String> producer = getTwitterProducer();
        
        // add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() ->{
            logger.info("Shuttfin down");
            client.stop();
            producer.close();
        }));
        
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            }
            catch(InterruptedException e) {
                e.printStackTrace();
                client.stop();
                
            }
            
            if(msg != null) {
                logger.info(msg);
                producer.send( new ProducerRecord<>("twitter-tweets", null, msg), ( recordMeta, e) ->{
                    if(e != null)
                        logger.error("Wasn not able to produce", e);
                });
            }
            
          }
    }
    
    public KafkaProducer<String, String> getTwitterProducer(){
        KafkaProperties kafkaProperties = new KafkaProperties();
        Properties producerProperties = kafkaProperties.getHighThroughputProperties();
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(producerProperties);
        return producer;
    }

    public static void main(String... args) {
        new TwitterProducer().run();
    }

    private Client getTwitterClient(BlockingQueue<String> msgQueue) {
       
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms
//        List<Long> followings = Arrays.asList(1234L, 566788L);
        List<String> terms = Arrays.asList("kafka", "usa", "politics");
        String consumerKey = "";
        String consumerSecret = "";
        String token = "";
        String secret = "";
//        hosebirdEndpoint.followings(followings);
        hosebirdEndpoint.trackTerms(terms);
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

        ClientBuilder builder = new ClientBuilder().name("Hosebird-Client-01") // optional: mainly for the logs
                .hosts(hosebirdHosts).authentication(hosebirdAuth).endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));
//                .eventMessageQueue(eventQueue);                          // optional: use this if you want to process client events

        Client hosebirdClient = builder.build();
        // Attempts to establish a connection.
        return hosebirdClient;
    }
}
