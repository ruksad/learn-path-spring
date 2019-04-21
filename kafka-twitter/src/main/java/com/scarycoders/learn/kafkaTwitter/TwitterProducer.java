package com.scarycoders.learn.kafkaTwitter;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;


public class TwitterProducer {
    public static void main(String[] args) throws InterruptedException {
        new TwitterProducer().run();
    }


    private void run() {
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);
        Client twitterClient = createTwitterClient(msgQueue);
        twitterClient.connect();

        KafkaProducer<String, String> kafkaProducer = createKafkaProducer();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Stopping the twitter kafka consumer");
            twitterClient.stop();
            kafkaProducer.close();
        }));
        while (!twitterClient.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                twitterClient.stop();
            }
            if (msg != null) {
                //log.info("message {}",msg);
                System.out.println("message " + msg);
                kafkaProducer.send(new ProducerRecord<String, String>("twitter_tweets", null, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if (Objects.nonNull(exception)) {
                            System.out.println("Something is wrong");
                        }
                    }
                });
            }
        }
    }

    private KafkaProducer<String, String> createKafkaProducer() {
        final String BOOT_STRAP_SERVERS = "localhost:9092";
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOT_STRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //safe producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        //no need to do following only for education purposes, below is involved in above property
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));

        KafkaProducer<String, String> stringStringKafkaProducer = new KafkaProducer<String, String>(
                properties);
        return stringStringKafkaProducer;
    }

    private Client createTwitterClient(BlockingQueue<String> msgQueue) {
        String consumerKey = "OlgIkqr1MZkCaFIHxXLQXuhiv";
        String consumerSecret = "p0ihVr8OYzWGaEQisyt5TBrXReV2MDxuKtbqCWRV9wfazKD7AR";
        String token = "696985484115910656-9M7bCBRoIQJeO9yBaKUkYmkS39rUlr0";
        String secret = "ywXyeP7ti8t9ex5Q9YNM5X3X1IYP71fuINA1VAWQ771Ve";

        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */


        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        List<String> terms = Lists.newArrayList("kafka");
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);


        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-Ruksad")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();
        // Attempts to establish a connection.
        // hosebirdClient.connect();
        return hosebirdClient;

    }

}
