package com.scarycoders.learn.kafka.producer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerWithkey {

  public static void main(String[] args) throws ExecutionException, InterruptedException {
    final Logger logger = LoggerFactory.getLogger(ProducerWithkey.class);

    final String BOOT_STRAP_SERVERS = "localhost:9092";
    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOT_STRAP_SERVERS);
    properties
        .setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        StringSerializer.class.getName());

    KafkaProducer<String, String> stringStringKafkaProducer = new KafkaProducer<String, String>(
        properties);
    String topic = "first-topic";
    String value = "hey i am from program";
    for (int i = 0; i < 10; i++) {
      ProducerRecord<String, String> hey_i_am_from_program = new ProducerRecord<String, String>(
          topic, "key_"+i ,value + "_" + i);
      stringStringKafkaProducer.send(hey_i_am_from_program, new Callback() {
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {

          if (e == null) {
            logger.info(
                "Topic: " + recordMetadata.topic() + " partition: " + recordMetadata.partition()
                    + " offset: " + recordMetadata.offset()+" timestamp: "+
                recordMetadata.timestamp());
          } else {
            logger.info("Exception is thrown");
          }

        }
      }).get();
    }

    stringStringKafkaProducer.flush();
    stringStringKafkaProducer.close();
  }
}
