package com.scarycoders.learn.kafka.producer;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class Producer {

  public static void main(String[] args) {
    final String BOOT_STRAP_SERVERS="localhost:9092";
    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,BOOT_STRAP_SERVERS);
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

    KafkaProducer<String, String> stringStringKafkaProducer = new KafkaProducer<String, String>(
        properties);
    ProducerRecord<String, String> hey_i_am_from_program = new ProducerRecord<String, String>(
        "first-topic", "hey i am from program");
    stringStringKafkaProducer.send(hey_i_am_from_program);
    stringStringKafkaProducer.flush();
    stringStringKafkaProducer.close();
  }
}
