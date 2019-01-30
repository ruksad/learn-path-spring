package com.scarycoders.learn.springintegrationkafka.config;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.expression.common.LiteralExpression;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.kafka.outbound.KafkaProducerMessageHandler;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.messaging.MessageHandler;

@Configuration
public class ProducerChannelConfig {

  @Value("${spring.kafka.bootstrap-servers}")
  String bootStrapServer;

  @Bean
  public DirectChannel producerChannel() {
    DirectChannel directChannel = new DirectChannel();
    return directChannel;
  }

  @Bean
  @ServiceActivator(inputChannel = "producerChannel")
  public MessageHandler kafkaMessageHandler() {
    KafkaProducerMessageHandler kafkaProducerMessageHandler = new KafkaProducerMessageHandler(
        kafkaTemplate());
    kafkaProducerMessageHandler.setMessageKeyExpression(new LiteralExpression("Ruksad-01"));
    return kafkaProducerMessageHandler;
  }

  @Bean
  public KafkaTemplate kafkaTemplate() {
    return new KafkaTemplate(producerFactory());
  }

  @Bean
  public ProducerFactory producerFactory() {
    return new DefaultKafkaProducerFactory(producerConfig());
  }

  public Map producerConfig() {
    HashMap<String, Object> properties = new HashMap<>();
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    properties.put(ProducerConfig.LINGER_MS_CONFIG, 5);
    return properties;
  }


}
