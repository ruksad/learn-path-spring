package com.scarycoders.learn.springintegrationkafka.config;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.kafka.inbound.KafkaMessageDrivenChannelAdapter;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.messaging.PollableChannel;
import org.springframework.kafka.listener.config.ContainerProperties;

@Configuration
public class ConsumerChannelConfig {

  @Value("${spring.kafka.bootstrap-servers}")
  private String bootStrapServers;

  @Value("${spring.kafka.topic}")
  private String springIntegrationKafkaTopic;

  @Bean
  public PollableChannel consumerChannel(){
    return new QueueChannel();
  }

  @Bean
  public KafkaMessageDrivenChannelAdapter kafkaMessageDrivenChannelAdapter(){
    KafkaMessageDrivenChannelAdapter kafkaMessageDrivenChannelAdapter = new KafkaMessageDrivenChannelAdapter(
        concurrentMessageListenerContainer());
    kafkaMessageDrivenChannelAdapter.setOutputChannel(consumerChannel());
    return kafkaMessageDrivenChannelAdapter;
  }

  @Bean
  public ConcurrentMessageListenerContainer concurrentMessageListenerContainer(){
    ContainerProperties containerProperties = new ContainerProperties(springIntegrationKafkaTopic);
    return new ConcurrentMessageListenerContainer(consumerFactory(),containerProperties);
  }

  @Bean
  public ConsumerFactory consumerFactory(){
    return new DefaultKafkaConsumerFactory(consumerConfig());
  }

  public  Map consumerConfig() {
    HashMap hashMap = new HashMap();
    hashMap.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootStrapServers);
    hashMap.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    hashMap.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class);
    hashMap.put(ConsumerConfig.GROUP_ID_CONFIG,"testingRuksad");
    return hashMap;
  }
}
