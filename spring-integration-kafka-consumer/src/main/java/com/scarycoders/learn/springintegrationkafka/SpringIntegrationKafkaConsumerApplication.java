package com.scarycoders.learn.springintegrationkafka;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.StandardIntegrationFlow;
import org.springframework.integration.dsl.context.IntegrationFlowContext;
import org.springframework.integration.dsl.kafka.Kafka;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.messaging.Message;
import org.springframework.messaging.PollableChannel;

@SpringBootApplication
public class SpringIntegrationKafkaConsumerApplication {

  @Autowired
  private IntegrationFlowContext integrationFlowContext;

  @Autowired
  private KafkaProperties kafkaProperties;

  public static void main(String[] args) {
    //SpringApplication.run(SpringIntegrationKafkaConsumerApplication.class, args);
    ConfigurableApplicationContext configurableApplicationContext = new SpringApplicationBuilder(
        SpringIntegrationKafkaConsumerApplication.class).web(false).run(args);

    List valid_topics = Arrays.asList("fantasy", "horror", "romance", "thriller");

    List<String> validTopics = Arrays.asList(args).stream().filter(valid_topics::contains)
        .collect(Collectors.toList());

    configurableApplicationContext.getBean(SpringIntegrationKafkaConsumerApplication.class)
        .run(configurableApplicationContext, validTopics);

    configurableApplicationContext.close();
  }

  private void run(ConfigurableApplicationContext configurableApplicationContext,
      List<String> validTopics) {

    PollableChannel consumerChannel = configurableApplicationContext
        .getBean("consumerChannel", PollableChannel.class);

    validTopics.forEach(topic -> {
      addAnotherListener(topic);
    });

    Message<?> receive = consumerChannel.receive();
    while (receive != null) {
      consumerChannel.receive();
      System.out.println("Recieved----------------" + receive.getPayload());
    }
  }

  private void addAnotherListener(String... topic) {
    Map consumerProperties = kafkaProperties.buildConsumerProperties();

    StandardIntegrationFlow consumerChannel = IntegrationFlows.from(Kafka
        .messageDrivenChannelAdapter(new DefaultKafkaConsumerFactory(consumerProperties), topic))
        .channel("consumerChannel").get();

    this.integrationFlowContext.registration(consumerChannel).register();
  }
}

