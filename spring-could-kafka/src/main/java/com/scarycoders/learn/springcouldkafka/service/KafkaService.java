package com.scarycoders.learn.springcouldkafka.service;

import com.scarycoders.learn.springcouldkafka.model.Greeting;
import com.scarycoders.learn.springcouldkafka.stream.KafkaStream;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;
import org.springframework.util.MimeTypeUtils;

@Service
@Slf4j
public class KafkaService {

  private final KafkaStream kafkaStream;

  public KafkaService(KafkaStream kafkaStream) {
    this.kafkaStream = kafkaStream;
  }

  public void sendGreeting(final Greeting greeting) {
    log.info("Sending greeting {}", greeting);
    MessageChannel messageChannel = kafkaStream.outBound();
    messageChannel.send(MessageBuilder.withPayload(greeting)
        .setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.APPLICATION_JSON).build());
  }
}
