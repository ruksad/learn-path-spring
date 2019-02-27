package com.scarycoders.learn.springcouldkafka.service;

import com.scarycoders.learn.springcouldkafka.model.Greeting;
import com.scarycoders.learn.springcouldkafka.stream.KafkaStream;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class KafkaConsumerSerivce {

  @StreamListener(KafkaStream.INPUT)
  public void handleStream(@Payload Greeting greeting){
    log.info("--------------- message recieved {}",greeting);
  }
}
