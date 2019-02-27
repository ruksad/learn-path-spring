package com.scarycoders.learn.springcouldkafka;

import com.scarycoders.learn.springcouldkafka.model.Greeting;
import com.scarycoders.learn.springcouldkafka.service.KafkaService;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class MessageController {

  private final KafkaService kafkaService;

  public MessageController(KafkaService kafkaService) {
    this.kafkaService = kafkaService;
  }
  @GetMapping("/message")
  @ResponseStatus(HttpStatus.ACCEPTED)
  public void greetings(@RequestParam("message") String message) {
    Greeting greetings = Greeting.builder()
        .message(message)
        .timestamp(System.currentTimeMillis())
        .build();
    kafkaService.sendGreeting(greetings);
  }
}
