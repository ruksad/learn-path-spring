package com.scarycoders.learn.springcouldkafka.stream;

import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.SubscribableChannel;

public interface KafkaStream {
  String INPUT="In-stream";
  String OUTPUT="Out-stream";

  @Input(INPUT)
  SubscribableChannel inbound();
  @Output(OUTPUT)
  MessageChannel outBound();
}
