package com.scarycoders.learn.springcouldkafka.config;

import com.scarycoders.learn.springcouldkafka.stream.KafkaStream;
import org.springframework.cloud.stream.annotation.EnableBinding;

@EnableBinding(KafkaStream.class)
public class StreamConfig {

}
