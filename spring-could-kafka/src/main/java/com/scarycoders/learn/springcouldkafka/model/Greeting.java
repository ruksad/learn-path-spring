package com.scarycoders.learn.springcouldkafka.model;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
@Builder
public class Greeting {
  private long timestamp;
  private String message;
}
