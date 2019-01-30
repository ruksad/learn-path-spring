package com.scarycoders.learn.springintegrationkafka.model;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@NoArgsConstructor
@ToString
public class Book {
  public enum Genre{
    fantasy,horror,romance,thriller
  }
  private long bookId;
  private String title;
  private Genre genre;
}
