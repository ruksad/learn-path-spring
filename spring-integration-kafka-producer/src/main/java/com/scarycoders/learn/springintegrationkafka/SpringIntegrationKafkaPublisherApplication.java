package com.scarycoders.learn.springintegrationkafka;

import com.scarycoders.learn.springintegrationkafka.model.Book;
import com.scarycoders.learn.springintegrationkafka.publisher.BookPublisher;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.GenericMessage;

@SpringBootApplication
public class SpringIntegrationKafkaPublisherApplication {

  @Autowired
  private BookPublisher bookPublisher;

  public static void main(String[] args) {
    ConfigurableApplicationContext configurableApplicationContext = new SpringApplicationBuilder(
        SpringIntegrationKafkaPublisherApplication.class).web(false).run(args);

    configurableApplicationContext.getBean(SpringIntegrationKafkaPublisherApplication.class)
        .run(configurableApplicationContext);
  }

  private void run(ConfigurableApplicationContext configurableApplicationContext) {
    System.out.println("Inside producer application.................");
    MessageChannel producerChannel = configurableApplicationContext
        .getBean("producerChannel", MessageChannel.class);
    List<Book> books = bookPublisher.getBooks();
    books.forEach(book -> {
      Map<String, String> headers = Collections
          .singletonMap(KafkaHeaders.TOPIC, book.getGenre().toString());
      producerChannel.send(new GenericMessage(book.toString(), headers));
    });
    System.out.println("Finished producer method----------------");
  }

}

