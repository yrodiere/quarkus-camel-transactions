package de.turing85.quarkus.camel.transactions;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;

class ToQueryTransformer implements Processor {
  private static final ToQueryTransformer INSTANCE = new ToQueryTransformer();

  private ToQueryTransformer() {}

  public static ToQueryTransformer instance() {
    return INSTANCE;
  }

  @Override
  public void process(Exchange exchange) {
    Object body = exchange.getIn().getBody();
    try {
      @SuppressWarnings("unchecked")
      List<Map<String, Object>> results = (List<Map<String, Object>>) body;
      if (results.isEmpty()) {
        exchange.getIn().setBody("");
      }
      // @formatter:off
      String builder = "INSERT INTO DATA(ID, NAME) VALUES " +
          results.stream()
              .map(result -> "(%s, '%s')".formatted(result.get("ID"), result.get("NAME")))
              .collect(Collectors.joining(","));
      // @formatter:on
      exchange.getIn().setBody(builder);
    } catch (ClassCastException e) {
      throw new IllegalArgumentException("exchange body is not of type List<Map<String, Object>>",
          e);
    }
  }
}
