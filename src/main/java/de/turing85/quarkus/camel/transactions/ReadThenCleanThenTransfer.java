package de.turing85.quarkus.camel.transactions;

import java.time.Duration;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import io.agroal.api.AgroalDataSource;
import io.quarkus.agroal.DataSource;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.sql.SqlConstants;
import org.apache.camel.component.sql.SqlOutputType;

import static org.apache.camel.builder.endpoint.StaticEndpointBuilders.scheduler;
import static org.apache.camel.builder.endpoint.StaticEndpointBuilders.sql;

@Singleton
public class ReadThenCleanThenTransfer extends RouteBuilder {
  public static final String INSERT_QUERY = "insertQuery";
  public static final String DELETE_QUERY = "deleteQuery";

  private final AgroalDataSource source;
  private final AgroalDataSource target;
  private final ToQueryTransformer toQueryTransformer;

  @Inject
  @SuppressWarnings("unused")
  public ReadThenCleanThenTransfer(
      @SuppressWarnings("CdiInjectionPointsInspection")
      @DataSource("source") AgroalDataSource source,

      @SuppressWarnings("CdiInjectionPointsInspection")
      @DataSource("target") AgroalDataSource target) {
    this(source, target, ToQueryTransformer.instance());
  }

  ReadThenCleanThenTransfer(AgroalDataSource source, AgroalDataSource target,
      ToQueryTransformer toQueryTransformer) {
    this.source = source;
    this.target = target;
    this.toQueryTransformer = toQueryTransformer;
  }

  @Override
  public void configure() {
    // @formatter:off
    from(
        scheduler("read-clean-write")
            .delay(Duration.ofSeconds(2).toMillis()))
        .id("scheduler -> db read -> db clean -> db write")
        .log("reading...")
        .transacted()
        .to(sql("SELECT * FROM data LIMIT 2")
            .dataSource(source)
            .outputType(SqlOutputType.SelectList))
        .log("done")
        .choice()
            .when(header(SqlConstants.SQL_ROW_COUNT).isGreaterThan(0))
                .log("${headers.%s} entries to transfer".formatted(SqlConstants.SQL_ROW_COUNT))
                .process(toQueryTransformer)
                .setProperty(INSERT_QUERY, simple("${body.insert}"))
                .setProperty(DELETE_QUERY, simple("${body.delete}"))
                .log("deleting...")
                    .setBody(exchangeProperty(DELETE_QUERY))
                    .to(sql("query-in-body")
                        .dataSource(source)
                        .useMessageBodyForSql(true))
                .log("done")
                .log("transferring...")
                    .setBody(exchangeProperty(INSERT_QUERY))
                    .removeProperty(INSERT_QUERY)
                    .to(sql("query-in-body")
                        .dataSource(target)
                        .useMessageBodyForSql(true))
                .log("done")
            .otherwise()
                .log("No entries to transfer");
    // @formatter:on
  }
}
