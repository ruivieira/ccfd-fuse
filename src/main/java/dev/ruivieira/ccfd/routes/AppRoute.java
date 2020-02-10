package dev.ruivieira.ccfd.routes;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.ruivieira.ccfd.routes.messages.Transaction;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.jackson.JacksonDataFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
public class AppRoute extends RouteBuilder {

    private static final Logger logger = LoggerFactory.getLogger(AppRoute.class);

    public void configure() {

        final String BROKER_URL = System.getenv("BROKER_URL");
        final String KAFKA_TOPIC = System.getenv("KAFKA_TOPIC");
        final String KIE_SERVER_URL = System.getenv("KIE_SERVER_URL");

        if (BROKER_URL==null) {
            final String message = "No Kafka broker provided";
            logger.error(message);
            throw new IllegalArgumentException(message);
        }

        if (KAFKA_TOPIC==null) {
            final String message = "No Kafka topic provided";
            logger.error(message);
            throw new IllegalArgumentException(message);
        }

        if (KIE_SERVER_URL==null) {
            final String message = "No KIE server provided";
            logger.error(message);
            throw new IllegalArgumentException(message);
        }



        from("kafka:" + KAFKA_TOPIC + "?brokers=" + BROKER_URL).routeId("mainRoute")
                .process(exchange -> {
                    // deserialise Kafka message
                    final ObjectMapper mapper = new ObjectMapper();
                    final Object body = exchange.getIn().getBody();
                    final Transaction transaction = mapper.readValue(body.toString(), Transaction.class);
                    final Map<String, Object> map = new HashMap<>();
                    map.put("account_id", transaction.getId());
                    map.put("transaction_amount", transaction.getAmount());
                    exchange.getOut().setBody(map);
                    System.out.println(exchange.getIn().getBody().toString());
                })
                .marshal(new JacksonDataFormat())
                .to("http://" + KIE_SERVER_URL + "/rest/server/containers/ccd-kjar-1_0-SNAPSHOT/processes/ccd-kjar.CCDProcess/instances");
    }
}
