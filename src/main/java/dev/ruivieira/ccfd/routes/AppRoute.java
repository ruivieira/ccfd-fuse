package dev.ruivieira.ccfd.routes;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import dev.ruivieira.ccfd.routes.messages.PredictionRequest;
import org.apache.camel.AggregationStrategy;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.jackson.JacksonDataFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Component
public class AppRoute extends RouteBuilder {

    private static List<String> parseKafkaPayload(String payload) {
        final ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.readValue(payload, ArrayList.class);
        } catch (IOException e) {
            // TODO: user logger. Handle exception
            e.printStackTrace();
        }
        return new ArrayList<>();
    }

    private static final Logger logger = LoggerFactory.getLogger(AppRoute.class);
    private final ObjectMapper requestMapper = new ObjectMapper();
    private boolean USE_SELDON_TOKEN = false;
    private String SELDON_TOKEN;

    public void configure() {

        requestMapper.enable(SerializationFeature.WRAP_ROOT_VALUE);

        final String BROKER_URL = System.getenv("BROKER_URL");
        final String KAFKA_TOPIC = System.getenv("KAFKA_TOPIC");
        final String KIE_SERVER_URL = System.getenv("KIE_SERVER_URL");
        final String SELDON_URL = System.getenv("SELDON_URL");
        SELDON_TOKEN = System.getenv("SELDON_TOKEN");

        if (BROKER_URL == null) {
            final String message = "No Kafka broker provided";
            logger.error(message);
            throw new IllegalArgumentException(message);
        }

        if (KAFKA_TOPIC == null) {
            final String message = "No Kafka topic provided";
            logger.error(message);
            throw new IllegalArgumentException(message);
        }

        if (KIE_SERVER_URL == null) {
            final String message = "No KIE server provided";
            logger.error(message);
            throw new IllegalArgumentException(message);
        }

        if (SELDON_URL == null) {
            final String message = "No Seldon server provided";
            logger.error(message);
            throw new IllegalArgumentException(message);
        }

        USE_SELDON_TOKEN = SELDON_TOKEN != null;

        final AggregationStrategy seldonStrategy = new SeldonAggregationStrategy();

        from("kafka:" + KAFKA_TOPIC + "?brokers=" + BROKER_URL).routeId("mainRoute")
                .process(exchange -> {
                    // deserialise Kafka message
                    final List<Double> feature = new ArrayList<>();
                    final String payload = exchange.getIn().getBody().toString();
                    final List<String> kafkaFeatures = parseKafkaPayload(payload);
                    // extract the features of interest
                    final int[] indices = {3, 4, 10, 11, 12, 14, 17, 29};
                    for (int index : indices) {
                        feature.add(Double.parseDouble(kafkaFeatures.get(index)));
                    }
                    final PredictionRequest requestObject = new PredictionRequest();
                    requestObject.addFeatures(feature);

                    if (USE_SELDON_TOKEN) {
                        exchange.getOut().setHeader("Authorization", "Bearer " + SELDON_TOKEN);
                    }
                    exchange.getOut().setBody(PredictionRequest.toJSON(requestObject));
                })
                .setHeader(Exchange.HTTP_METHOD, constant("POST"))
                .setHeader(Exchange.CONTENT_TYPE, constant("application/json"))
                .enrich(SELDON_URL + "/predict", seldonStrategy)
                .process(exchange -> {
                    System.out.println(exchange.getIn().getBody(String.class));
                })
                .choice()
                .when(header("fraudulent").isEqualTo(true))

                .marshal(new JacksonDataFormat())
                .to(KIE_SERVER_URL + "/rest/server/containers/ccd-fraud-kjar-1_0-SNAPSHOT/processes/ccd-fraud-kjar.CCDProcess/instances")
                .otherwise()
                .marshal(new JacksonDataFormat())
                .to(KIE_SERVER_URL + "/rest/server/containers/ccd-fraud-kjar-1_0-SNAPSHOT/processes/ccd-fraud-kjar.CCDProcess/instances");
    }
}
