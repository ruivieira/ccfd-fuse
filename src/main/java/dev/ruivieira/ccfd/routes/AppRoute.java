package dev.ruivieira.ccfd.routes;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import dev.ruivieira.ccfd.routes.messages.NotificationResponse;
import dev.ruivieira.ccfd.routes.processors.PredictionProcessor;
import org.apache.camel.AggregationStrategy;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.jackson.JacksonDataFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class AppRoute extends RouteBuilder {


    private static final Logger logger = LoggerFactory.getLogger(AppRoute.class);
    private final ObjectMapper requestMapper = new ObjectMapper();
    private final AggregationStrategy seldonStrategy;

    private final Boolean USE_SELDON_STANDARD;

    private static final String SELDON_ENDPOINT_KEY = "SELDON_ENDPOINT";
    private static final String SELDON_ENDPOINT_DEFAULT = "predict";
    private final String KAFKA_TOPIC;
    private String SELDON_ENDPOINT;
    private final String SELDON_URL;
    private final String KIE_SERVER_URL;
    private final String SELDON_TOKEN;
    private final String BROKER_URL;
    private final String CUSTOMER_NOTIFICATION_TOPIC;
    private final String CUSTOMER_RESPONSE_TOPIC;

    // Process names
    private static final String FRAUD_PROCESS_CONTAINER = "ccd-fraud-kjar-1_0-SNAPSHOT";
    private static final String STANDARD_PROCESS_CONTAINER = "ccd-standard-kjar-1_0-SNAPSHOT";

    public AppRoute() {
        USE_SELDON_STANDARD = System.getenv("SELDON_STANDARD") != null;

        BROKER_URL = System.getenv("BROKER_URL");

        KAFKA_TOPIC = System.getenv("KAFKA_TOPIC");

        KIE_SERVER_URL = System.getenv("KIE_SERVER_URL");

        SELDON_URL = System.getenv("SELDON_URL");


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

        SELDON_ENDPOINT = System.getenv(SELDON_ENDPOINT_KEY);

        if (SELDON_ENDPOINT == null) {
            logger.debug("Using default Seldon endpoint /predict");
            SELDON_ENDPOINT = SELDON_ENDPOINT_DEFAULT;
        } else {
            logger.debug("Using custom Seldon endpoint " + SELDON_ENDPOINT);
        }

        CUSTOMER_NOTIFICATION_TOPIC = System.getenv("CUSTOMER_NOTIFICATION_TOPIC");

        if (CUSTOMER_NOTIFICATION_TOPIC == null) {
            logger.error("No customer notification kafka topic provided (CUSTOMER_NOTIFICATION_TOPIC)");
            throw new IllegalArgumentException("Invalid CUSTOMER_NOTIFICATION_TOPIC");
        }

        CUSTOMER_RESPONSE_TOPIC = System.getenv("CUSTOMER_RESPONSE_TOPIC");

        if (CUSTOMER_RESPONSE_TOPIC == null) {
            logger.error("No customer response Kafka topic provided (CUSTOMER_RESPONSE_TOPIC)");
            throw new IllegalArgumentException("Invalid CUSTOMER_RESPONSE_TOPIC");
        }

        seldonStrategy = new SeldonAggregationStrategy();

    }

    public void configure() {

        requestMapper.enable(SerializationFeature.WRAP_ROOT_VALUE);


        from("kafka:" + KAFKA_TOPIC + "?brokers=" + BROKER_URL).routeId("mainRoute")
                .log("incoming payload: ${body}")
                .to("micrometer:counter:transaction.incoming?increment=1")
                .process(new PredictionProcessor(USE_SELDON_STANDARD, SELDON_TOKEN))
                .log("outgoing payload: ${body}")

                .setHeader(Exchange.HTTP_METHOD, constant("POST"))
                .setHeader(Exchange.CONTENT_TYPE, constant("application/json"))
                .enrich(SELDON_URL + "/" + SELDON_ENDPOINT, seldonStrategy)
                .log("enriched: ${body}")
                .choice()
                .when(header("fraudulent").isEqualTo(true))
                .marshal(new JacksonDataFormat())
                .to("micrometer:counter:transaction.outgoing?increment=1&tags=type=fraud")
                .to(KIE_SERVER_URL + "/rest/server/containers/" + FRAUD_PROCESS_CONTAINER + "/processes/ccd-fraud-kjar.CCDProcess/instances")
                .otherwise()
                .marshal(new JacksonDataFormat())
                .to("micrometer:counter:transaction.outgoing?increment=1&tags=type=standard")
                .to(KIE_SERVER_URL + "/rest/server/containers/" + STANDARD_PROCESS_CONTAINER + "/processes/ccd-fraud-kjar.CCDProcess/instances");

        from("kafka:" + CUSTOMER_NOTIFICATION_TOPIC + "?brokers=" + BROKER_URL).routeId("customerIncoming")
                .log("${body}")
                .to("micrometer:counter:notifications.outgoing?increment=1");

        from("kafka:" + CUSTOMER_RESPONSE_TOPIC + "?brokers=" + BROKER_URL).routeId("customerResponse")
                .process(exchange -> {
                    final String payload = exchange.getIn().getBody(String.class);
                    ObjectMapper mapper = new ObjectMapper();
                    NotificationResponse response = mapper.readValue(payload, NotificationResponse.class);
                    exchange.getOut().setHeader("processId", response.responseId);
                    exchange.getOut().setBody(response.response);
                    exchange.getOut().setHeader("response", response.response ? "approved" : "non_approved");
                })
                .marshal(new JacksonDataFormat())
                .log("${body}")
                .to("micrometer:counter:notifications.incoming?increment=1&tags=response=${header.response}")
                .toD(KIE_SERVER_URL + "/rest/server/containers/" + FRAUD_PROCESS_CONTAINER + "/processes/instances/${header.processId}/signal/customerAcknowledgement");
    }
}
