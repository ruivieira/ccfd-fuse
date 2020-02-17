package dev.ruivieira.ccfd.routes;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import dev.ruivieira.ccfd.routes.messages.PredictionRequest;
import dev.ruivieira.ccfd.routes.messages.PredictionResponse;
import dev.ruivieira.ccfd.routes.messages.Transaction;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.List;

@Component
public class AppRoute extends RouteBuilder {

    private static final Logger logger = LoggerFactory.getLogger(AppRoute.class);
    final ObjectMapper requestMapper = new ObjectMapper();
    final ObjectMapper responseMapper = new ObjectMapper();

    public void configure() {

        requestMapper.enable(SerializationFeature.WRAP_ROOT_VALUE);

        final String BROKER_URL = System.getenv("BROKER_URL");
        final String KAFKA_TOPIC = System.getenv("KAFKA_TOPIC");
        final String KIE_SERVER_URL = System.getenv("KIE_SERVER_URL");
        final String SELDON_URL = System.getenv("SELDON_URL");

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

        if (SELDON_URL==null) {
            final String message = "No Seldon server provided";
            logger.error(message);
            throw new IllegalArgumentException(message);
        }



        from("kafka:" + KAFKA_TOPIC + "?brokers=" + BROKER_URL).routeId("mainRoute")
                .process(exchange -> {
//                    // deserialise Kafka message
//                    final ObjectMapper mapper = new ObjectMapper();
//                    final Object body = exchange.getIn().getBody();
//                    final Transaction transaction = mapper.readValue(body.toString(), Transaction.class);
//                    final Map<String, Object> map = new HashMap<>();
//                    map.put("account_id", transaction.getId());
//                    map.put("transaction_amount", transaction.getAmount());
//                    exchange.getOut().setBody(map);
//                    System.out.println(exchange.getIn().getBody().toString());
                    final List<List<Double>> features = new ArrayList<>();
                    final List<Double> feature = new ArrayList<>();
                    final ObjectMapper mapper = new ObjectMapper();
                    final Object body = exchange.getIn().getBody();
                    final Transaction transaction = mapper.readValue(body.toString(), Transaction.class);
                    feature.add(Double.valueOf(transaction.getId()));
                    feature.add(transaction.getAmount());
                    features.add(feature);
                    final PredictionRequest requestObject = new PredictionRequest(features);

                    final String JSON = requestMapper.writeValueAsString(requestObject);

//                    if (USE_SELDON_TOKEN) {
//                        request = request.header("Authorization", "Bearer " + SELDON_TOKEN);
//                    }
                    exchange.getOut().setBody(JSON);
                })
//                .marshal(new JacksonDataFormat())
                .setHeader(Exchange.HTTP_METHOD, constant("POST"))
                .setHeader(Exchange.CONTENT_TYPE, constant("application/json"))
                .to(SELDON_URL + "/predict")
        .process(exchange -> {
            // parse the response
            final String strResponse = exchange.getIn().getBody(String.class);
            responseMapper.enable(SerializationFeature.WRAP_ROOT_VALUE);
            final PredictionResponse response = requestMapper.readValue(strResponse, PredictionResponse.class);
            final List<Double> probabilities = response.getData().getOutcomes().get(0);
            final double pA = probabilities.get(0);
            final double pB = probabilities.get(1);

            if (pA >= pB) {
                exchange.getOut().setBody(true, Boolean.class);
            } else {
                exchange.getOut().setBody(false, Boolean.class);
            }


        })
        .choice()
        .when(body().isEqualTo(true)).log("It is true").when(body().isEqualTo(false)).log("It is false");
        // send to another container appropriately
    }
}
