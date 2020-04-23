package dev.ruivieira.ccfd.routes.processors;

import dev.ruivieira.ccfd.routes.messages.MessageParser;
import dev.ruivieira.ccfd.routes.messages.v1.PredictionRequest;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class PredictionProcessor implements Processor {

    final boolean useSeldonStandard;
    final boolean useSeldonToken;
    final String seldonToken;

    public PredictionProcessor(boolean useSeldonStandard, String seldonToken) {
        this.useSeldonStandard = useSeldonStandard;
        this.useSeldonToken = seldonToken != null;
        this.seldonToken = seldonToken;

    }

    @Override
    public void process(Exchange exchange) throws Exception {
        // deserialise Kafka message
        final List<Double> feature = new ArrayList<>();
        final String payload = exchange.getIn().getBody().toString();
        final List<String> kafkaFeatures;

        final int[] indices = {3, 4, 10, 11, 12, 14, 17, 29};
        Integer amountIndex;
        if (useSeldonStandard) {
            kafkaFeatures = MessageParser.parseV0(payload);
            // extract the features of interest
            for (int index : indices) {
                feature.add(Double.parseDouble(kafkaFeatures.get(index)));
            }
            amountIndex = 30;
        } else {
            kafkaFeatures = MessageParser.parseV1(payload);
            // extract the features of interest
            for (int index : indices) {
                feature.add(Double.parseDouble(kafkaFeatures.get(index - 1)));
            }
            amountIndex = 29;
        }

        exchange.getOut().setHeader("amount", Double.parseDouble(kafkaFeatures.get(amountIndex)));

        String outgoingPayload;

        if (useSeldonStandard) {
            final PredictionRequest requestObject = new PredictionRequest();
            requestObject.addFeatures(feature);
            outgoingPayload = PredictionRequest.toJSON(requestObject);
        } else {
            outgoingPayload = "{\"strData\":\"";

            outgoingPayload += feature.stream()
                    .map(Object::toString)
                    .collect(Collectors.joining(","));
            outgoingPayload += "\"}";
        }

        exchange.getOut().setBody(outgoingPayload);
        if (useSeldonToken) {
            exchange.getOut().setHeader("Authorization", "Bearer " + seldonToken);
        }

    }
}

