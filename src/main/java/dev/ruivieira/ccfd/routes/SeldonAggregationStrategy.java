package dev.ruivieira.ccfd.routes;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import dev.ruivieira.ccfd.routes.messages.PredictionRequest;
import dev.ruivieira.ccfd.routes.messages.PredictionResponse;
import org.apache.camel.AggregationStrategy;
import org.apache.camel.Exchange;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class SeldonAggregationStrategy implements AggregationStrategy {

    private final ObjectMapper responseMapper = new ObjectMapper();

    public SeldonAggregationStrategy() {
        responseMapper.enable(SerializationFeature.WRAP_ROOT_VALUE);
    }

    public Exchange aggregate(Exchange original, Exchange resource) {
        Object originalBody = original.getIn().getBody();
        Object resourceResponse = resource.getIn().getBody(String.class);

        try {
            PredictionRequest request = PredictionRequest.fromString(originalBody.toString());
            PredictionResponse response = responseMapper.readValue(resourceResponse.toString(), PredictionResponse.class);

            Map<String, Object> mergeResult = new HashMap<>();
            mergeResult.put("account_id", request.getData().getOutcomes().get(0).get(0).intValue());
            mergeResult.put("transaction_amount", request.getData().getOutcomes().get(0).get(1));
            boolean fraudulent = response.getData().getOutcomes().get(0).get(0) >= response.getData().getOutcomes().get(0).get(1);

            if (original.getPattern().isOutCapable()) {
                original.getOut().setBody(mergeResult, Map.class);
                original.getOut().setHeader("fraudulent", fraudulent);
            } else {
                original.getIn().setBody(mergeResult, Map.class);
                original.getIn().setHeader("fraudulent", fraudulent);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        return original;
    }
}
