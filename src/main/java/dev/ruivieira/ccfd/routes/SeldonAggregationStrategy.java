package dev.ruivieira.ccfd.routes;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import dev.ruivieira.ccfd.routes.messages.PredictionRequest;
import dev.ruivieira.ccfd.routes.messages.PredictionResponse;
import org.apache.camel.AggregationStrategy;
import org.apache.camel.Exchange;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
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
            // build KIE server data payload
            List<Double> features = request.getData().getOutcomes().get(0);
            mergeResult.put("v3", features.get(0));
            mergeResult.put("v4", features.get(1));
            mergeResult.put("v10", features.get(2));
            mergeResult.put("v11", features.get(3));
            mergeResult.put("v12", features.get(4));
            mergeResult.put("v14", features.get(5));
            mergeResult.put("v17", features.get(6));
            mergeResult.put("v29", features.get(7));

            List<Double> prediction = response.getData().getOutcomes().get(0);
            boolean fraudulent = prediction.get(0) <= prediction.get(1);

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
