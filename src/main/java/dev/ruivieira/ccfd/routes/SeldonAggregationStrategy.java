package dev.ruivieira.ccfd.routes;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import dev.ruivieira.ccfd.routes.messages.v1.PredictionRequest;
import dev.ruivieira.ccfd.routes.model.Classification;
import dev.ruivieira.ccfd.routes.model.Prediction;
import org.apache.camel.AggregationStrategy;
import org.apache.camel.Exchange;
import org.kie.api.KieServices;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class SeldonAggregationStrategy implements AggregationStrategy {

    private static final Logger logger = LoggerFactory.getLogger(SeldonAggregationStrategy.class);

    private Boolean USE_SELDON_STANDARD;

    private final ObjectMapper responseMapper = new ObjectMapper();

    private KieSession kieSession;

    final String FRAUDULENT_HEADER = "fraudulent";

    public SeldonAggregationStrategy() {
        USE_SELDON_STANDARD = System.getenv("SELDON_STANDARD") != null;
        responseMapper.enable(SerializationFeature.WRAP_ROOT_VALUE);

        KieServices kieServices = KieServices.Factory.get();
        KieContainer kContainer = kieServices.getKieClasspathContainer();
        kieSession = kContainer.newKieSession();
    }

    public Exchange aggregate(Exchange original, Exchange resource) {
        final Object originalBody = original.getIn().getBody();
        final Object resourceResponse = resource.getIn().getBody(String.class);


        List<Double> features = new ArrayList<>();

        try {
            if (USE_SELDON_STANDARD) {
                PredictionRequest request = PredictionRequest.fromString(originalBody.toString());
                // build KIE server data payload
                features = request.getData().getOutcomes().get(0);
            } else {
                ObjectMapper requestMapper = new ObjectMapper();
                Map<String, String> map = requestMapper.readValue(originalBody.toString(), Map.class);
                String[] featureString = map.get("strData").split(",");
                for (String f : featureString) {
                    features.add(Double.parseDouble(f));
                }
            }


            Prediction prediction = new Prediction();

            if (USE_SELDON_STANDARD) {
                dev.ruivieira.ccfd.routes.messages.v1.PredictionResponse response = responseMapper.readValue(resourceResponse.toString(), dev.ruivieira.ccfd.routes.messages.v1.PredictionResponse.class);
                prediction.setProbability(response.getData().getOutcomes().get(0).get(0));
            } else {
                dev.ruivieira.ccfd.routes.messages.v0.PredictionResponse response = responseMapper.readValue(resourceResponse.toString(), dev.ruivieira.ccfd.routes.messages.v0.PredictionResponse.class);
                List<Double> predictionList = response.getData().getOutcomes();
                prediction.setProbability(predictionList.get(0));
            }

            final Classification classification = new Classification();
            kieSession.setGlobal("classification", classification);
            kieSession.insert(prediction);
            kieSession.fireAllRules();

            final Map<String, Object> mergeResult = new HashMap<>();

            // generate a random customer id, since this is not available in the original dataset
            mergeResult.put("customer_id", new Random().nextInt(10000));

            // add selected features from the Kaggle dataset
            mergeResult.put("v3", features.get(0));
            mergeResult.put("v4", features.get(1));
            mergeResult.put("v10", features.get(2));
            mergeResult.put("v11", features.get(3));
            mergeResult.put("v12", features.get(4));
            mergeResult.put("v14", features.get(5));
            mergeResult.put("v17", features.get(6));
            mergeResult.put("v29", features.get(7));
            mergeResult.put("fraud_probability", prediction.getProbability());
            mergeResult.put("amount", original.getIn().getHeader("amount"));

            logger.info("Merged payload: " + mergeResult.toString());



            if (original.getPattern().isOutCapable()) {
                original.getOut().setBody(mergeResult, Map.class);
                original.getOut().setHeader(FRAUDULENT_HEADER, classification.isFraudulent());
            } else {
                original.getIn().setBody(mergeResult, Map.class);
                original.getIn().setHeader(FRAUDULENT_HEADER, classification.isFraudulent());
            }

        } catch (IOException e) {
            logger.error("Not possible to merge payload with prediction");
            logger.error(e.getMessage());
        }

        return original;
    }
}
