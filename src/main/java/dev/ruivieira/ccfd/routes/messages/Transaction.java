package dev.ruivieira.ccfd.routes.messages;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class Transaction {
    @JsonProperty(
        value = "id",
        required = true
    )
    private Integer id;
    @JsonProperty(
        value = "amount",
        required = true
    )
    private Double amount;

    @JsonProperty(
            value = "features"
    )
    private List<Double> features;

    public Transaction() {
    }

    public Integer getId() {
        return this.id;
    }

    public Double getAmount() {
        return this.amount;
    }

    public List<Double> getFeatures() {
        return features;
    }
}