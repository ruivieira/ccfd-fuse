package dev.ruivieira.ccfd.routes.messages;

import com.fasterxml.jackson.annotation.JsonProperty;

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

    public Transaction() {
    }

    public Integer getId() {
        return this.id;
    }

    public Double getAmount() {
        return this.amount;
    }
}