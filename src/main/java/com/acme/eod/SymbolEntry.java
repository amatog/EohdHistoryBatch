package com.acme.eod;

import com.fasterxml.jackson.annotation.JsonProperty;

public class SymbolEntry {
    @JsonProperty("Place")
    public String place;

    @JsonProperty("Name")
    public String name;

    @JsonProperty("Symbol")
    public String symbol;
}
