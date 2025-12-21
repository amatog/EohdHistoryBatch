package com.acme.eod;

import java.time.Instant;
import java.time.LocalDate;

public class PriceRow {
    public LocalDate date;
    public String symbol;
    public double open;
    public double high;
    public double low;
    public double close;
    public double adjustedClose;
    public long volume;
    public String source;
    public Instant ingestedAt;
}
