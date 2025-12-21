package com.acme.eod;

import java.util.List;

public class AppConfig {
    public Eodhd eodhd = new Eodhd();
    public Symbols symbols = new Symbols();
    public Output output = new Output();
    public Runtime runtime = new Runtime();

    public static class Eodhd {
        public String apiKey;
        public String baseUrl;
        public String from;
        public String to;
        public String period = "d";
        public String fmt = "json";
    }

    public static class Symbols {
        public String jsonFile;
        public List<String> placeWhitelist;
    }

    public static class Output {
        public String dir;
        public String compression = "SNAPPY";
    }

    public static class Runtime {
        public int httpTimeoutSeconds = 30;
        public int retries = 5;
        public long retryBackoffMillis = 1500;
        public long minDelayBetweenRequestsMillis = 250;
    }
}
