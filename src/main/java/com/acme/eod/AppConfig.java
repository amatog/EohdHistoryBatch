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
        /** Inline symbols list (optional). */
        public List<String> list;

        /** Symbols JSON file path (preferred key name). */
        public String file;

        /** Symbols JSON file path (legacy/alias key name used in your YAML). */
        public String jsonFile;

        /** Optional whitelist for "place" field in JSON entries. */
        public List<String> placeWhitelist;
    }

    public static class Output {
        public String dir;
        public String primaryDir = "data_primary";
        public String symbolDir = "data_by_symbol";
        public String compression = "SNAPPY";
        public int expectedSymbols = 0;
    }

    public static class Runtime {
        public int httpTimeoutSeconds = 30;
        public int retries = 5;
        public long retryBackoffMillis = 1500;
        public long minDelayBetweenRequestsMillis = 250;
    }
}
