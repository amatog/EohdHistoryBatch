package com.acme.eod;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

public class EodhdClient {

    private final AppConfig cfg;
    private final HttpClient http;
    private final ObjectMapper om = new ObjectMapper();

    public EodhdClient(AppConfig cfg) {
        this.cfg = cfg;
        this.http = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(cfg.runtime.httpTimeoutSeconds))
                .build();
    }

    public List<PriceRow> fetch(String symbol, LocalDate from, LocalDate to) throws Exception {
        String encodedSymbol = URLEncoder.encode(symbol, StandardCharsets.UTF_8);

        String url = cfg.eodhd.baseUrl + "/" + encodedSymbol
                + "?from=" + URLEncoder.encode(from.toString(), StandardCharsets.UTF_8)
                + "&to=" + URLEncoder.encode(to.toString(), StandardCharsets.UTF_8)
                + "&period=" + URLEncoder.encode(cfg.eodhd.period, StandardCharsets.UTF_8)
                + "&api_token=" + URLEncoder.encode(cfg.eodhd.apiKey, StandardCharsets.UTF_8)
                + "&fmt=" + URLEncoder.encode(cfg.eodhd.fmt, StandardCharsets.UTF_8);

        int attempts = 0;
        while (true) {
            attempts++;
            try {
                HttpRequest req = HttpRequest.newBuilder()
                        .uri(URI.create(url))
                        .timeout(Duration.ofSeconds(cfg.runtime.httpTimeoutSeconds))
                        .GET()
                        .build();

                HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
                int sc = resp.statusCode();
                String bodyTrunc = truncate(resp.body(), 400);

                // Success
                if (sc >= 200 && sc < 300) {
                    return parse(symbol, resp.body());
                }

                // Non-retryable
                if (sc == 404) {
                    throw new RuntimeException("HTTP 404 Ticker Not Found: " + symbol);
                }
                if (sc == 401) {
                    throw new RuntimeException("HTTP 401 Unauthenticated (check apiKey)");
                }

                // Retryable
                if (sc == 429 || (sc >= 500 && sc <= 599)) {
                    throw new RuntimeException("HTTP " + sc + " body=" + bodyTrunc);
                }

                // Other 4xx - treat as non-retryable (input / auth / entitlement / bad request)
                throw new RuntimeException("HTTP " + sc + " (non-retryable) body=" + bodyTrunc);

            } catch (Exception ex) {

                // If we already know it's non-retryable, fail fast (no retry spam)
                String msg = ex.getMessage() != null ? ex.getMessage() : ex.toString();
                if (msg.contains("HTTP 404") || msg.contains("HTTP 401") || msg.contains("non-retryable")) {
                    throw ex;
                }

                if (attempts >= cfg.runtime.retries) {
                    throw ex;
                }

                System.out.println("[RETRY] symbol=" + symbol + " attempt=" + attempts + " err=" + msg);
                Thread.sleep(cfg.runtime.retryBackoffMillis);
            }
        }
    }


    private List<PriceRow> parse(String symbol, String json) throws Exception {
        JsonNode node = om.readTree(json);
        if (!node.isArray()) {
            throw new RuntimeException("Unexpected JSON (not array): " + truncate(json, 400));
        }

        List<PriceRow> out = new ArrayList<>();
        Instant now = Instant.now();

        for (JsonNode r : node) {
            PriceRow pr = new PriceRow();
            pr.date = LocalDate.parse(r.get("date").asText());
            pr.symbol = symbol;

            pr.open = r.path("open").asDouble();
            pr.high = r.path("high").asDouble();
            pr.low = r.path("low").asDouble();
            pr.close = r.path("close").asDouble();

            pr.adjustedClose = r.path("adjusted_close").asDouble(pr.close);
            pr.volume = r.path("volume").asLong(0);

            pr.source = "EODHD";
            pr.ingestedAt = now;
            out.add(pr);
        }

        return out;
    }

    private static String truncate(String s, int max) {
        if (s == null) return "";
        return s.length() <= max ? s : s.substring(0, max) + "...";
    }
}
