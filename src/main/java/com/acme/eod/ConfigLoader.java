package com.acme.eod;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

public class ConfigLoader {

    public static AppConfig load(Path path) throws Exception {
        ObjectMapper om = new ObjectMapper(new YAMLFactory());
        AppConfig cfg = om.readValue(Files.readAllBytes(path), AppConfig.class);

        if (cfg.eodhd == null || cfg.eodhd.apiKey == null || cfg.eodhd.apiKey.isBlank()) {
            throw new IllegalArgumentException("Missing eodhd.apiKey in config");
        }
        if (cfg.eodhd.baseUrl == null || cfg.eodhd.baseUrl.isBlank()) {
            throw new IllegalArgumentException("Missing eodhd.baseUrl in config");
        }
        if (cfg.eodhd.from == null || cfg.eodhd.from.isBlank()) {
            throw new IllegalArgumentException("Missing eodhd.from in config");
        }
        if (cfg.eodhd.to == null || cfg.eodhd.to.isBlank()) {
            throw new IllegalArgumentException("Missing eodhd.to in config");
        }
        if (cfg.output == null || cfg.output.dir == null || cfg.output.dir.isBlank()) {
            throw new IllegalArgumentException("Missing output.dir in config");
        }
        if (cfg.symbols == null || cfg.symbols.jsonFile == null || cfg.symbols.jsonFile.isBlank()) {
            throw new IllegalArgumentException("Missing symbols.jsonFile in config");
        }
        return cfg;
    }

    public static List<String> loadSymbols(AppConfig cfg) throws Exception {
        Path file = Path.of(cfg.symbols.jsonFile);
        if (!Files.exists(file)) {
            throw new IllegalArgumentException("symbols.jsonFile does not exist: " + file.toAbsolutePath());
        }

        ObjectMapper om = new ObjectMapper();
        SymbolEntry[] entries = om.readValue(Files.readAllBytes(file), SymbolEntry[].class);

        Set<String> whitelist = new HashSet<>();
        if (cfg.symbols.placeWhitelist != null) {
            for (String p : cfg.symbols.placeWhitelist) {
                if (p != null && !p.isBlank()) whitelist.add(p.trim().toUpperCase());
            }
        }

        List<String> out = new ArrayList<>();
        for (SymbolEntry e : entries) {
            if (e == null || e.symbol == null || e.symbol.isBlank()) continue;

            if (!whitelist.isEmpty()) {
                String place = (e.place == null) ? "" : e.place.trim().toUpperCase();
                if (!whitelist.contains(place)) continue;
            }
            out.add(e.symbol.trim());
        }

        if (out.isEmpty()) {
            throw new IllegalArgumentException("No symbols loaded from " + file.toAbsolutePath()
                    + " (check placeWhitelist and JSON content)");
        }

        LinkedHashSet<String> dedup = new LinkedHashSet<>(out);
        return new ArrayList<>(dedup);
    }
}
