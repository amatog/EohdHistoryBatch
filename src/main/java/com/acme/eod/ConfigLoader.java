package com.acme.eod;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

public class ConfigLoader {

    public static AppConfig load(Path path) throws Exception {
        ObjectMapper om = new ObjectMapper(new YAMLFactory());
        AppConfig cfg = om.readValue(Files.readAllBytes(path), AppConfig.class);

        if (cfg.output == null) cfg.output = new AppConfig.Output();
        if (cfg.runtime == null) cfg.runtime = new AppConfig.Runtime();
        if (cfg.symbols == null) cfg.symbols = new AppConfig.Symbols();
        if (cfg.eodhd == null) cfg.eodhd = new AppConfig.Eodhd();

        if (cfg.output.dir == null || cfg.output.dir.isBlank()) {
            throw new IllegalArgumentException("output.dir is required in config");
        }

        // Backward compatible: symbols.jsonFile is an alias for symbols.file
        if ((cfg.symbols.file == null || cfg.symbols.file.isBlank())
                && (cfg.symbols.jsonFile != null && !cfg.symbols.jsonFile.isBlank())) {
            cfg.symbols.file = cfg.symbols.jsonFile;
        }

        return cfg;
    }

    public static List<String> loadSymbols(AppConfig cfg) throws Exception {
        if (cfg.symbols == null) throw new IllegalArgumentException("symbols section missing");

        // 1) Inline list
        if (cfg.symbols.list != null && !cfg.symbols.list.isEmpty()) {
            return dedupAndTrim(cfg.symbols.list);
        }

        // 2) File based (supports both 'file' and 'jsonFile' via aliasing in load())
        if (cfg.symbols.file != null && !cfg.symbols.file.isBlank()) {
            return loadSymbolsFromJson(Path.of(cfg.symbols.file), cfg.symbols.placeWhitelist);
        }

        throw new IllegalArgumentException("No symbols provided. Use symbols.list or symbols.file/jsonFile");
    }

    private static List<String> loadSymbolsFromJson(Path file, List<String> placeWhitelist) throws Exception {
        if (!Files.exists(file)) {
            throw new IllegalArgumentException("symbols file not found: " + file.toAbsolutePath());
        }

        ObjectMapper om = new ObjectMapper();
        JsonNode root = om.readTree(Files.readAllBytes(file));

        List<String> out = new ArrayList<>();

        if (root.isArray()) {
            for (JsonNode n : root) {
                if (n.isTextual()) {
                    out.add(n.asText());
                } else if (n.isObject() && n.hasNonNull("symbol")) {
                    if (placeWhitelist != null && !placeWhitelist.isEmpty()) {
                        String place = n.hasNonNull("place") ? n.get("place").asText() : "";
                        if (!placeWhitelist.contains(place)) continue;
                    }
                    out.add(n.get("symbol").asText());
                }
            }
        } else {
            throw new IllegalArgumentException("symbols.json must be an array (strings or objects with field 'symbol')");
        }

        if (out.isEmpty()) {
            throw new IllegalArgumentException("No symbols loaded from " + file.toAbsolutePath()
                    + " (check placeWhitelist and JSON content)");
        }

        return dedupAndTrim(out);
    }

    private static List<String> dedupAndTrim(List<String> in) {
        LinkedHashSet<String> set = new LinkedHashSet<>();
        for (String s : in) {
            if (s == null) continue;
            String t = s.trim();
            if (!t.isEmpty()) set.add(t);
        }
        return new ArrayList<>(set);
    }
}
