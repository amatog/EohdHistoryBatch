package com.acme.eod;

import java.nio.file.Path;
import java.time.LocalDate;
import java.util.List;
import java.util.ArrayList;




public class Main {

    public static void main(String[] args) throws Exception {
        ensureWindowsHadoopHome();

        String configPath = "./config/application.yaml";
        String mode = "ingest";   // default
        String ref = "SPY";       // default reference symbol for validate

        for (int i = 0; i < args.length; i++) {
            if ("--config".equals(args[i]) && i + 1 < args.length) {
                configPath = args[i + 1];
                            } else if ("--mode".equals(args[i]) && i + 1 < args.length) {
                                mode = args[i + 1];
                            } else if ("--ref".equals(args[i]) && i + 1 < args.length) {
                                ref = args[i + 1];
            }
        }

        AppConfig cfg = ConfigLoader.load(Path.of(configPath));
        List<String> symbols = ConfigLoader.loadSymbols(cfg);

        // ---- MODE SWITCH ----------------------------------------------------
                if ("validate".equalsIgnoreCase(mode)) {
                        // Writes CSV into cfg.output.dir (same root as ParquetStore)
                                TimeseriesCoverageValidator.runFromConfig(cfg, ref, "validation_coverage_report.csv");
                        return;
                    }
                if (!"ingest".equalsIgnoreCase(mode)) {
                        throw new IllegalArgumentException("Unknown --mode=" + mode + " (allowed: ingest, validate)");
                    }
                // ---------------------------------------------------------------------

        System.out.println("[BOOT] symbols=" + symbols.size()
                + " from=" + cfg.eodhd.from
                + " to=" + cfg.eodhd.to
                + " period=" + cfg.eodhd.period
                + " out=" + cfg.output.dir);

        EodhdClient client = new EodhdClient(cfg);
        ParquetStore store = new ParquetStore(cfg);

        LocalDate from = LocalDate.parse(cfg.eodhd.from);
        LocalDate to = LocalDate.parse(cfg.eodhd.to);

        List<PriceRow> allRows = new java.util.ArrayList<>();
        List<String> failed = new java.util.ArrayList<>();

        for (String sym : symbols) {
            System.out.println("[INGEST] symbol=" + sym);
            try {
                List<PriceRow> rows = client.fetch(sym, from, to);
                allRows.addAll(rows);
            } catch (Exception ex) {
                String msg = ex.getMessage() != null ? ex.getMessage() : ex.toString();
                failed.add(sym + " -> " + msg);
                System.out.println("[ERROR] symbol=" + sym + " err=" + msg);
            }
            Thread.sleep(cfg.runtime.minDelayBetweenRequestsMillis);
        }

// Jetzt genau EINMAL partitioniert schreiben (alle Symbole zusammen)
        store.writePartitioned(allRows);

        if (!failed.isEmpty()) {
            System.out.println("[DONE] with failures=" + failed.size());
            for (String f : failed) {
                System.out.println("[FAIL] " + f);
            }
        } else {
            System.out.println("[DONE] all ok");
        }
    }




    private static void ensureWindowsHadoopHome() {
        String os = System.getProperty("os.name", "").toLowerCase();
        if (!os.contains("win")) return;

        // If the user already configured it, do not override
        if (System.getProperty("hadoop.home.dir") != null) return;
        if (System.getenv("HADOOP_HOME") != null && !System.getenv("HADOOP_HOME").isBlank()) return;

        // Convention: ./hadoop (project root)
        String cwd = System.getProperty("user.dir");
        String hadoopHome = Path.of(cwd, "hadoop").toString();
        System.setProperty("hadoop.home.dir", hadoopHome);

        java.nio.file.Path winutils = Path.of(hadoopHome, "bin", "winutils.exe");
        if (!java.nio.file.Files.exists(winutils)) {
            System.out.println("[WARN] Windows detected. Hadoop requires winutils.exe for local Parquet writes.");
            System.out.println("[WARN] Please place winutils.exe at: " + winutils);
            System.out.println("[WARN] See: ./hadoop/README_WINUTILS.txt");
        } else {
            System.out.println("[INFO] Windows Hadoop home set to: " + hadoopHome);
        }
    }
}
