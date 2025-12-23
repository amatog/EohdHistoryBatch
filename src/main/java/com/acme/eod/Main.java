package com.acme.eod;

import java.nio.file.Path;
import java.time.LocalDate;
import java.util.List;
import java.util.ArrayList;

public class Main {

    public static void main(String[] args) throws Exception {
        ensureWindowsHadoopHome();

        String configPath = "./config/application.yaml";
        String mode = "ingest";   // ingest | validate | materialize
        String ref = "SPY";       // for validate
        String a2Mode = "INCREMENTAL"; // FULL_REBUILD | INCREMENTAL
        String dateFrom = null;   // override for materialize
        String dateTo = null;     // override for materialize

        for (int i = 0; i < args.length; i++) {
            if ("--config".equals(args[i]) && i + 1 < args.length) {
                configPath = args[i + 1];
            } else if ("--mode".equals(args[i]) && i + 1 < args.length) {
                mode = args[i + 1];
            } else if ("--ref".equals(args[i]) && i + 1 < args.length) {
                ref = args[i + 1];
            } else if ("--a2Mode".equals(args[i]) && i + 1 < args.length) {
                a2Mode = args[i + 1];
            } else if ("--dateFrom".equals(args[i]) && i + 1 < args.length) {
                dateFrom = args[i + 1];
            } else if ("--dateTo".equals(args[i]) && i + 1 < args.length) {
                dateTo = args[i + 1];
            }
        }

        AppConfig cfg = ConfigLoader.load(Path.of(configPath));

        // ---- MODE SWITCH ----------------------------------------------------
        if ("validate".equalsIgnoreCase(mode)) {
            TimeseriesCoverageValidator.runFromConfig(cfg, ref, "validation_coverage_report.csv");
            return;
        }

        if ("materialize".equalsIgnoreCase(mode)) {
            runA2Materialize(cfg, a2Mode, dateFrom, dateTo);
            return;
        }

        if (!"ingest".equalsIgnoreCase(mode)) {
            throw new IllegalArgumentException("Unknown --mode=" + mode + " (allowed: ingest, validate, materialize)");
        }
        // ---------------------------------------------------------------------

        // Ingest requires EODHD config + symbols
        if (cfg.eodhd.apiKey == null || cfg.eodhd.apiKey.isBlank()) {
            throw new IllegalArgumentException("eodhd.apiKey is required for ingest mode");
        }
        if (cfg.eodhd.from == null || cfg.eodhd.to == null) {
            throw new IllegalArgumentException("eodhd.from and eodhd.to are required for ingest mode");
        }

        List<String> symbols = ConfigLoader.loadSymbols(cfg);

        System.out.println("[BOOT] mode=ingest symbols=" + symbols.size()
                + " from=" + cfg.eodhd.from
                + " to=" + cfg.eodhd.to
                + " period=" + cfg.eodhd.period
                + " out=" + cfg.output.dir);

        EodhdClient client = new EodhdClient(cfg);
        ParquetStore store = new ParquetStore(cfg);

        LocalDate from = LocalDate.parse(cfg.eodhd.from);
        LocalDate to = LocalDate.parse(cfg.eodhd.to);

        List<PriceRow> allRows = new ArrayList<>();
        List<String> failed = new ArrayList<>();

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

        // Primary store write (date=...)
        store.writePartitioned(allRows);

        if (!failed.isEmpty()) {
            System.out.println("[DONE] with failures=" + failed.size());
            for (String f : failed) System.out.println("[FAIL] " + f);
        } else {
            System.out.println("[DONE] OK");
        }
    }

    private static void runA2Materialize(AppConfig cfg, String a2Mode, String dateFrom, String dateTo) throws Exception {
        // Candidate A: <output.dir>/<primaryDir>
        Path primaryA = Path.of(cfg.output.dir, cfg.output.primaryDir);
        // Candidate B: <output.dir> (legacy/current A.1 layout: date=... directly under output.dir)
        Path primaryB = Path.of(cfg.output.dir);

        Path primaryRoot = resolvePrimaryRoot(primaryA, primaryB);

        // BySymbol always under <output.dir>/<symbolDir>
        Path symbolRoot = Path.of(cfg.output.dir, cfg.output.symbolDir);

        MaterializeSymbolStoreJob.Args a = new MaterializeSymbolStoreJob.Args();
        a.primaryRoot = primaryRoot;
        a.symbolRoot = symbolRoot;
        a.mode = MaterializeSymbolStoreJob.Mode.valueOf(a2Mode.toUpperCase());
        a.compression = cfg.output.compression;
        a.expectedSymbols = cfg.output.expectedSymbols;

        a.dateFrom = (dateFrom != null && !dateFrom.isBlank()) ? java.time.LocalDate.parse(dateFrom) : null;
        a.dateTo = (dateTo != null && !dateTo.isBlank()) ? java.time.LocalDate.parse(dateTo) : null;

        System.out.println("[BOOT] mode=materialize primaryRoot=" + primaryRoot
                + " symbolRoot=" + symbolRoot
                + " a2Mode=" + a.mode
                + " dateFrom=" + (a.dateFrom != null ? a.dateFrom : "")
                + " dateTo=" + (a.dateTo != null ? a.dateTo : ""));

        new MaterializeSymbolStoreJob().run(a);

        System.out.println("[A2] DONE");
    }

    /**
     * Prefer <output.dir>/<primaryDir> if it contains date=YYYY-MM-DD partitions.
     * Otherwise fallback to <output.dir> if it contains date=... directly.
     */
    private static Path resolvePrimaryRoot(Path primaryA, Path primaryB) throws java.io.IOException {
        if (containsDatePartitions(primaryA)) return primaryA;
        if (containsDatePartitions(primaryB)) return primaryB;

        throw new IllegalStateException("No date partitions found under primaryRoot candidates: "
                + primaryA + " OR " + primaryB
                + " (expected folders like date=YYYY-MM-DD)");
    }

    private static boolean containsDatePartitions(Path root) throws java.io.IOException {
        if (!java.nio.file.Files.isDirectory(root)) return false;
        try (java.nio.file.DirectoryStream<java.nio.file.Path> ds = java.nio.file.Files.newDirectoryStream(root)) {
            for (java.nio.file.Path p : ds) {
                if (!java.nio.file.Files.isDirectory(p)) continue;
                String name = p.getFileName().toString();
                if (name.startsWith("date=") && name.length() == "date=YYYY-MM-DD".length()) {
                    // quick sanity check: date=####-##-##
                    return true;
                }
            }
        }
        return false;
    }


    private static void ensureWindowsHadoopHome() {
        // Keep your existing behavior
        if (!System.getProperty("os.name").toLowerCase().contains("win")) return;

        if (System.getProperty("hadoop.home.dir") != null) return;
        String env = System.getenv("HADOOP_HOME");
        if (env != null && !env.isBlank()) return;

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
