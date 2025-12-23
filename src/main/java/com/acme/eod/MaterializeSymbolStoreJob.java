package com.acme.eod;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;

import java.io.IOException;
import java.nio.file.*;
import java.time.Instant;
import java.time.LocalDate;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A.2 Job: Materialize symbol-oriented store from Primary date-partitioned store.
 *
 * Input (Primary):
 *   <primaryRoot>/date=YYYY-MM-DD/part-*.parquet
 *
 * Output (BySymbol, YEAR chunking, append-safe):
 *   <symbolRoot>/symbol=XYZ/year=YYYY/part-YYYY-MM-DD.parquet
 *
 * State:
 *   <symbolRoot>/_state/materialize_state.json
 *
 * Audit:
 *   <symbolRoot>/_audit/run_<runId>.json
 */
public class MaterializeSymbolStoreJob {

    public enum Mode { FULL_REBUILD, INCREMENTAL }

    public static class Args {
        public Path primaryRoot;
        public Path symbolRoot;
        public LocalDate dateFrom;         // optional
        public LocalDate dateTo;           // optional
        public Mode mode = Mode.INCREMENTAL;
        public String compression = "SNAPPY";
        public int expectedSymbols = 0;    // warn only
    }

    /** State persisted under <symbolRoot>/_state/materialize_state.json */
    public static class State {
        public Set<String> processedDates = new HashSet<>();
        public String lastRunId;
        public String lastUpdatedAtUtc;
    }

    private static final Pattern DATE_PART = Pattern.compile("^date=(\\d{4}-\\d{2}-\\d{2})$");

    public void run(Args args) throws Exception {
        Objects.requireNonNull(args, "args");
        Objects.requireNonNull(args.primaryRoot, "args.primaryRoot");
        Objects.requireNonNull(args.symbolRoot, "args.symbolRoot");

        String runId = "a2_" + Instant.now().toString().replace(":", "").replace("-", "").replace(".", "");
        Path stateFile = args.symbolRoot.resolve("_state").resolve("materialize_state.json");
        Path auditDir = args.symbolRoot.resolve("_audit");
        Files.createDirectories(auditDir);

        ObjectMapper om = new ObjectMapper();

        State st = loadState(om, stateFile);
        if (args.mode == Mode.FULL_REBUILD) {
            // We reset state; you may delete symbolRoot content manually if desired.
            st = new State();
        }

        List<LocalDate> available = discoverDatePartitions(args.primaryRoot);
        if (available.isEmpty()) {
            throw new IllegalStateException("No date partitions found under primaryRoot: " + args.primaryRoot);
        }

        List<LocalDate> requested = filterRange(available, args.dateFrom, args.dateTo);

        List<LocalDate> toProcess = new ArrayList<>();
        for (LocalDate d : requested) {
            if (args.mode == Mode.FULL_REBUILD || !st.processedDates.contains(d.toString())) {
                toProcess.add(d);
            }
        }

        if (toProcess.isEmpty()) {
            Map<String, Object> noopAudit = new LinkedHashMap<>();
            noopAudit.put("run_id", runId);
            noopAudit.put("status", "NOOP");
            noopAudit.put("message", "No new date partitions to process");
            noopAudit.put("mode", args.mode.name());
            noopAudit.put("primaryRoot", args.primaryRoot.toString());
            noopAudit.put("symbolRoot", args.symbolRoot.toString());
            noopAudit.put("dateFrom", args.dateFrom != null ? args.dateFrom.toString() : "");
            noopAudit.put("dateTo", args.dateTo != null ? args.dateTo.toString() : "");
            noopAudit.put("updatedAtUtc", Instant.now().toString());

            writeAudit(om, auditDir.resolve("run_" + runId + ".json"), noopAudit);
            return;
        }

        long totalRows = 0;
        long totalInputFiles = 0;
        long totalSymbolWrites = 0;

        // Process date by date (bounded memory)
        for (LocalDate date : toProcess) {
            Path dateDir = args.primaryRoot.resolve("date=" + date);
            if (!Files.isDirectory(dateDir)) {
                System.out.println("[A2][WARN] Missing partition dir: " + dateDir);
                continue;
            }

            List<Path> parquetFiles = listParquetFiles(dateDir);
            if (parquetFiles.isEmpty()) {
                System.out.println("[A2][WARN] No parquet files in: " + dateDir);
                continue;
            }

            Map<String, List<PriceRow>> bySymbol = new HashMap<>();
            Set<String> symbolsSeenThisDate = new HashSet<>();
            long rowsThisDate = 0;

            for (Path pf : parquetFiles) {
                totalInputFiles++;
                rowsThisDate += readParquetIntoGroups(pf, bySymbol, symbolsSeenThisDate);
            }

            // Universe-size check (warn only)
            if (args.expectedSymbols > 0 && symbolsSeenThisDate.size() < args.expectedSymbols) {
                System.out.println("[A2][WARN] date=" + date
                        + " symbols=" + symbolsSeenThisDate.size()
                        + " < expectedSymbols=" + args.expectedSymbols);
            }

            // Write per symbol into symbol/year directory, filename includes date (append-safe)
            for (Map.Entry<String, List<PriceRow>> e : bySymbol.entrySet()) {
                String sym = e.getKey();
                List<PriceRow> rows = e.getValue();

                // Validate: expect exactly one row for this symbol on this date
                ensureExactlyOneRowForDate(rows, sym, date);

                int year = date.getYear();
                Path outDir = args.symbolRoot
                        .resolve("symbol=" + sym)
                        .resolve("year=" + year);

                Files.createDirectories(outDir);

                // One file per (symbol,date) to avoid concurrent append complexity
                Path outFile = outDir.resolve("part-" + date + ".parquet");

                // Write using existing ParquetStore schema
                AppConfig tmpCfg = new AppConfig();
                tmpCfg.output = new AppConfig.Output();
                tmpCfg.output.compression = args.compression;
                tmpCfg.output.dir = args.symbolRoot.toString(); // not used directly by writeParquet()

                ParquetStore store = new ParquetStore(tmpCfg);
                store.writeParquet(outFile, rows);

                totalSymbolWrites++;
            }

            totalRows += rowsThisDate;
            st.processedDates.add(date.toString());

            System.out.println("[A2] processed date=" + date
                    + " rows=" + rowsThisDate
                    + " symbols=" + symbolsSeenThisDate.size());
        }

        st.lastRunId = runId;
        st.lastUpdatedAtUtc = Instant.now().toString();
        saveState(om, stateFile, st);

        Map<String, Object> okAudit = new LinkedHashMap<>();
        okAudit.put("run_id", runId);
        okAudit.put("status", "OK");
        okAudit.put("mode", args.mode.name());
        okAudit.put("primaryRoot", args.primaryRoot.toString());
        okAudit.put("symbolRoot", args.symbolRoot.toString());
        okAudit.put("dateFrom", args.dateFrom != null ? args.dateFrom.toString() : "");
        okAudit.put("dateTo", args.dateTo != null ? args.dateTo.toString() : "");
        okAudit.put("processedDatesCount", toProcess.size());
        okAudit.put("totalInputFiles", totalInputFiles);
        okAudit.put("totalRows", totalRows);
        okAudit.put("symbolWrites", totalSymbolWrites);
        okAudit.put("updatedAtUtc", st.lastUpdatedAtUtc);

        writeAudit(om, auditDir.resolve("run_" + runId + ".json"), okAudit);
    }

    // -------------------- Read/Write Helpers --------------------

    private static long readParquetIntoGroups(Path parquetFile,
                                              Map<String, List<PriceRow>> bySymbol,
                                              Set<String> symbolsSeenThisDate) throws IOException {

        long rows = 0;
        Configuration conf = new Configuration();

        org.apache.hadoop.fs.Path hp = new org.apache.hadoop.fs.Path(parquetFile.toUri());
        try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(hp).withConf(conf).build()) {

            GenericRecord r;
            while ((r = reader.read()) != null) {
                String symbol = asString(r, "symbol");
                String dateStr = asString(r, "date");

                if (symbol == null || symbol.isBlank()) {
                    throw new IllegalStateException("Missing symbol in " + parquetFile);
                }
                if (dateStr == null || dateStr.isBlank()) {
                    throw new IllegalStateException("Missing date in " + parquetFile);
                }

                PriceRow pr = new PriceRow();
                pr.symbol = symbol;
                pr.date = LocalDate.parse(dateStr);
                pr.open = asDouble(r, "open");
                pr.high = asDouble(r, "high");
                pr.low = asDouble(r, "low");
                pr.close = asDouble(r, "close");
                pr.adjustedClose = hasField(r, "adjusted_close") ? asDouble(r, "adjusted_close") : pr.close;
                pr.volume = asLong(r, "volume");
                pr.source = hasField(r, "source") ? asString(r, "source") : "EODHD";

                long ingMs = hasField(r, "ingested_at_epoch_ms") ? asLong(r, "ingested_at_epoch_ms") : System.currentTimeMillis();
                pr.ingestedAt = Instant.ofEpochMilli(ingMs);

                // Sanity checks (fail fast)
                if (pr.close <= 0.0) throw new IllegalStateException("Invalid close<=0 for " + symbol + " " + pr.date + " in " + parquetFile);
                if (pr.high < pr.low) throw new IllegalStateException("Invalid high<low for " + symbol + " " + pr.date + " in " + parquetFile);
                if (pr.volume < 0) throw new IllegalStateException("Invalid volume<0 for " + symbol + " " + pr.date + " in " + parquetFile);

                bySymbol.computeIfAbsent(symbol, k -> new ArrayList<>()).add(pr);
                symbolsSeenThisDate.add(symbol);
                rows++;
            }
        }

        return rows;
    }

    private static void ensureExactlyOneRowForDate(List<PriceRow> rows, String symbol, LocalDate expectedDate) {
        if (rows == null || rows.isEmpty()) {
            throw new IllegalStateException("No rows for symbol=" + symbol + " expectedDate=" + expectedDate);
        }
        if (rows.size() != 1) {
            throw new IllegalStateException("Duplicate rows for symbol=" + symbol + " date=" + expectedDate + " rows=" + rows.size());
        }
        PriceRow r = rows.get(0);
        if (!expectedDate.equals(r.date)) {
            throw new IllegalStateException("Mixed date for symbol=" + symbol + " expectedDate=" + expectedDate + " actualDate=" + r.date);
        }
    }

    // -------------------- Partition Discovery --------------------

    private static List<LocalDate> discoverDatePartitions(Path primaryRoot) throws IOException {
        if (!Files.isDirectory(primaryRoot)) return List.of();
        List<LocalDate> out = new ArrayList<>();

        try (DirectoryStream<Path> ds = Files.newDirectoryStream(primaryRoot)) {
            for (Path p : ds) {
                if (!Files.isDirectory(p)) continue;
                Matcher m = DATE_PART.matcher(p.getFileName().toString());
                if (!m.matches()) continue;
                out.add(LocalDate.parse(m.group(1)));
            }
        }

        out.sort(Comparator.naturalOrder());
        return out;
    }

    private static List<LocalDate> filterRange(List<LocalDate> all, LocalDate from, LocalDate to) {
        if (all == null || all.isEmpty()) return List.of();
        if (from == null && to == null) return all;

        List<LocalDate> out = new ArrayList<>();
        for (LocalDate d : all) {
            if (from != null && d.isBefore(from)) continue;
            if (to != null && d.isAfter(to)) continue;
            out.add(d);
        }
        return out;
    }

    private static List<Path> listParquetFiles(Path dir) throws IOException {
        List<Path> out = new ArrayList<>();
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(dir)) {
            for (Path p : ds) {
                if (Files.isRegularFile(p) && p.getFileName().toString().endsWith(".parquet")) {
                    out.add(p);
                }
            }
        }
        out.sort(Comparator.naturalOrder());
        return out;
    }

    // -------------------- Record Field Helpers --------------------

    private static boolean hasField(GenericRecord r, String field) {
        return r != null && r.getSchema() != null && r.getSchema().getField(field) != null;
    }

    private static String asString(GenericRecord r, String field) {
        Object v = r.get(field);
        return v == null ? null : v.toString();
    }

    private static double asDouble(GenericRecord r, String field) {
        Object v = r.get(field);
        if (v == null) return Double.NaN;
        if (v instanceof Number n) return n.doubleValue();
        return Double.parseDouble(v.toString());
    }

    private static long asLong(GenericRecord r, String field) {
        Object v = r.get(field);
        if (v == null) return 0L;
        if (v instanceof Number n) return n.longValue();
        return Long.parseLong(v.toString());
    }

    // -------------------- State & Audit --------------------

    private static State loadState(ObjectMapper om, Path stateFile) throws IOException {
        if (!Files.exists(stateFile)) return new State();
        return om.readValue(Files.readAllBytes(stateFile), State.class);
    }

    private static void saveState(ObjectMapper om, Path stateFile, State st) throws IOException {
        Files.createDirectories(stateFile.getParent());
        om.writerWithDefaultPrettyPrinter().writeValue(stateFile.toFile(), st);
    }

    private static void writeAudit(ObjectMapper om, Path auditFile, Map<String, Object> payload) throws IOException {
        Files.createDirectories(auditFile.getParent());
        om.writerWithDefaultPrettyPrinter().writeValue(auditFile.toFile(), payload);
    }
}
