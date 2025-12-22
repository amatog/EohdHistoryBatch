package com.acme.eod;

import org.apache.avro.SchemaParseException;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;

/**
 * TimeseriesCoverageValidator
 *
 * Validiert Missing Bars / Coverage pro Symbol bezogen auf einen Referenzkalender.
 *
 * Referenzkalender = alle Dates, an denen das Referenzsymbol (z.B. SPY) eine Bar hat.
 * Falls Referenzsymbol nicht im Dataset vorkommt, wird automatisch ein Fallback-Referenzsymbol gewählt
 * (das "häufigste" Symbol aus einem Sample).
 *
 * Erwartetes Layout (Price-Parquets):
 *   <root>/date=YYYY-MM-DD/part-*.parquet
 *
 * Erwartetes Schema (Avro GenericRecord):
 *   symbol (String)
 *   date   (String, ISO yyyy-MM-dd)
 *
 * Output:
 *   CSV: <root>/validation_coverage_report.csv (oder beliebiger Dateiname via runFromConfig)
 *   CSV enthält auditierbare Felder: calendar_reference_used, calendar_source
 */
public class TimeseriesCoverageValidator {

    private final java.nio.file.Path rootDir;       // NIO Path
    private final String referenceSymbol;           // user-requested ref (may not exist)
    private final java.nio.file.Path reportCsv;     // NIO Path

    public TimeseriesCoverageValidator(java.nio.file.Path rootDir, String referenceSymbol, java.nio.file.Path reportCsv) {
        this.rootDir = Objects.requireNonNull(rootDir, "rootDir");
        this.referenceSymbol = Objects.requireNonNull(referenceSymbol, "referenceSymbol");
        this.reportCsv = Objects.requireNonNull(reportCsv, "reportCsv");
    }

    /**
     * Convenience entrypoint compatible with your existing config loader.
     * Writes CSV into cfg.output.dir
     */
    public static void runFromConfig(AppConfig cfg, String referenceSymbol, String reportFileName) throws Exception {
        java.nio.file.Path root = Paths.get(cfg.output.dir);
        java.nio.file.Path report = root.resolve(reportFileName);
        new TimeseriesCoverageValidator(root, referenceSymbol, report).run();
    }

    /**
     * Execute validation and write CSV report.
     */
    public void run() throws Exception {
        List<java.nio.file.Path> parquetFiles = listParquetFiles(rootDir);
        if (parquetFiles.isEmpty()) {
            throw new IllegalStateException("No price parquet files found under: " + rootDir +
                    " (expected .../date=YYYY-MM-DD/part-*.parquet)");
        }

        // 1) Build reference trading calendar (dates where referenceSymbol has a row)
        TreeSet<LocalDate> refDates = new TreeSet<>();
        scanReferenceDates(parquetFiles, referenceSymbol, refDates);

        String calendarReferenceUsed = referenceSymbol;
        String calendarSource = "explicit";

        if (refDates.isEmpty()) {
            Map<String, Long> top = sampleTopSymbols(parquetFiles, 50);
            System.out.println("[VALIDATE][WARN] referenceSymbol '" + referenceSymbol + "' not found.");
            System.out.println("[VALIDATE][DIAG] sample symbols (top): " + top.keySet());

            String fallback = top.keySet().stream().findFirst().orElse(null);
            if (fallback == null) {
                throw new IllegalStateException("No symbols found in dataset; cannot build trading calendar.");
            }

            System.out.println("[VALIDATE][WARN] using fallback referenceSymbol=" + fallback);
            calendarReferenceUsed = fallback;
            calendarSource = "fallback";

            refDates.clear();
            scanReferenceDates(parquetFiles, fallback, refDates);
            if (refDates.isEmpty()) {
                throw new IllegalStateException("Fallback reference symbol '" + fallback + "' produced no dates.");
            }
        }

        // Index reference dates
        List<LocalDate> refList = new ArrayList<>(refDates);
        Map<LocalDate, Integer> refIndex = new HashMap<>(refList.size() * 2);
        for (int i = 0; i < refList.size(); i++) {
            refIndex.put(refList.get(i), i);
        }

        // 2) Scan all symbols and compute coverage
        Map<String, SymbolAcc> acc = new HashMap<>();
        scanAllSymbols(parquetFiles, refIndex, refList, acc);

        // 3) Write CSV report
        writeReportCsv(acc, reportCsv, calendarReferenceUsed, calendarSource);

        System.out.println("[VALIDATE] referenceSymbol=" + referenceSymbol
                + " calendarReferenceUsed=" + calendarReferenceUsed
                + " calendarSource=" + calendarSource
                + " refDays=" + refList.size());
        System.out.println("[VALIDATE] report=" + reportCsv.toAbsolutePath());
        System.out.println("[VALIDATE] symbols=" + acc.size());
    }

    // ---------------------------------------------------------------------
    // File selection (IMPORTANT: only price-parquets under date= partitions)
    // ---------------------------------------------------------------------

    private static List<java.nio.file.Path> listParquetFiles(java.nio.file.Path rootDir) throws IOException {
        if (!Files.exists(rootDir)) return List.of();

        List<java.nio.file.Path> files = new ArrayList<>();
        try (var stream = Files.walk(rootDir)) {
            stream.filter(p -> Files.isRegularFile(p) && p.getFileName().toString().endsWith(".parquet"))
                    .filter(TimeseriesCoverageValidator::isUnderDatePartition)
                    .forEach(files::add);
        }
        files.sort(Comparator.naturalOrder());
        return files;
    }

    private static boolean isUnderDatePartition(java.nio.file.Path parquetFile) {
        // expected: .../<root>/date=YYYY-MM-DD/part-*.parquet
        java.nio.file.Path parent = parquetFile.getParent();
        if (parent == null) return false;
        String parentName = parent.getFileName().toString();
        return parentName.startsWith("date=") && parentName.length() >= "date=YYYY-MM-DD".length();
    }

    // ---------------------------------------------------------------------
    // Parquet scanning (Hadoop Path only used locally for the reader)
    // ---------------------------------------------------------------------

    private static void scanReferenceDates(List<java.nio.file.Path> parquetFiles,
                                           String referenceSymbol,
                                           Set<LocalDate> outRefDates) throws Exception {
        Configuration conf = new Configuration();

        for (java.nio.file.Path f : parquetFiles) {
            org.apache.hadoop.fs.Path hPath = new org.apache.hadoop.fs.Path(f.toUri());
            try (ParquetReader<GenericRecord> reader =
                         AvroParquetReader.<GenericRecord>builder(hPath).withConf(conf).build()) {

                GenericRecord rec;
                while ((rec = reader.read()) != null) {
                    String symbol = asString(rec.get("symbol"));
                    if (!referenceSymbol.equals(symbol)) continue;

                    LocalDate date = LocalDate.parse(asString(rec.get("date")));
                    outRefDates.add(date);
                }

            } catch (SchemaParseException spe) {
                // If a wrong parquet slips into date= partitions, skip gracefully
                System.out.println("[VALIDATE][SKIP] schema parse failed file=" + f + " err=" + spe.getMessage());
            } catch (Exception ex) {
                System.out.println("[VALIDATE][SKIP] read failed file=" + f + " err=" + ex.getMessage());
            }
        }
    }

    private static void scanAllSymbols(List<java.nio.file.Path> parquetFiles,
                                       Map<LocalDate, Integer> refIndex,
                                       List<LocalDate> refList,
                                       Map<String, SymbolAcc> outAcc) throws Exception {
        Configuration conf = new Configuration();

        for (java.nio.file.Path f : parquetFiles) {
            org.apache.hadoop.fs.Path hPath = new org.apache.hadoop.fs.Path(f.toUri());
            try (ParquetReader<GenericRecord> reader =
                         AvroParquetReader.<GenericRecord>builder(hPath).withConf(conf).build()) {

                GenericRecord rec;
                while ((rec = reader.read()) != null) {
                    String symbol = asString(rec.get("symbol"));
                    LocalDate date = LocalDate.parse(asString(rec.get("date")));

                    SymbolAcc a = outAcc.computeIfAbsent(symbol, s -> new SymbolAcc(refList.size()));
                    a.totalRows++;

                    if (a.minDate == null || date.isBefore(a.minDate)) a.minDate = date;
                    if (a.maxDate == null || date.isAfter(a.maxDate)) a.maxDate = date;

                    Integer idx = refIndex.get(date);
                    if (idx == null) {
                        // This row is outside the reference calendar (should be rare in practice).
                        a.outOfCalendarRows++;
                        continue;
                    }

                    if (a.seen.get(idx)) {
                        a.duplicateRows++;
                    } else {
                        a.seen.set(idx);
                        a.actualDaysInCalendar++;
                    }
                }

            } catch (SchemaParseException spe) {
                System.out.println("[VALIDATE][SKIP] schema parse failed file=" + f + " err=" + spe.getMessage());
            } catch (Exception ex) {
                System.out.println("[VALIDATE][SKIP] read failed file=" + f + " err=" + ex.getMessage());
            }
        }

        // Range-based metrics: expected days within observed (min..max) intersection with ref calendar
        for (SymbolAcc a : outAcc.values()) {
            if (a.minDate == null || a.maxDate == null) continue;

            int startIdx = lowerBound(refList, a.minDate);
            int endIdx = upperBound(refList, a.maxDate) - 1;

            if (startIdx < 0) startIdx = 0;
            if (endIdx >= refList.size()) endIdx = refList.size() - 1;

            if (startIdx > endIdx) {
                a.expectedDaysInRange = 0;
                a.actualDaysInRange = 0;
                a.missingDaysInRange = 0;
                continue;
            }

            a.expectedDaysInRange = (endIdx - startIdx + 1);
            a.actualDaysInRange = a.seen.get(startIdx, endIdx + 1).cardinality();
            a.missingDaysInRange = Math.max(0, a.expectedDaysInRange - a.actualDaysInRange);
        }
    }

    // ---------------------------------------------------------------------
    // CSV output
    // ---------------------------------------------------------------------

    private static void writeReportCsv(Map<String, SymbolAcc> acc,
                                       java.nio.file.Path reportCsv,
                                       String calendarReferenceUsed,
                                       String calendarSource) throws IOException {
        Files.createDirectories(reportCsv.getParent());

        List<String> symbols = new ArrayList<>(acc.keySet());
        symbols.sort(String::compareTo);

        try (BufferedWriter w = Files.newBufferedWriter(
                reportCsv,
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING
        )) {
            w.write(String.join(",",
                    "calendar_reference_used",
                    "calendar_source",
                    "symbol",
                    "first_date",
                    "last_date",
                    "expected_days_in_range",
                    "actual_days_in_range",
                    "missing_days_in_range",
                    "coverage_pct_in_range",
                    "actual_days_in_calendar",
                    "duplicate_rows",
                    "out_of_calendar_rows",
                    "total_rows"
            ));
            w.newLine();

            for (String s : symbols) {
                SymbolAcc a = acc.get(s);

                String first = a.minDate != null ? a.minDate.toString() : "";
                String last = a.maxDate != null ? a.maxDate.toString() : "";

                double cov = (a.expectedDaysInRange > 0)
                        ? (100.0 * a.actualDaysInRange / (double) a.expectedDaysInRange)
                        : 0.0;

                w.write(csv(calendarReferenceUsed)); w.write(",");
                w.write(csv(calendarSource)); w.write(",");
                w.write(csv(s)); w.write(",");
                w.write(csv(first)); w.write(",");
                w.write(csv(last)); w.write(",");
                w.write(Integer.toString(a.expectedDaysInRange)); w.write(",");
                w.write(Integer.toString(a.actualDaysInRange)); w.write(",");
                w.write(Integer.toString(a.missingDaysInRange)); w.write(",");
                w.write(String.format(Locale.ROOT, "%.4f", cov)); w.write(",");
                w.write(Integer.toString(a.actualDaysInCalendar)); w.write(",");
                w.write(Long.toString(a.duplicateRows)); w.write(",");
                w.write(Long.toString(a.outOfCalendarRows)); w.write(",");
                w.write(Long.toString(a.totalRows));
                w.newLine();
            }
        }
    }

    // ---------------------------------------------------------------------
    // Diagnostics: sample symbols (for fallback selection)
    // ---------------------------------------------------------------------

    private static Map<String, Long> sampleTopSymbols(List<java.nio.file.Path> parquetFiles, int limit) throws Exception {
        Configuration conf = new Configuration();
        Map<String, Long> counts = new HashMap<>();

        int filesScanned = 0;
        for (java.nio.file.Path f : parquetFiles) {
            org.apache.hadoop.fs.Path hPath = new org.apache.hadoop.fs.Path(f.toUri());
            try (ParquetReader<GenericRecord> reader =
                         AvroParquetReader.<GenericRecord>builder(hPath).withConf(conf).build()) {

                GenericRecord rec;
                while ((rec = reader.read()) != null) {
                    String sym = asString(rec.get("symbol"));
                    if (sym != null && !sym.isEmpty()) {
                        counts.merge(sym, 1L, Long::sum);
                    }
                    if (counts.size() >= limit) return sortByValueDesc(counts);
                }
            } catch (Exception ignored) {
                // ignore in sampling
            }

            filesScanned++;
            if (filesScanned >= 10 && !counts.isEmpty()) break;
        }

        return sortByValueDesc(counts);
    }

    private static Map<String, Long> sortByValueDesc(Map<String, Long> m) {
        return m.entrySet().stream()
                .sorted((a, b) -> Long.compare(b.getValue(), a.getValue()))
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (x, y) -> x,
                        LinkedHashMap::new
                ));
    }

    // ---------------------------------------------------------------------
    // Utils
    // ---------------------------------------------------------------------

    private static int lowerBound(List<LocalDate> refList, LocalDate key) {
        int lo = 0, hi = refList.size();
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (refList.get(mid).isBefore(key)) lo = mid + 1;
            else hi = mid;
        }
        return lo;
    }

    private static int upperBound(List<LocalDate> refList, LocalDate key) {
        int lo = 0, hi = refList.size();
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (refList.get(mid).isAfter(key)) hi = mid;
            else lo = mid + 1;
        }
        return lo;
    }

    private static String asString(Object v) {
        return v == null ? "" : v.toString();
    }

    private static String csv(String s) {
        if (s == null) return "";
        boolean needsQuotes = s.contains(",") || s.contains("\"") || s.contains("\n") || s.contains("\r");
        if (!needsQuotes) return s;
        return "\"" + s.replace("\"", "\"\"") + "\"";
    }

    // ---------------------------------------------------------------------
    // Accumulator
    // ---------------------------------------------------------------------

    private static final class SymbolAcc {
        final BitSet seen;

        LocalDate minDate;
        LocalDate maxDate;

        long totalRows = 0;
        long duplicateRows = 0;
        long outOfCalendarRows = 0;

        int actualDaysInCalendar = 0;

        int expectedDaysInRange = 0;
        int actualDaysInRange = 0;
        int missingDaysInRange = 0;

        SymbolAcc(int refCalendarSize) {
            this.seen = new BitSet(refCalendarSize);
        }
    }
}
