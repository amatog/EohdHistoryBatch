package com.acme.eod;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.nio.file.Files;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

public class ParquetStore {

    /** Canonical schema for price bars used across Primary and BySymbol stores. */
    public static final Schema PRICE_SCHEMA = SchemaBuilder.record("PriceBar").namespace("com.acme.eod")
            .fields()
            .requiredString("date")                 // ISO yyyy-MM-dd
            .requiredString("symbol")
            .requiredDouble("open")
            .requiredDouble("high")
            .requiredDouble("low")
            .requiredDouble("close")
            .requiredDouble("adjusted_close")
            .requiredLong("volume")
            .optionalString("source")
            .optionalLong("ingested_at_epoch_ms")
            .endRecord();

    private final AppConfig cfg;

    public ParquetStore(AppConfig cfg) {
        this.cfg = Objects.requireNonNull(cfg, "cfg");
    }

    /** Primary store root: <output.dir>/<output.primaryDir> */
    public java.nio.file.Path primaryRoot() {
        return java.nio.file.Path.of(cfg.output.dir, cfg.output.primaryDir);
    }

    /** Writes date-partitioned store: <root>/date=YYYY-MM-DD/part-0000.parquet */
    public void writePartitioned(List<PriceRow> rows) throws IOException {
        if (rows == null || rows.isEmpty()) {
            System.out.println("[STORE] no rows, nothing to write.");
            return;
        }

        Map<String, List<PriceRow>> byDate = rows.stream()
                .collect(Collectors.groupingBy(r -> r.date.toString()));

        for (Map.Entry<String, List<PriceRow>> e : byDate.entrySet()) {
            String date = e.getKey();
            List<PriceRow> dayRows = e.getValue();

            java.nio.file.Path outDir = primaryRoot().resolve("date=" + date);
            Files.createDirectories(outDir);

            java.nio.file.Path outFile = outDir.resolve("part-0000.parquet");

            System.out.println("[STORE] date=" + date + " rows=" + dayRows.size()
                    + " -> " + outFile.toAbsolutePath());

            try (java.nio.channels.FileChannel channel = java.nio.channels.FileChannel.open(
                    outDir.resolve(".ingest.lock"), java.nio.file.StandardOpenOption.CREATE,
                    java.nio.file.StandardOpenOption.WRITE);
                 java.nio.channels.FileLock lock = channel.tryLock()) {
                if (lock == null) throw new IOException("Partition is being written: " + date);
                // Merge by symbol so retrying a subset does not discard existing bars.
                Map<String, PriceRow> merged = new TreeMap<>();
                for (PriceRow row : readParquet(outFile)) merged.put(row.symbol, row);
                for (PriceRow row : dayRows) merged.put(row.symbol, row);
                writeParquet(outFile, new ArrayList<>(merged.values()));
            }
        }
    }

    public void writeParquet(java.nio.file.Path file, List<PriceRow> rows) throws IOException {
        Objects.requireNonNull(file, "file");
        Objects.requireNonNull(rows, "rows");

        Files.createDirectories(file.getParent());

        CompressionCodecName codec = parseCodec(cfg.output.compression);

        Configuration hadoopConf = new Configuration();
        java.nio.file.Path temporary = file.resolveSibling("." + file.getFileName() + "." + UUID.randomUUID() + ".tmp");

        try {
        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(
                new org.apache.parquet.io.LocalOutputFile(temporary))
                .withSchema(PRICE_SCHEMA)
                .withCompressionCodec(codec)
                .withConf(hadoopConf)
                .build()) {

            long nowMs = System.currentTimeMillis();
            for (PriceRow r : rows) {
                GenericRecord gr = new GenericData.Record(PRICE_SCHEMA);
                gr.put("date", r.date.toString());
                gr.put("symbol", r.symbol);
                gr.put("open", r.open);
                gr.put("high", r.high);
                gr.put("low", r.low);
                gr.put("close", r.close);
                gr.put("adjusted_close", r.adjustedClose);
                gr.put("volume", r.volume);
                gr.put("source", r.source != null ? r.source : "EODHD");
                gr.put("ingested_at_epoch_ms", r.ingestedAt != null ? r.ingestedAt.toEpochMilli() : nowMs);
                writer.write(gr);
            }
        }
        Files.move(temporary, file, java.nio.file.StandardCopyOption.ATOMIC_MOVE,
                java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        } finally {
            Files.deleteIfExists(temporary);
        }
    }

    private List<PriceRow> readParquet(java.nio.file.Path file) throws IOException {
        List<PriceRow> rows = new ArrayList<>();
        if (!Files.exists(file)) return rows;
        try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(
                new org.apache.parquet.io.LocalInputFile(file)).withConf(new Configuration()).build()) {
            GenericRecord record;
            while ((record = reader.read()) != null) {
                PriceRow row = new PriceRow();
                row.date = java.time.LocalDate.parse(record.get("date").toString());
                row.symbol = record.get("symbol").toString();
                row.open = ((Number) record.get("open")).doubleValue();
                row.high = ((Number) record.get("high")).doubleValue();
                row.low = ((Number) record.get("low")).doubleValue();
                row.close = ((Number) record.get("close")).doubleValue();
                row.adjustedClose = ((Number) record.get("adjusted_close")).doubleValue();
                row.volume = ((Number) record.get("volume")).longValue();
                row.source = record.get("source") == null ? "EODHD" : record.get("source").toString();
                rows.add(row);
            }
        }
        return rows;
    }

    public static CompressionCodecName parseCodec(String s) {
        if (s == null) return CompressionCodecName.SNAPPY;
        String x = s.trim().toUpperCase(Locale.ROOT);
        return switch (x) {
            case "GZIP" -> CompressionCodecName.GZIP;
            case "ZSTD" -> CompressionCodecName.ZSTD;
            case "UNCOMPRESSED" -> CompressionCodecName.UNCOMPRESSED;
            default -> CompressionCodecName.SNAPPY;
        };
    }
}
