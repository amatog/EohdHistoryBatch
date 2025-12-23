package com.acme.eod;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
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

            writeParquet(outFile, dayRows);
        }
    }

    public void writeParquet(java.nio.file.Path file, List<PriceRow> rows) throws IOException {
        Objects.requireNonNull(file, "file");
        Objects.requireNonNull(rows, "rows");

        Files.createDirectories(file.getParent());

        CompressionCodecName codec = parseCodec(cfg.output.compression);

        Configuration hadoopConf = new Configuration();
        Path hadoopPath = new Path(file.toUri());

        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(hadoopPath)
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
