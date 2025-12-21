package com.acme.eod;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ParquetStore {

    private final AppConfig cfg;

    private final Schema schema = new Schema.Parser().parse("""
        {
          "type":"record",
          "name":"prices",
          "fields":[
            {"name":"date","type":"string"},
            {"name":"symbol","type":"string"},
            {"name":"open","type":"double"},
            {"name":"high","type":"double"},
            {"name":"low","type":"double"},
            {"name":"close","type":"double"},
            {"name":"adjusted_close","type":"double"},
            {"name":"volume","type":"long"},
            {"name":"source","type":"string"},
            {"name":"ingested_at_epoch_ms","type":"long"}
          ]
        }
        """);

    public ParquetStore(AppConfig cfg) {
        this.cfg = cfg;
    }

    public void writePartitioned(List<PriceRow> rows) throws Exception {
        if (rows == null || rows.isEmpty()) {
            System.out.println("[STORE] empty rows - skip");
            return;
        }

        Map<String, List<PriceRow>> byDate = rows.stream()
                .collect(Collectors.groupingBy(r -> r.date.toString()));

        for (Map.Entry<String, List<PriceRow>> e : byDate.entrySet()) {
            String date = e.getKey();
            List<PriceRow> part = e.getValue();

            java.nio.file.Path dir = java.nio.file.Path.of(cfg.output.dir, "date=" + date);
            java.nio.file.Path file = dir.resolve("part-0000.parquet");

            Files.createDirectories(dir);
            Files.deleteIfExists(file);

            writeFile(file, part);
            System.out.println("[STORE] date=" + date + " rows=" + part.size() + " -> " + file);
        }
    }

    private void writeFile(java.nio.file.Path file, List<PriceRow> rows) throws IOException {
        CompressionCodecName codec = "GZIP".equalsIgnoreCase(cfg.output.compression)
                ? CompressionCodecName.GZIP
                : CompressionCodecName.SNAPPY;

        Configuration hadoopConf = new Configuration();
        Path outPath = new Path(file.toUri());

        try (ParquetWriter<GenericRecord> writer =
                     AvroParquetWriter.<GenericRecord>builder(outPath)
                             .withSchema(schema)
                             .withCompressionCodec(codec)
                             .withConf(hadoopConf)
                             .build()) {

            for (PriceRow r : rows) {
                GenericRecord gr = new GenericData.Record(schema);
                gr.put("date", r.date.toString());
                gr.put("symbol", r.symbol);
                gr.put("open", r.open);
                gr.put("high", r.high);
                gr.put("low", r.low);
                gr.put("close", r.close);
                gr.put("adjusted_close", r.adjustedClose);
                gr.put("volume", r.volume);
                gr.put("source", r.source != null ? r.source : "EODHD");
                gr.put("ingested_at_epoch_ms", r.ingestedAt != null ? r.ingestedAt.toEpochMilli() : System.currentTimeMillis());
                writer.write(gr);
            }
        }
    }
}
