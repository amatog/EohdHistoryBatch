package com.acme.eod;

import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import java.nio.file.*;
import java.time.LocalDate;
import java.util.*;
import static org.junit.Assert.*;

public class StoreRecoveryTest {
    static {
        if (System.getProperty("os.name").toLowerCase().contains("win")) {
            System.setProperty("hadoop.home.dir", Path.of("hadoop").toAbsolutePath().toString());
        }
    }
    @Rule public TemporaryFolder temporary = new TemporaryFolder();
    private PriceRow row(String symbol, double close) {
        PriceRow row = new PriceRow();
        row.symbol = symbol; row.date = LocalDate.of(2025, 1, 2);
        row.open = row.high = row.low = row.close = row.adjustedClose = close;
        row.volume = 100;
        return row;
    }
    @Test public void retryAndRebuildProduceReadableStore() throws Exception {
        AppConfig config = new AppConfig();
        config.output.dir = temporary.getRoot().toString();
        config.output.compression = "UNCOMPRESSED";
        ParquetStore store = new ParquetStore(config);
        store.writePartitioned(List.of(row("AAPL", 100), row("MSFT", 200)));
        store.writePartitioned(List.of(row("AAPL", 101)));
        MaterializeSymbolStoreJob.Args args = new MaterializeSymbolStoreJob.Args();
        args.primaryRoot = store.primaryRoot();
        args.symbolRoot = temporary.getRoot().toPath().resolve("by_symbol");
        args.compression = "UNCOMPRESSED";
        MaterializeSymbolStoreJob job = new MaterializeSymbolStoreJob();
        job.run(args);
        Path msft = args.symbolRoot.resolve("symbol=MSFT/year=2025/part-2025-01-02.parquet");
        assertTrue(Files.exists(msft)); // subset ingestion must preserve MSFT
        // Simulate loss of checkpoint after successful file publication.
        Files.delete(args.symbolRoot.resolve("_state/materialize_state.json"));
        job.run(args);
        Files.writeString(args.symbolRoot.resolve("stale.txt"), "stale");
        args.mode = MaterializeSymbolStoreJob.Mode.FULL_REBUILD;
        job.run(args);
        assertTrue(Files.exists(msft));
        assertFalse(Files.exists(args.symbolRoot.resolve("stale.txt")));
    }
    @Test public void overlappingStoresAreRejected() throws Exception {
        MaterializeSymbolStoreJob.Args args = new MaterializeSymbolStoreJob.Args();
        args.primaryRoot = temporary.getRoot().toPath();
        args.symbolRoot = args.primaryRoot.resolve("nested");
        assertThrows(IllegalArgumentException.class, () -> new MaterializeSymbolStoreJob().run(args));
    }
}
