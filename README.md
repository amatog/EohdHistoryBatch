# EohdHistoryBatch (v5)

This version fixes:
- `FileOutputFormat` missing by including Hadoop MapReduce deps.
- Windows `HADOOP_HOME` / `winutils.exe` issue by providing a conventional folder `./hadoop/bin/winutils.exe`
  and auto-setting `hadoop.home.dir` if not provided.

## Windows requirement (local dev only)
Place `winutils.exe` at:
- `./hadoop/bin/winutils.exe`

Then:
- `mvn -DskipTests clean package`
- `java -jar target/eodhd-parquet-batch-1.0.0.jar --config ./config/application.yaml`

## Linux / javaprovider.net
No winutils.exe required.


1) Common options
   --config <path>

Optional. Defaults to ./config/application.yaml

Example:

--config ./config/application.yaml
--config C:\projects\EohdHistoryBatch\config\application.yaml

--mode <ingest|validate>

Optional. Defaults to ingest

--ref <SYMBOL>

Optional. Defaults to SPY

Used only when --mode validate

2) All valid command combinations
   A) Ingest (default mode)

Default ingest

java -jar target/eodhd-parquet-batch-1.0.0.jar


Explicit ingest

java -jar target/eodhd-parquet-batch-1.0.0.jar --mode ingest


Ingest with config

java -jar target/eodhd-parquet-batch-1.0.0.jar --config ./config/application.yaml


Ingest with config + explicit mode

java -jar target/eodhd-parquet-batch-1.0.0.jar --config ./config/application.yaml --mode ingest


Note: Passing --ref with ingest is syntactically accepted by your simple parser, but it is ignored (no effect). If you want strict behavior, we can reject --ref unless mode=validate.

B) Validate (coverage report)

Validate (defaults to ref=SPY)

java -jar target/eodhd-parquet-batch-1.0.0.jar --mode validate


Validate with explicit reference symbol

java -jar target/eodhd-parquet-batch-1.0.0.jar --mode validate --ref SPY


Validate with config (defaults ref=SPY)

java -jar target/eodhd-parquet-batch-1.0.0.jar --config ./config/application.yaml --mode validate


Validate with config + explicit reference symbol

java -jar target/eodhd-parquet-batch-1.0.0.jar --config ./config/application.yaml --mode validate --ref AAPL

3) Equivalent orderings (your parser behavior)

Your argument parsing is “scan tokens sequentially,” so order does not matter as long as each flag is followed by its value.

So these are equivalent:

--config ./config/application.yaml --mode validate --ref AAPL
--mode validate --ref AAPL --config ./config/application.yaml
--ref AAPL --mode validate --config ./config/application.yaml

4) Invalid combinations (will fail)

Unknown mode:

--mode foo


→ throws: Unknown --mode=foo (allowed: ingest, validate)

Missing value after a flag:

--config
--mode
--ref


→ value not set; likely leads to wrong defaults or a later failure.

5) Output locations
   Ingest

Writes Parquet under:

cfg.output.dir/date=YYYY-MM-DD/part-*.parquet

Validate

Writes CSV under:

cfg.output.dir/validation_coverage_report.csv