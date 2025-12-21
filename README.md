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
