# Wiederholbare Speicherung

Ingestion ersetzt eine Datumspartition atomar nach erfolgreichem Schreiben einer temporären Datei. Vorhandene Werte werden nach Symbol zusammengeführt; ein erneuter Lauf für einzelne Symbole entfernt keine anderen Symbole. Gleichzeitige Schreibvorgänge auf dieselbe Partition werden durch Dateisperren abgewiesen. Ein einzelner Lauf soll den Primary-Store verwalten; andere Programme müssen die gleichen Sperren respektieren.

Materialisierung schreibt Dateien ebenfalls zunächst temporär und veröffentlicht sie atomar. Ein Checkpoint wird nach jedem abgeschlossenen Datum atomar ersetzt. Nach einem Abbruch kann ein Datum sicher erneut verarbeitet werden, auch wenn bereits einzelne Ausgabedateien existieren. INCREMENTAL verarbeitet weiterhin nur neue Daten; bei Korrekturen bereits verarbeiteter Primary-Daten einen FULL_REBUILD ausführen.

FULL_REBUILD baut einen vollständigen neuen Store in einem benachbarten `.staging-*`-Verzeichnis. Anschließend wird der bisherige Store nach `.backup-*` verschoben und der neue Store veröffentlicht. Fehler beim zweiten Schritt stellen den alten Store wieder her. Alte Backups werden nicht automatisch gelöscht. Datumsfilter sind bei FULL_REBUILD nicht zulässig. Primary- und Symbol-Store dürfen sich nicht überlappen.

Die beiden Verzeichnisumbenennungen sind zusammen keine atomare Operation. Leser können während des Wechsels kurz kein Zielverzeichnis sehen; den Neuaufbau in einem Wartungsfenster veröffentlichen. Nach einem harten Prozess-/Systemabbruch in dieser Lücke sind Backup und Staging erhalten; vor dem nächsten Lauf den vollständigen Store wieder unter dem Zielnamen bereitstellen. Atomare Dateiumbenennungen müssen vom Dateisystem unterstützt werden. Es gibt keinen stillen Fallback auf unvollständige Kopien.

Lokale Parquet-I/O für diese Schreib-/Materialisierungspfade verwendet Java-Dateizugriff statt Hadoop-Checksum-Dateien, damit beim atomaren Ersetzen keine veralteten `.crc`-Dateien gelesen werden.

Tests: `mvn test` (2 Tests einschließlich Wiederholung, Teiluniversum, verlorenem Checkpoint, Neuaufbau und überlappenden Pfaden).
