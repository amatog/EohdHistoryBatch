WINDOWS ONLY
===========

Hadoop's LocalFileSystem on Windows requires winutils.exe to handle file permission calls.

Place winutils.exe here:
  ./hadoop/bin/winutils.exe

Then run the app (either set env var or let Main.java set it automatically):
  setx HADOOP_HOME "%CD%\hadoop"
  # reopen terminal

Notes:
- winutils.exe must match your Hadoop major version (3.x). In practice, Hadoop 3.3.x winutils.exe works with 3.3.6 deps.
- On Linux (javaprovider.net) you do NOT need winutils.exe.
