# Konfiguration des „Clusters“

Die Installation des Hadoop-Clusters erfolgte im pseudoverteilten Modus, d.h.
alle in einem echten Cluster laufenden Dienste werden lokal auf einem einzigen
Rechner ausgeführt. Dabei muss vor allem auf den Speicherverbrauch der
Dienste geachtet werden und diese entsprechend konfiguriert werden. Diese
Anleitung geht von 6 GiB RAM aus.

## Verwendete Software

Bei der Auswahl der Versionen der Software muss auf die Kompatibilität geachtet
werden. So [benötigt Kylin][kylin-version] 1.2 die alten Versionen Hive 0.14
und HBase 0.98, während die aktuelle Hadoop-Version verwendet werden kann.
Unsere Installation verwendet die folgenden Versionen:

* [Kylin 1.2][kylin]
* [Hadoop 2.7.1][hadoop]
* [HBase 0.98.15][hbase]
* [Hive 0.14.0][hive]
* [Flink 0.10.1][flink]
* Java 1.7.95 (openjdk-7-jre-headless)
* Snappy 1.1.0 (libsnappy1)
* MySQL 5.5.47 (mysql-server)

Der Cluster wurde auf einem Rechner mit Ubuntu 14.04 installiert, wobei das
enthaltene OpenJDK als Java-Implementierung verwendet wurde. Außerdem wurden
noch Snappy und MySQL (als Hive-Metastore) aus den Ubuntu-Quellen installiert.

Hadoop, HBase, Hive, Kylin und Flink wurden dagegen nicht aus den Paket-Quellen,
sondern lokal in das home-Verzeichnis eines einfachen Benutzers installiert
(d.h. entpackt). Dabei wurde die folgende Verzeichnisstruktur verwendet:

```
~/node1/
├── lib/
│   ├── flink/
│   ├── hadoop/
│   ├── hbase/
│   ├── hive/
│   └── kylin/
├── hdfs/ (Enthält die im HDFS gespeicherten Daten)
└── zookeeper/ (Konfigurations- und Zustandsdateien)
```

Nach der Konfiguration sind die Benutzeroberflächen der einzelnen Dienst über
folgendende Adressen erreichbar:

* HDFS: http://localhost:50070
* Yarn: http://localhost:8088/
* Hbase: http://localhost:60010/
* Kylin: http://localhost:7070/kylin (ADMIN/KYLIN)
* Flink: über Yarn-Interface

Da die meisten Dienst standardmäßig auch auf den öffentlichen IP-Adressen
lauschen, wir die Verwendung einer Firewall empfohlen (z.B. ufw unter Ubuntu).
Der Zugriff auf die Weboberflächen kann dann z.B. über SSH-Portweiterleitungen
erfolgen.

## Umgebungsvariablen:

Die folgenden Umgebungsvariablen wurden in der `.bashrc` des einfachen
Benutzers (im Beispiel *bigprak*) gesetzt:

```sh
export JAVA_HOME=/usr/lib/jvm/java-7-openjdk-amd64
export HADOOP_PREFIX=/home/bigprak/node1/lib/hadoop
export HADOOP_HOME=/home/bigprak/node1/lib/hadoop
export HBASE_HOME=/home/bigprak/node1/lib/hbase
export HIVE_HOME=/home/bigprak/node1/lib/hive
export KYLIN_HOME=/home/bigprak/node1/lib/kylin
export CLASSPATH=/usr/share/java/mysql-connector-java.jar
```

## Start- und Stoppskripte

Der Cluster sollte mit Hilfe der Startskripte in folgender Reihenfolge
gestartet werden. Das Beenden erfolgt dann analog in umgekehrter Reihenfolge.

```
~/node1/lib/hadoop/sbin/start-dfs.sh -> NN + DD + 2ndNN
~/node1/lib/hadoop/sbin/start-yarn.sh -> RM + NM
~/node1/lib/hbase/bin/start-hbase.sh -> Master + Region + Zookeeper
~/node1/lib/kylin/bin/kylin.sh start|stop
```

Eine Hive-Shell kann über `~/node1/lib/hive/bin/hive` gestartet werden.

Für einen einfacheren Zugriff auf die Programme kann auch die
`PATH`-Umgebungsvariable in der `.bashrc` entsprechend ergänzt werden:

```sh
export PATH=$HADOOP_HOME/bin:$HBASE_HOME/bin:$HIVE_HOME/bin:$KYLIN_HOME/bin:$PATH
```

## Flink

Flink läuft direkt als Yarn-Job und hat keinen eigenen Dienst. Der Start
erfolgt über `~/node1/lib/flink/bin/yarn-session.sh -d -n 4`. Dabei werden ein
JobManager und vier TaskManager mit jeweils 1 GiB Speicher gestartet. Das dies
den Yarn-Cluster in unseren Fall bereits komplett auslastet, sollten, während
Flink gestartet ist, keine anderen Dienste Yarn-Jobs starten. Nach beendeter
Arbeit kann Flink über die Yarn-Weboberfläche (oder die Konsole) wieder beendet
werden.

## Konfiguration des Clusters

Im Folgenden sind die bei unserer Installation verwendeten Einstellungen für
die einzelnen Dienste, teilweise mit zusätzlichen Anmerkungen, aufgelistet.

Bei Betrieb aller Dienste eines Cluster auf einem Rechner ist der Standardwert
für die Anzahl geöffneter Dateien (ulimit -n; 1024) zu niedrig. Dieser sollte
für den einfachen Benutzer (normalerweise in `/etc/security/limits.conf`)
erhöht werden:

```
bigprak          -       nofile          32768
```

### Hadoop

Hadoop (und HBase) verwenden SSH zur Steuerung ihrer Knoten. Aus diesem Grund
muss man für den Benutzer, unter dem die Dienst laufen, ein passwortloses
SSH für localhost einrichten: Dazu erstellt man einen SSH-Schlüssel ohne
Passwort und fügt dessen öffentlichen Teil in die eigene `authorized_keys` ein.

* **hadoop/etc/hadoop/hdfs-site.xml** (http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/hdfs-default.xml)
  * `dfs.datanode.data.dir` = `file:///home/bigprak/node1/hdfs/datanode`
  * `dfs.namenode.name.dir` = `file:///home/bigprak/node1/hdfs/namenode`
  * `dfs.replication` = `1` (Keine Replikation)
* **hadoop/etc/hadoop/core-site.xml** (http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/core-default.xml)
  * `fs.defaultFS` = `hdfs://localhost/` (Adresse des NN)
* **hadoop/etc/hadoop/yarn-site.xml** (http://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-common/yarn-default.xml)
  * `yarn.nodemanager.aux-services` = `mapreduce_shuffle` (Aktivieren von MR)
  * `yarn.scheduler.minimum-allocation-mb` = `128` (Min Speicher pro Job)
  * `yarn.scheduler.maximum-allocation-mb` = `2048` (Max Speicher pro Job)
  * `yarn.scheduler.minimum-allocation-vcores` = `1` (Min Anzahl Kerne pro Job)
  * `yarn.scheduler.maximum-allocation-vcores` = `4` (Max Anzahl Kerne pro Job)
  * `yarn.nodemanager.resource.memory-mb` = `5120` (Max zu vergebender Speicher)
  * `yarn.nodemanager.resource.cpu-vcores` = `8` (Max Anzahl zu vergebender Kerne)
* **hadoop/etc/hadoop/mapred-site.xml** (http://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/mapred-default.xml)
  * `mapreduce.framework.name` = `yarn` (Benutze Yarn für MR)
  * `mapreduce.jobtracker.address` = `local` (Benutze den lokalen RM)
  * `mapreduce.map.memory.mb` = `1536`
  * `mapreduce.map.java.opts` = `-Xmx1024M` (Speicher für Unterprozesse erhöhen, die Kylin benötigt)
  * `mapreduce.reduce.memory.mb` = `2048`
  * `mapreduce.reduce.java.opts` = `-Xmx1536M`
* **hadoop/etc/hadoop/hadoop-env.sh**
  * `export JAVA_HOME=/usr/lib/jvm/java-7-openjdk-amd64` (In .bashrc reicht nicht)

### HBase

* **hbase/conf/hbase-site.xml** (http://hbase.apache.org/book.html#hbase_default_configurations)
  * `hbase.cluster.distributed` = `true` (Benutze HDFS, kein Standalone)
  * `hbase.rootdir` = `hdfs://localhost/hbase` (Pfad im HDFS zur Datenspeicherung)
  * `hbase.zookeeper.property.dataDir` = `/home/bigprak/node1/zookeeper` (Konfigurations- und Zustandsdateien des integrierten ZooKeeper)
  * `hbase.regionserver.codecs` = `snappy` (Nicht starten, falls Snappy nicht verfübar)
* **hbase/conf/hbase-env.sh**
  * `export JAVA_HOME=/usr/lib/jvm/java-7-openjdk-amd64/` (In .bashrc reicht nicht)
  * `export LD_LIBRARY_PATH=/home/bigprak/node1/lib/hadoop/lib/native` (Für Snappy)

### Hive

Standardmäßig verwendet Hive für seine Metadaten Derby. Allerdings erlaubt
Derby immer nur eine Verbindungs gleichzeitig, was dazu führt, dass immer nur
eine Hive-Instanz aktiv sein kann.

Aus diesem Grund haben wir MySQL für den Metastore verwendet, es wird jedoch
auch PostgreSQL unterstützt. Die Konfiguration ist ausführlich in einem
[Artikel von Cloudera][hive-meta] beschrieben, weshalb hier nicht näher darauf
eingegangen wird.

* **hive/conf/hive-site.xml**
  * `javax.jdo.option.ConnectionDriverName` = `com.mysql.jdbc.Driver`
  * `javax.jdo.option.ConnectionURL` = `jdbc:mysql://localhost/metastore`
  * `javax.jdo.option.ConnectionUserName` = `hive`
  * `javax.jdo.option.ConnectionPassword` = `hive`
  * `datanucleus.autoCreateSchema` = `false`
  * `datanucleus.fixedDatastore` = `true`
  * `hive.metastore.uris` = `""` (Metastore Host, leerer String bedeutet lokaler Modus)
  * `hive.auto.convert.join` = `false` (Workaround: Erzwinge MR für alle Joins, da alte Hive-Version Probleme bei der Berechnug des benötigten Speichers für einen in-memory-Join hat)

Damit Hive den passenden JDBC-Treiber findet, muss noch folgender symbolischer
Link angelegt werden:

```sh
ln -s /usr/share/java/mysql-connector-java.jar ~/node1/lib/hive/lib/
```

### Kylin

* **kylin/conf/kylin.properties**
  * `kylin.job.yarn.app.rest.check.status.url` = `http://localhost:8088/ws/v1/cluster/apps/${job_id}?anonymous=true` (Workaround für alte Hadoop-Version von HBase; Kylin 1.2 benötigt eigentlich >= 2.4)
  * `kylin.table.snapshot.max_mb` = `500` (DBLP-Titel-Tabelle ist größer als die voreingestellten 300 MiB)

## Weitere Dokumentation

Die folgenden Dokumentationen und Anleitungen haben sich bei der Installation
als nützlich erwiesen:

* https://hadoop.apache.org/docs/r2.7.1/hadoop-project-dist/hadoop-common/ClusterSetup.html
* http://www.alexjf.net/blog/distributed-systems/hadoop-yarn-installation-definitive-guide/
* https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html
* https://hbase.apache.org/book.html
* https://cwiki.apache.org/confluence/display/Hive/GettingStarted
* https://ci.apache.org/projects/flink/flink-docs-release-0.10/quickstart/setup_quickstart.html
* https://ci.apache.org/projects/flink/flink-docs-release-0.10/setup/yarn_setup.html

[kylin-version]: https://kylin.apache.org/docs/install/index.html
[kylin]: https://kylin.apache.org/download/
[hadoop]: https://hadoop.apache.org/releases.html
[hbase]: https://www-eu.apache.org/dist/hbase/
[hive]: https://archive.apache.org/dist/hive/hive-0.14.0/
[flink]: https://flink.apache.org/downloads.html
[hive-meta]: http://www.cloudera.com/documentation/archive/cdh/4-x/4-2-0/CDH4-Installation-Guide/cdh4ig_topic_18_4.html
