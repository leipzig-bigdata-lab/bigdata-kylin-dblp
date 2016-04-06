# Publikationsanalyse mit Kylin

Beitrag zum „Big-Data-Praktikum“ der Fakultät für Mathematik und Informatik
an der Universität Leipzig.

## Ziel

Das Ziel dieser Praktikumsaufgabe bestand in der Untersuchung der
Praxistauglichkeit von Apache Kylin für die Data-Warehouse-Analyse von großen
Datenmengen. Dazu sollte eine Zitierungs- bzw. Publikationsanalyse auf Basis der Daten der [dblp]
durchgeführt werden.

Details zum Ablauf und den Ergebnissen finden sich in der
[Lösungsskizze][solution] und der [Abschlusspräsentation][presentation]
des Praktikums. Der LaTeX-Quelltext befindet sich im Ordner `documents`.

## Software

* Kylin 1.2
* Hadoop 2.7.1
* HBase 0.98.15
* Hive 0.14.0
* Flink 0.10.1

## Installation des „Clusters“

Die verwendete Software wurde im pseudoverteilten Modus auf einem von der
Abteilung Datenbanken bereitgestellten Rechner mit einem Intel Xeon W3520
und 6 GiB RAM unter Verwendung von Ubuntu 14.04 installiert. Details zur
Installation und Konfiguration finden sich in der Datei [INSTALL.md][install].

## Werkzeuge

### pub-importer

Import der DBLP-Daten in Form der [`dblp.xml`][xml]-Datei in das lokale HDFS.
Die Ausgabe erfolgt dabei in zwei CSV-Dateien, eine für die Publikationen und
eine für Sammlungen wie Tagungsbände und Bücher.

```sh
cat "dblp.xml" | java -jar pub-importer.jar dblp --csv-format escaped "/path/in/hdfs"
```

### pub-formatter

Erzeugung des Sternschemas aus den vom `pub-importer` erzeugten CSV-Dateien
mit Hilfe von Flink. Die Ausgabe erfolgt in Hive-kompatiblen CSV-Dateien.

```sh
flink run pub-formatter.jar -type dblp -source "hdfs:////path/in/hdfs" -target "hdfs:////hive/dblp"
```

#### Definition externer Hive-Tabellen

Auf den vom `pub-formatter` erzeugten CSV-Dateien können direkt externe
Hive-Tabellen definiert werden. Dazu wurde ein [Skript][hqlscript] angelegt,
das eine neue Datenbank und die Tabellen des Sternschemas anlegt:

```sh
hql/create-star-schema "db-name" "/hive/dblp"
```

[solution]: ./documents/solution-outline.pdf
[presentation]: ./documents/presentation.pdf
[dblp]: http://dblp.uni-trier.de/
[install]: ./INSTALL.md
[xml]: http://dblp.uni-trier.de/xml/
[hqlscript]: ./hql/create-star-schema
