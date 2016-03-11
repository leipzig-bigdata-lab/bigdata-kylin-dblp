# This file contains information about the different PDI files in this directory #

### General: ###
A transformation (*.ktr) contains several steps. Steps transform data in different ways.  A job (*.kjb) contains one or more transformation an should always begin with a dummy object called "START" and end with a dummy object called "Success" (best way for noticing those events in log-files while debugging errors). The files in this directory are supposed to be combined/parallelized later on in one job. So it would be possible to handle the whole process (download, validate, import into hdfs, transform and load into hive) for dblp-data with pdi. If this is implemented it would also be possible to run this process automated every day or week.

### Files: ###
* **getData.ktr** (Transformation)
    * Updated version of "xmlToCsv_bak.ktr". Is not final and contains errors. Problem: MergeJoins shouldn't be used. Use cartesian join instead.
* **ImportData.kjb** (Job)
    * Job that contains "getData.ktr". It runs the transformation and could be parallelized with other transformation.
* **loadFilesIntoHive.kjb** (Job)
    * Copies files to hdfs, so hive can use the data for its tables.
* **loadXmlFiles.kjb** (Job)
    * Loads xml-source file into hdfs filesystem.
* **validateDblpXml.kjb** (Job)
    * Validates the source xml-file of dblp-data. It uses the "dblp.dtd" file provided by uni-trier. (See http://dblp.uni-trier.de/xml/)
* **xmlToCsv_bak.ktr** (Transformation)
    * Is the original transformation which generates the csv-files for the tables "author", "titles", "publication_author_map" and "publication". Some information in the publication.csv are still missing and need to be added. It works for the whole set of data but is incomplete.
