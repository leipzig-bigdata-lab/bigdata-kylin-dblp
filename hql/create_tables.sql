use ${hiveconf:DB_SCHEMA};

drop table if exists Timeinfo;
drop table if exists TimeInfo;
create external table if not exists timeinfo (
	time_id int,
	year int, 
	month int, 
	day int, 
	decade int)
row format delimited
fields terminated by "," escaped by "\\"
lines terminated by "\n"
stored as textfile
location '/${hiveconf:TARGET}/${hiveconf:TYPE}/time';

drop table if exists Venue_Series;
create external table if not exists Venue_Series (
	venue_series_id int,
	name String, 
	description String)
row format delimited
fields terminated by "," escaped by "\\"
lines terminated by "\n"
stored as textfile
location '/${hiveconf:TARGET}/${hiveconf:TYPE}/venue_series';

drop table if exists Title;
create external table if not exists Title (
	title_id int,
	title String)
row format delimited
fields terminated by "," escaped by "\\"
lines terminated by "\n"
stored as textfile
location '/${hiveconf:TARGET}/${hiveconf:TYPE}/title';

drop table if exists Author;
create external table if not exists Author (
	author_id int,
	name String,
	institution_id int)
row format delimited
fields terminated by "," escaped by "\\"
lines terminated by "\n"
stored as textfile
location '/${hiveconf:TARGET}/${hiveconf:TYPE}/author';

drop table if exists Document_Type;
create external table if not exists Document_Type (
	type_id int,
	name String, 
	description String)
row format delimited
fields terminated by "," escaped by "\\"
lines terminated by "\n"
stored as textfile
location '/${hiveconf:TARGET}/${hiveconf:TYPE}/document_type';

drop table if exists Publication;
create external table if not exists Publication (
	publication_id String,
	title_id int,
	type_id int,
	time_id int,
	venue_series_id int, 
	citings_dblp int, 
	citings_gs int, 
	citings_acm int, 
	citings_acm_self int)
row format delimited
fields terminated by "," escaped by "\\"
lines terminated by "\n"
stored as textfile
location '/${hiveconf:TARGET}/${hiveconf:TYPE}/publication';

use default;
