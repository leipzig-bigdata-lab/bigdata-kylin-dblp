use ${hiveconf:DB_SCHEMA};

create external table timeinfo (
	time_id int,
	year int,
	month int,
	day int,
	decade int
)
row format delimited
fields terminated by "," escaped by "\\"
lines terminated by "\n"
stored as textfile
location '${hiveconf:TABLE_PATH}/time';

create external table venue_series (
	venue_series_id int,
	name string,
	dblp_key string
)
row format delimited
fields terminated by "," escaped by "\\"
lines terminated by "\n"
stored as textfile
location '${hiveconf:TABLE_PATH}/venue_series';

create external table title (
	title_id int,
	title string,
	ignore1 string
)
row format delimited
fields terminated by "," escaped by "\\"
lines terminated by "\n"
stored as textfile
location '${hiveconf:TABLE_PATH}/title';

create external table author (
	author_id int,
	name string
)
row format delimited
fields terminated by "," escaped by "\\"
lines terminated by "\n"
stored as textfile
location '${hiveconf:TABLE_PATH}/author';

create external table document_type (
	type_id int,
	name string,
	description string
)
row format delimited
fields terminated by "," escaped by "\\"
lines terminated by "\n"
stored as textfile
location '${hiveconf:TABLE_PATH}/document_type';

create external table publication (
	publication_id int,
	title_id int,
	type_id int,
	time_id int,
	venue_series_id int,
	citings_dblp int,
	ignore1 int,
	ignore2 int,
	ignore3 int,
	author_id int,
	ignore4 string
)
row format delimited
fields terminated by "," escaped by "\\"
lines terminated by "\n"
stored as textfile
location '${hiveconf:TABLE_PATH}/publication';
