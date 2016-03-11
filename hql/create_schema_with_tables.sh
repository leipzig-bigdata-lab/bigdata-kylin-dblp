hive --hiveconf DB_SCHEMA=$1 -f create_schema.sql
hive --hiveconf DB_SCHEMA=$1 TYPE=$2 TARGET=$3 -f create_tables.sql