#git clone https://github.com/morenoh149/postgresDBSamples.git
export PGPASSWORD=postgres
export PGUSER=postgres
export PGHOST=localhost

psql -c "CREATE DATABASE \"Adventureworks\";"
cd ./postgresDBSamples/adventureworks || exit 1
psql -d Adventureworks < install.sql


psql -c "CREATE DATABASE \"Adventureworks2\";"
psql -d Adventureworks2 < install.sql
psql -d Adventureworks2 -c "TRUNCATE TABLE sales.salesperson CASCADE"
