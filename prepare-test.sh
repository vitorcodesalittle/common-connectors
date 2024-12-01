#git clone https://github.com/morenoh149/postgresDBSamples.git
export PGPASSWORD=postgres
export PGUSER=postgres
export PGHOST=localhost
cd ./postgresDBSamples/adventureworks || exit 1

#psql -c "DROP DATABASE \"Adventureworks\";" || echo 'no database'
#psql -c "CREATE DATABASE \"Adventureworks\";"
#psql -d Adventureworks < install.sql
psql -c "DROP DATABASE \"Adventureworks2\";" || echo 'no database'
psql -c "CREATE DATABASE \"Adventureworks2\";"
psql -d Adventureworks2 < install.sql
psql -d Adventureworks2 -c "TRUNCATE TABLE sales.salesperson CASCADE"