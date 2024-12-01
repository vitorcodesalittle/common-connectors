CREATE OR REPLACE FUNCTION truncate_all_tables()
RETURNS void AS $$
DECLARE
    rec RECORD;
BEGIN
    -- Loop through all user-defined schemas and their tables
    FOR rec IN
        SELECT
            table_schema, table_name
        FROM
            information_schema.tables
        WHERE
            table_type = 'BASE TABLE' AND
            table_schema NOT IN ('pg_catalog', 'information_schema')
    LOOP
        -- Dynamically construct and execute the TRUNCATE statement
        EXECUTE format('TRUNCATE TABLE %I.%I CASCADE', rec.table_schema, rec.table_name);
    END LOOP;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION drop_non_primary_constraints()
RETURNS void AS $$
DECLARE
    rec RECORD;
BEGIN
    -- Loop through all non-primary key constraints in user-defined schemas
    FOR rec IN
        SELECT
            table_schema,
            table_name,
            constraint_name
        FROM
            information_schema.table_constraints
        WHERE
            (constraint_type = 'FOREIGN KEY' OR
            constraint_type = 'UNIQUE') AND
            table_schema NOT IN ('pg_catalog', 'information_schema')
    LOOP
        -- Dynamically drop each constraint
        EXECUTE format('ALTER TABLE %I.%I DROP CONSTRAINT %I',
                       rec.table_schema, rec.table_name, rec.constraint_name);
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- SELECT drop_non_primary_constraints();
-- SELECT truncate_all_tables();