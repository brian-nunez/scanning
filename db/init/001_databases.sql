DO
$$
BEGIN
   IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'scanning') THEN
      CREATE ROLE scanning LOGIN PASSWORD 'scanning';
   END IF;

   IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'airflow') THEN
      CREATE ROLE airflow LOGIN PASSWORD 'airflow';
   END IF;
END
$$;

SELECT 'CREATE DATABASE scanning OWNER scanning'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'scanning')\gexec

SELECT 'CREATE DATABASE airflow OWNER airflow'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow')\gexec
