-- Source table for the CDC leg: an operational "watchlist" that analysts edit.
-- Debezium streams every change; Spark folds them into an SCD2 dimension in Iceberg,
-- which the activity marts join to answer "how active are OUR tier-1 repos".
CREATE TABLE IF NOT EXISTS public.repo_watchlist (
    repo_name   text PRIMARY KEY,
    tier        text NOT NULL CHECK (tier IN ('tier1', 'tier2', 'tier3')),
    owner_team  text NOT NULL,
    notes       text,
    updated_at  timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE public.repo_watchlist REPLICA IDENTITY FULL;   -- Debezium gets `before` on updates/deletes

INSERT INTO public.repo_watchlist (repo_name, tier, owner_team, notes) VALUES
    ('apache/iceberg',        'tier1', 'platform',  'table format we run on'),
    ('apache/spark',          'tier1', 'platform',  'stream processor'),
    ('redpanda-data/redpanda','tier1', 'platform',  'broker'),
    ('debezium/debezium',     'tier2', 'platform',  'cdc'),
    ('duckdb/duckdb',         'tier2', 'analytics', 'serving engine'),
    ('dbt-labs/dbt-core',     'tier2', 'analytics', NULL),
    ('apache/airflow',        'tier3', 'orchestration', NULL),
    ('dagster-io/dagster',    'tier3', 'orchestration', NULL),
    ('prometheus/prometheus', 'tier3', 'observability', NULL),
    ('grafana/grafana',       'tier3', 'observability', NULL)
ON CONFLICT DO NOTHING;
