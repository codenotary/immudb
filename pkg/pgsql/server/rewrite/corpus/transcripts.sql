-- B1 parser-fidelity corpus.
--
-- One SQL statement per non-comment line. Lines starting with `--` are
-- ignored. The TestParserFidelity test parses each entry and records
-- the parse result; this gives us an objective measure of auxten's
-- coverage before we port additional regex rules in B2.
--
-- When adding a line, pick SQL that real clients actually send
-- (pgAdmin, psql backslash commands, Rails, XORM, Gitea, JDBC). Synthetic
-- SQL that only matters for unit tests should NOT land here — this
-- file is the production-traffic ground truth.

-- psql \d <table> round-trip one
SELECT c.oid, n.nspname, c.relname FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE c.relname = 'continent' ORDER BY 2, 3

-- psql \d <table> round-trip two
SELECT c.relchecks, c.relkind, c.relhasindex, c.relhasrules, c.relhastriggers, c.relrowsecurity, c.relforcerowsecurity FROM pg_catalog.pg_class c WHERE c.oid = 16384

-- psql \dt
SELECT n.nspname, c.relname FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE c.relkind = 'r' AND n.nspname = 'public' ORDER BY 1, 2

-- psql \l
SELECT d.datname as name, d.datallowconn FROM pg_catalog.pg_database d ORDER BY 1

-- psql \du
SELECT r.rolname, r.rolsuper, r.rolinherit FROM pg_catalog.pg_roles r ORDER BY 1

-- psql \dn
SELECT nspname FROM pg_catalog.pg_namespace ORDER BY nspname

-- psql \dT
SELECT typname FROM pg_catalog.pg_type ORDER BY typname

-- psql regex-pattern match (A2 rewrite normalises this before parse)
SELECT c.relname FROM pg_catalog.pg_class c WHERE c.relname = 'continent'

-- Rails pg_type load (JOIN pg_range)
SELECT t.oid, t.typname, t.typelem, t.typdelim FROM pg_type as t

-- Rails schema_migrations lookup
SELECT version FROM schema_migrations ORDER BY version ASC

-- Gitea table-existence probe
SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename = 'gitea_users'

-- XORM index enumeration
SELECT indexname, indexdef FROM pg_indexes WHERE tablename = 't'

-- information_schema.columns probe
SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name = 'users'

-- information_schema.tables probe
SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'

-- pg cast to regclass (common in psql)
SELECT c.oid::regclass FROM pg_class c

-- pg cast to text
SELECT 'bar'::text

-- COLLATE "default"
SELECT 'x' COLLATE "default"

-- Simple SELECT
SELECT 1

-- Simple FROM
SELECT * FROM users

-- WHERE + ORDER BY
SELECT id, name FROM users WHERE active = true ORDER BY id

-- INSERT
INSERT INTO users (id, name) VALUES (1, 'alice')

-- UPDATE
UPDATE users SET name = 'bob' WHERE id = 1

-- DELETE
DELETE FROM users WHERE id = 1

-- CREATE TABLE without immudb extensions (parses)
CREATE TABLE posts (id INTEGER, body TEXT)

-- XORM COUNT(1) aggregate
SELECT comment_id, COUNT(1) AS n FROM issue_content_history GROUP BY comment_id

-- Rails ActiveRecord projection
SELECT "users".* FROM "users" WHERE "users"."id" = 1

-- Rails ON CONFLICT upsert
INSERT INTO schema_migrations (version) VALUES ('20231201000000') ON CONFLICT (version) DO NOTHING

-- pg_dump CREATE TABLE with CHECK
CREATE TABLE products (id INTEGER, price INTEGER, CHECK (price >= 0))

-- Rails FK column-level
CREATE TABLE orders (id INTEGER, user_id INTEGER REFERENCES users(id))

-- pg_dump FK with ON DELETE
CREATE TABLE orders (id INTEGER, user_id INTEGER, FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE)

-- pg_dump CREATE INDEX with name
CREATE INDEX idx_users_email ON users (email)

-- pg_dump CREATE VIEW with column list
CREATE VIEW user_emails(user_id, email) AS SELECT id, email FROM users

-- CREATE TABLE with immudb VARCHAR[N] (does NOT parse — goes to regex chain)
CREATE TABLE posts (id INTEGER, body VARCHAR[128], PRIMARY KEY id)

-- CREATE TABLE with AUTO_INCREMENT (does NOT parse)
CREATE TABLE posts (id INTEGER AUTO_INCREMENT, PRIMARY KEY id)

-- COPY statement (immudb wire-level, does NOT parse via auxten)
COPY users (id, name) FROM stdin
