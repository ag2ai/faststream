CREATE TABLE IF NOT EXISTS users (
    id       bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name     text NOT NULL,
    age      integer NOT NULL,
    fullname text NOT NULL
);


CREATE INDEX IF NOT EXISTS ix_users_name ON users (name);

TRUNCATE users RESTART IDENTITY;

INSERT INTO users (name, age, fullname) VALUES
    ('John', 39, repeat('LongString', 8)),
    ('Mike', 8,  repeat('LongString', 8));

INSERT INTO users (name, age, fullname)
SELECT
    'user_' || g,
    (g % 90) + 1,
    repeat('LongString', 8)
FROM generate_series(1, 10000) AS g;

ANALYZE users;
