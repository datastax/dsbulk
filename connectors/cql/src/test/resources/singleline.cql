BEGIN UNLOGGED batch INSERT INTO t1 (pk, c1) VALUES (1, 'a'); INSERT INTO t1 (pk, c1) VALUES (1, 'a'); apply BATCH;
-- this is a single line comment -- /* */ ; '' "" $$
// this is a single line comment -- /* */ ; '' "" $$

INSERT INTO t1 (pk,"""""this is a "" quoted identifier"""""," ""this is a "" quoted identifier"" ",c1, c2) VALUES (0,'''''this is a string '' literal''''',' ''this is a string '' literal'' ');

INSERT INTO t1 (pk, c1) VALUES (1, 'a');
INSERT INTO t1 (pk, c1) VALUES (2, 'b');
INSERT INTO t1 (pk, c1) VALUES (3, 'c');

-- this is a single line comment -- /* */ ; '' "" $$
// this is a single line comment -- /* */ ; '' "" $$




