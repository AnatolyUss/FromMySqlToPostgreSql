-- Invalid copy data from mysql
DROP TABLE IF EXISTS dates;
CREATE TABLE dates (field DATE);
-- INSERT INTO dates VALUES ('0000-00-00');
-- INSERT INTO dates VALUES ('9999-99-99');
-- INSERT INTO dates VALUES ('2000-00-00');
INSERT INTO dates VALUES (NULL);
INSERT INTO dates VALUES ('2001-01-01');

-- unsigned ints
DROP TABLE IF EXISTS ints;
CREATE TABLE ints (field int unsigned);
INSERT INTO ints VALUES (0);
INSERT INTO ints VALUES (NULL);
INSERT INTO ints VALUES (4294967295);

DROP TABLE IF EXISTS bigints;
CREATE TABLE bigints (field bigint(50) unsigned);
INSERT INTO bigints VALUES (0);
INSERT INTO bigints VALUES (NULL);
INSERT INTO bigints VALUES (18446744073709551615);
-- Verify column comments
ALTER TABLE `bigints` CHANGE `field` `field` bigint(50) unsigned COMMENT 'column comment with \' in it.';
-- Check unique indexes are moved.
CREATE UNIQUE INDEX unique_index ON bigints (field);

-- Bits
DROP TABLE IF EXISTS bitfield;
CREATE TABLE bitfield (field bit default b'0');
INSERT INTO bitfield VALUES (b'0');
INSERT INTO bitfield VALUES (b'1');
INSERT INTO bitfield VALUES (NULL);

DROP TABLE IF EXISTS bitfield2;
CREATE TABLE bitfield2 (field bit(2) default b'01');
INSERT INTO bitfield2 VALUES (b'00');
INSERT INTO bitfield2 VALUES (b'01');
INSERT INTO bitfield2 VALUES (b'10');
INSERT INTO bitfield2 VALUES (b'11');
INSERT INTO bitfield2 VALUES (NULL);

-- Fun text
DROP TABLE IF EXISTS textfield;
CREATE TABLE textfield (field varchar(255));
INSERT INTO textfield VALUES ('"\',\\t\\n\\r');
INSERT INTO textfield VALUES ('interesting field data.');
INSERT INTO textfield VALUES ('interesting field data.\n');
INSERT INTO textfield VALUES ('interesting field data.\t');

-- Longblob
DROP TABLE IF EXISTS blobfield;
CREATE TABLE blobfield (field longblob);
INSERT INTO blobfield VALUES ('cat');
INSERT INTO blobfield VALUES (x'00');
INSERT INTO blobfield VALUES (x'08');
INSERT INTO blobfield VALUES (x'0A');
INSERT INTO blobfield VALUES (x'0D');
-- Verify table comments
ALTER TABLE blobfield COMMENT = 'This is test comment for confirming table comments are migrated with \' included.';

-- View with brackets
DROP VIEW IF EXISTS badview;
CREATE VIEW badview AS SELECT c.field FROM dates a JOIN dates b ON (a.field=b.field) JOIN dates c
ON (a.field=c.field);
