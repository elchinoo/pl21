-------------------- Initial setup
-- Disable psql pager
\pset pager off
\set PROMPT1 'pl21 [1] =# '

-- We need to have autovacuum OFF for this entire talk. NEVER do it in a production database
ALTER SYSTEM SET autovacuum = OFF;
SELECT pg_reload_conf();
SHOW autovacuum;

--
-- Create and populate the necessary tables to investigate how Postgres deals with its heap files
-- 
DROP TABLE IF EXISTS t_heap;
CREATE TABLE t_heap(a integer, b text NOT NULL, c DECIMAL(5,2));
INSERT INTO t_heap(a,b, c) SELECT i, ('Row # ' || i::text)::text, (random() * 999)::DECIMAL(5,2) FROM generate_series(1, 1000) AS i;
CHECKPOINT;

-- Install needed extensions
CREATE EXTENSION IF NOT EXISTS pg_freespacemap;
CREATE EXTENSION IF NOT EXISTS pageinspect;
CREATE EXTENSION IF NOT EXISTS pg_visibility;
--------------------

-------------------- Heap Table Files 
-- Let's get the physical location of our relation
SHOW data_directory;
SELECT oid, datname FROM pg_database WHERE datname = 'pl21';
SELECT relfilenode, relname FROM pg_class WHERE relname = 't_heap';

-- Inspect the physical file
-- We first go to the $DATADIR/base
cd /v01/projects/presentations/pl21/database/costs/pg/base
-- Then we access the database folder
cd 25553
-- We make sure the table heap file exists
ls -lha 26753
-- Then we use hexdump to inspect it
hexdump -C 26753 | less
-- As we will see later, there is a header in the beginning of each page
-- Then we have the data
-- End then the next page, and header, and data...

-- Each heap file (table) has an internal pseudo-column (RID or ctid) used to identify each row
-- We can select it
SELECT ctid, * FROM t_heap;
-- A Postgre RID (ctid) is a pair of (<page_number>, <row_slot>)
-- Remember that the page is a contiguous block of 8kB
SELECT ctid, * FROM t_heap WHERE ctid < '(1,0)';

--------------------
-------------------- Page Layout
-- -- -- -- -- -- -- -- -- -- -- -- -- -- 
-- Overall Page Layout
-- Item 	        Description
-- PageHeaderData 	24 bytes long. Contains general information about the page, including free space pointers.
-- ItemIdData 	    Array of item identifiers pointing to the actual items. Each entry is an (offset,length) pair. 4 bytes per item.
-- Free space 	    The unallocated space. New item identifiers are allocated from the start of this area, new items from the end.
-- Items 	        The actual items themselves.
-- Special space 	Index access method specific data. Different methods store different data. Empty in ordinary tables.
-- -- -- -- -- -- -- -- -- -- -- -- -- -- 
-- -- -- -- -- -- -- -- -- -- -- -- -- -- 
-- PageHeaderData Layout
-- Field 	            Type 	            Length 	    Description
-- pd_lsn 	            PageXLogRecPtr 	    8 bytes 	LSN: next byte after last byte of WAL record for last change to this page
-- pd_checksum 	        uint16 	            2 bytes 	Page checksum
-- pd_flags 	        uint16 	            2 bytes 	Flag bits
-- pd_lower 	        LocationIndex 	    2 bytes 	Offset to start of free space
-- pd_upper 	        LocationIndex 	    2 bytes 	Offset to end of free space
-- pd_special 	        LocationIndex 	    2 bytes 	Offset to start of special space
-- pd_pagesize_version 	uint16 	            2 bytes 	Page size and layout version number information
-- pd_prune_xid 	    TransactionId 	    4 bytes 	Oldest unpruned XMAX on page, or zero if none
-- -- -- -- -- -- -- -- -- -- -- -- -- -- 

SELECT ctid, * FROM t_heap WHERE ctid < '(1,0)';

-- We can use the extension pageinspect to inspect the pages from our heap file
-- The function get_raw_page gives a copy as a bytea value of the specified page 
SELECT get_raw_page('t_heap', 0);

-- We can use the function page_header to inspect the PageHeaderData
SELECT * FROM page_header(get_raw_page('t_heap', 0));
SELECT * FROM page_header(get_raw_page('t_heap', 5));

-- -- -- -- -- -- -- -- -- -- -- -- -- -- 
-- The most important fields for our discussion here are
-- lower: This is where our lower pointer is located, the last LINE POINTER where the free space starts
-- upper: This is where our upper pointer is located, the last ROW inside this page
-- pagesize: The pagesize, which will usually be 8kB
-- Note that if we subtract the lower pointer from the upper pointer we have the actual free space inside of the page

-- Let's check our pages #0 and #5 again and calculate how much free space each of them has:
-- Starting with page #0 we see that is has 28 bytes of free space which isn't enough to hold one of our average tuple size here
--      indicating this page is FULL
SELECT lsn, checksum, flags, lower, upper, (upper - lower) AS free_space, special, pagesize, version, prune_xid 
FROM page_header(get_raw_page('t_heap', 0));

-- The page #5 for the other hand will show us 4868 bytes of free space
SELECT lsn, checksum, flags, lower, upper, (upper - lower) AS free_space, special, pagesize, version, prune_xid 
FROM page_header(get_raw_page('t_heap', 5));


-------------------- 
-------------------- Table Row Layout
-- -- -- -- -- -- -- -- -- -- -- -- -- -- 
-- Overall Row Layout
-- Item 	            Description
-- HeapTupleHeaderData  The row header which has a fixed-size of 23 bytes 
-- NullBitmap           Option and the bitmap is NOT stored if t_infomask shows that there are no nulls in the tuple.
-- Alignment padding    (as needed to make user data MAXALIGN'd)
-- ObjectID             An optional OID that is stored if HEAP_HASOID_OLD is set in t_infomask, not created anymore
-- Payload              The user data fields
-- -- -- -- -- -- -- -- -- -- -- -- -- -- 

-- We can use the function heap_page_items() to inspect the page items
SELECT * FROM heap_page_items(get_raw_page('t_heap', 5)) limit 2;

-- -- -- -- -- -- -- -- -- -- -- -- -- -- 
-- HeapTupleHeaderData Layout
-- Field 	    Type 	            Length 	    Description
-- t_xmin 	    TransactionId 	    4 bytes 	insert XID stamp
-- t_xmax 	    TransactionId 	    4 bytes 	delete XID stamp
-- t_cid 	    CommandId 	        4 bytes 	insert and/or delete CID stamp (overlays with t_xvac)
-- t_xvac 	    TransactionId 	    4 bytes 	XID for VACUUM operation moving a row version
-- t_ctid 	    ItemPointerData 	6 bytes 	current TID of this or newer row version
-- t_infomask2 	uint16 	            2 bytes 	number of attributes, plus various flag bits
-- t_infomask 	uint16 	            2 bytes 	various flag bits
-- t_hoff 	    uint8 	            1 byte 	    offset to user data
-- -- -- -- -- -- -- -- -- -- -- -- -- -- 

-- Not all fields are of interest from our discussion here, then lets limit our SELECT to the fields of interest
SELECT lp, lp_off, lp_len, t_hoff, t_ctid, t_infomask::bit(16), t_infomask2
FROM heap_page_items(get_raw_page('t_heap', 0)) limit 5;

-- lp: Line pointer
-- lp_off: The offset to tuple from the beginning of page
-- lp_len: The size (lenght) of the tuple in bytes
SELECT lp, lp_off, lp_len FROM heap_page_items(get_raw_page('t_heap', 0)) LIMIT 3;

-- We can use hexdump again to chek what we have at inside the rows
-- Let's inspect the row (0, 5) 
SELECT lp, lp_off, lp_len, t_data FROM heap_page_items(get_raw_page('t_heap', 0)) WHERE lp in (71, 72);
 -- 71 |   4928 |     44 | \x4700000013526f7720232037310f00818501280a
 -- 72 |   4880 |     44 | \x4800000013526f7720232037320f0081ee028025

-- If we check the lp_off between those 2 tuples they don't match with the lp_len
-- (0,72) lp_off = 4880 with lp_len = 44
-- (0,71) lp_off = 4928
-- But 4928 - 4880 = 48, not 44. 
-- It happens because there is padding to make the tuple aligned with a 8 bytes word size
-- We'll discuss it later but we need to take it in consideration for hexdump
SHOW data_directory;
SELECT oid, datname FROM pg_database WHERE datname = 'pl21';
SELECT relfilenode, relname FROM pg_class WHERE relname = 't_heap';

cd /v01/projects/presentations/pl21/database/costs/pg/base/25553
hexdump -C -n 48 -s 4928 24713


-- t_infomask2: The number of attributes, plus various flag bits
--  | t_infomask2 
--  +-------------
--  |           2

-- t_infomask: Various flag bits, for example, the last attribute says if the row has any NULL value
--  |    t_infomask    
--  +------------------
--  | 0000100000000010 

-- t_xmin: The transaction ID that INSERTED the tuple
-- t_xmax: The transaction ID that REMOVED the tuple
SELECT lp, t_ctid, t_xmin, t_xmax FROM heap_page_items(get_raw_page('t_heap', 0)) LIMIT 3;

-- Say we have concurrent transactions
BEGIN;
SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;
SHOW TRANSACTION ISOLATION LEVEL;
SELECT * FROM t_heap LIMIT 3;
DELETE FROM t_heap WHERE a = 2;
SELECT * FROM t_heap LIMIT 3;
SELECT * FROM heap_page_items(get_raw_page('t_heap', 0)) LIMIT 3;
SELECT txid_current();

-- Let's take a closer look into those masks
SELECT lp, t_ctid, t_xmin, t_xmax,
    t_infomask::bit(16), t_infomask2::bit(16),
    NOT (t_infomask::bit(16) & b'0000100000000000')::int::bool AS "t_xmax VALID",
    (t_infomask::bit(16) & E'x0400')::int::bool AS "t_xmax COMMITED",
    (t_infomask2::bit(16) & E'x2000')::int::bool AS "DELETED"    
FROM heap_page_items(get_raw_page('t_heap', 0)) 
LIMIT 3;

-- What happens to the other transactions if we commit?
-- It highly depends on the TRANSACTION ISOLATION LEVEL
-- If anything LOWER than REPEATABLE READ it will reflect the changes, what we call dirt reads
-- If REPEATABLE READ or SERIALIZABLE isolation levels then the transaction will have a repeatable and clean view
COMMIT;
SELECT * FROM t_heap LIMIT 3;

-- Let's look again at those masks and check what has changed
SELECT lp, t_ctid, t_xmin, t_xmax,
    t_infomask::bit(16), t_infomask2::bit(16),
    NOT (t_infomask::bit(16) & b'0000100000000000')::int::bool AS "t_xmax VALID",
    (t_infomask::bit(16) & E'x0400')::int::bool AS "t_xmax COMMITED",
    (t_infomask2::bit(16) & E'x2000')::int::bool AS "DELETED"    
FROM heap_page_items(get_raw_page('t_heap', 0)) 
LIMIT 3;

-- t_ctid: The current TID (tuple identifier) of this tuple or a pointer to the newer row version

-- One of the features that take advantage of this "redundant" data is Heap Only Tuple (HOT) updates
-- Checking the table we see the index is pointing to the position
SELECT ctid, * FROM t_heap WHERE ctid = '(0, 50)';
SELECT lp, lp_off, lp_len, t_hoff, t_ctid, t_xmin, t_xmax FROM heap_page_items(get_raw_page('t_heap', 0)) WHERE lp = 50;

-- Let's vacuum te table before we start to make sure we clean up everything from our previous deletes
VACUUM t_heap;

-- Let's create an index
CREATE INDEX idx_t_heap_a ON t_heap(a);
SELECT * FROM bt_page_stats('idx_t_heap_a', 0);
SELECT * FROM bt_page_stats('idx_t_heap_a', 1);
-- The page 0 of the index file is a meta page, so we start from page 1
-- Assuming it is on the first page, #1, we can try to get the records pointing to our heap on position (0,3)
-- We shall remember that our index holds the PK, which is the column a = 3 ('03 00 00 00 00 00 00 00' in 64 bits representation)
SELECT * FROM bt_page_items('idx_t_heap_a', 1) WHERE data = '32 00 00 00 00 00 00 00';

-- We want to keep the PK value to be able to track the index
-- Then let's update any other column other than the PK
SELECT ctid, * FROM t_heap WHERE a = 50;
UPDATE t_heap SET c = random() WHERE a = 50 RETURNING ctid, *;

-- Checking for the CTID we can see that it was moved to another page because the page #0 was full
-- We are now on page #5 (5,76)
SELECT ctid, * FROM t_heap WHERE a = 50;

-- Checking the index we'll see now two values for the same key
-- One (dead tuple) pointing to the old position and another one pointing to the new position
-- The autovacuum will take care of it later and clean up our index but index visibility and dead tuples are out of the scope of this talk
SELECT * FROM bt_page_items('idx_t_heap_a', 1) WHERE data = '32 00 00 00 00 00 00 00';
SELECT * FROM heap_page_items(get_raw_page('t_heap', 0)) WHERE lp > 47 LIMIT 5;
SELECT * FROM heap_page_items(get_raw_page('t_heap', 0)) WHERE lp < 5;

-- Right but what about that ctid thing? 
-- Let's try again and change the a = 1
SELECT * FROM bt_page_items('idx_t_heap_a', 1) WHERE data = '01 00 00 00 00 00 00 00';
UPDATE t_heap SET c = random() WHERE a = 1 RETURNING ctid, *;
SELECT * FROM heap_page_items(get_raw_page('t_heap', 0)) WHERE lp < 4;
-- hmmm, it's now point to the end of the page, tuple (0,158) and if we check it
SELECT * FROM heap_page_items(get_raw_page('t_heap', 0)) WHERE lp = 158;
-- This means that the index doesn't need to be updated, less IO
-- The index points to the old Line Pointer and it will point to the correct value
-- If the tuple stays in the same page and the index value itself isn't updated, then there is no need to update the b-tree index
SELECT * FROM bt_page_items('idx_t_heap_a', 1) WHERE data = '01 00 00 00 00 00 00 00';

-- And if we take a look at it again, including those nice masks, this is what we can see
SELECT lp, lp_off, lp_len, t_hoff, t_ctid, t_xmin, t_xmax,
    (t_infomask::bit(16) & b'0010000000000000')::int::bool AS "Updated",
    (t_infomask2::bit(16) & b'0100000000000000')::int::bool AS "HOT updated",
    t_data
FROM heap_page_items(get_raw_page('t_heap', 0)) 
WHERE lp in (1, 158);

-- But one can think about waste of space, right? In the end the tuple is still there, including the data
-- Well, autovacuum can take care of it. Let's do a manual vacuum to check what is done
VACUUM t_heap;
SELECT lp, lp_off, lp_len, t_hoff, t_ctid, t_xmin, t_xmax,
    (t_infomask::bit(16) & b'0010000000000000')::int::bool AS "Updated",
    (t_infomask2::bit(16) & b'0100000000000000')::int::bool AS "HOT updated",
    t_data
FROM heap_page_items(get_raw_page('t_heap', 0)) 
WHERE lp in (1, 158);
-- Note that there is no information on the fields other than the lp and lp_off, 
-- This is because the ROW itself was cleaned and we are left with only the Line Pointer
-- This is an integer of 4 bytes pointing to the next LP in the chain
-- A small price to avoid extra IO updating the indexes ;)


-- t_hoff: The offset to user data in bytes, this tells us where we can find the beginning of data inside the row
--  | t_hoff 
--  +--------
--  |     24 


--------------------
-------------------- Data Alignment and Padding

SELECT a.attnum, a.attname, a.attlen, a.attstorage, a.attalign
FROM pg_attribute a
WHERE a.attrelid = 't_heap'::regclass AND a.attnum > 0;
-- attlen
--    integer: 4 bytes
--    varchar: unknown as it is of variable type
--    decimal: just like varchar it is also unknown as it is of variable type
-- attstorage:
--    p: Value must always be stored plain.
--    e: Value can be stored in a “secondary” relation (if relation has one, see pg_class.reltoastrelid).
--    m: Value can be stored compressed inline.
--    x: Value can be stored compressed inline or stored in “secondary” storage.

-- Let's create anothe table to better understand this padding thing
DROP TABLE IF EXISTS t_queue_item;
CREATE TABLE t_queue_item (
   item_type int2,
   q_id int8 not null,
   is_active boolean,
   q_item_id int8,
   q_item_value numeric(10,2),
   q_item_parent int8
);

-- Populate it with 1M random rows
INSERT INTO t_queue_item
   SELECT
       (random() * 125)::int,           -- item_type
       (random() * 99999)::int,         -- q_id
       ((random() * 999)::int % 2 = 0), -- is_active
       i,                               -- q_item_id
       (random() * 999)::int,           -- q_item_value
       (random() * 999)::int            -- q_item_parent
   FROM generate_series(1, 1000000) AS i;
CHECKPOINT;

-- And check the final table size
SELECT relname, pg_size_pretty(pg_relation_size(relname::TEXT)) as size
FROM pg_class
WHERE relname = 't_queue_item';

-- Let's take a look at the row types and if need padding
SELECT a.attname, t.typname, t.typalign, t.typlen
FROM pg_class c
    JOIN pg_attribute a ON (a.attrelid = c.oid)
    JOIN pg_type t ON (t.oid = a.atttypid)
WHERE c.relname = 't_queue_item'
    AND a.attnum >= 0
ORDER BY a.attnum;

-- All those typelen in different orders look fishy
-- Let's take a peak on the file itself
SELECT lp, lp_off, lp_len, t_ctid FROM heap_page_items(get_raw_page('t_queue_item', 0)) LIMIT 5;

-- Just looking at the lp_off and lp_len values we can't see anything wrong here
-- They look pretty good:
--      8120 - 72 = 8048
--      8048 - 72 = 7096 and so on!
-- I'm still not convinced, let's look at the file
-- We first force a checkpoint to make sure we have all data flushed to disk, just in case ;)
CHECKPOINT;

-- Let's check the data on the table itself but a smaller sample
SELECT ctid, * FROM t_queue_item WHERE ctid < '(0, 4)' ORDER BY 1;
/*
pl21=# SELECT ctid, * FROM t_queue_item LIMIT 3;
   ctid   | item_type | q_id  | is_active | q_item_id | q_item_value | q_item_parent 
----------+-----------+-------+-----------+-----------+--------------+---------------
 (0,1) |        75 |  5220 | t         |         1 |       261.60 |           400
 (0,2) |        57 | 94874 | t         |         2 |       679.34 |           338
 (0,3) |       121 | 14542 | t         |         3 |       187.42 |           120
 */

SHOW data_directory;
SELECT oid, datname FROM pg_database WHERE datname = 'pl21';
SELECT relfilenode, relname FROM pg_class WHERE relname = 't_queue_item';

hexdump -C -n 72 -s 8120 26738



-- What if we reorganize columns?
-- Let's start with the ones that are of 8 bytes:
--       q_id          | int8    | d        |      8
--       q_item_id     | int8    | d        |      8
--       q_item_parent | int8    | d        |      8
-- We then get the ones that might be multiple of 4 and/or 2
--       item_type     | int2    | s        |      2
-- Finally we are left with the ones of 1 byte and unknown size
-- We leave the ones with unknown size to the end of the table
--       is_active     | bool    | c        |      1
--       q_item_value  | numeric | i        |     -1
-- The final table is:
DROP TABLE IF EXISTS t_queue_item_good;
CREATE TABLE t_queue_item_good AS  
   SELECT q_id, 
      q_item_id, 
      q_item_parent, 
      item_type, 
      is_active, 
      q_item_value 
   FROM t_queue_item;

CHECKPOINT;
VACUUM t_queue_item;
VACUUM t_queue_item_good;

-- Time to check if any difference:
SELECT relname, pg_size_pretty(pg_relation_size(relname::TEXT)) as size
FROM pg_class 
WHERE relname like 't_queue_item%';

-- We got a gain of at least 20% in disk space, not bad for a table with only few columns!!!
-- All that space was waste in padding between the the columns

--------------------
-------------------- Free Space Map
-- Let's just recreate our table t_heap
DROP TABLE IF EXISTS t_heap;
CREATE TABLE t_heap(a integer, b text NOT NULL, c DECIMAL(5,2));
INSERT INTO t_heap(a, b, c) SELECT i, ('Row # ' || i::text)::text, (random() * 999)::DECIMAL(5,2) FROM generate_series(1, 1000) AS i;
VACUUM t_heap; CHECKPOINT;

-- Let's check what Postgres created
SELECT relfilenode, relname FROM pg_class WHERE relname = 't_heap';
ls -lha 26720*

-- Note that we have 2 files for that table, pay attention to the one with the suffix _fsm
-- This is this table's free space map
-- Let's use the pg_freespacemap we've installed before to inspect it

SELECT * FROM pg_freespace('t_heap');

-- Note that there are 5 pages with no empty space and the last one with some space available
-- What if we delete some rows?
DELETE from t_heap WHERE a BETWEEN 297 and 770; VACUUM t_heap;

SELECT * FROM pg_freespace('t_heap');
-- We made a lot of empty spaces inside the pages
-- Let's insert one record to see where Postgres will place it
INSERT INTO t_heap(a, b, c) VALUES (-11111, '-11111', '-111.11'); VACUUM t_heap;
SELECT * FROM pg_freespace('t_heap');

-- It inserted somewhere in the middle of the file, in the next available free space
-- We can even check to see it in the middle of our table
SELECT ctid, * FROM t_heap;

-- NOTE that there is no order at all inside a heaple table and 
-- The data can be stored at anywhere there is enough free for it to be stored

--------------------
-------------------- 
-- Here we come to the end!
-- I hope you've enjoyed the presentation
-- Have a great day !!
--------------------
-------------------- 
