BEGIN
 UNLOGGED
batch
-- this is a single line comment -- /* */ ; '' "" $$
// this is a single line comment -- /* */ ; '' "" $$
INSERT INTO t1 (pk, begin_batch) VALUES (1, 'a');
INSERT INTO t1 (pk, end_batch) VALUES (1, 'a');
end BATCH


;
--
//
/**/

------ ------ this is a single line comment -- /* */ ; '' "" $$------
////// ////// this is a single line comment -- /* */ ; '' "" $$//////

/*
This is a multi line comment  -- /* ; '' "" $$
*/

/* This is a multi line comment in one single line -- /* ; $$ '' ""*/INSERT INTO t1 (pk,
"""""this is a "" quoted identifier""""",
" ""this is a "" quoted identifier"" ",
c1, c2, c3, c4) VALUES (0,
'''''this is a string '' literal''''',
' ''this is a string '' literal'' ', '
''
this is a string literal '' spanning multiple lines
''
',
$$
This is a string literal spanning multiple lines and surrounded by dollar signs
$$,
'',-- this was an empty string literal
$$$$/* this was an empty string literal*/
)

;

INSERT INTO t1 (pk, c1) VALUES (1, 'a');INSERT INTO t1 (pk, c1) VALUES (2, 'b')
;INSERT INTO t1 (pk, c1) VALUES (3, 'c');

-- this is a single line comment -- /* */ ; '' "" $$
// this is a single line comment -- /* */ ; '' "" $$

/*
This is a multi line comment  -- /* ; '' "" $$
*/



