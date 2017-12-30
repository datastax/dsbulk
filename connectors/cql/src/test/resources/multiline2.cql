INSERT INTO t1 (pk,
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
'',$$$$,
$$  $  $$
)
