/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */


/*
 * This is an ANTLR4 grammar for DSBulk mappings (see schema.mapping setting and MappingInspector).
*/

grammar Mapping;

mapping
    : simpleEntry  ( ',' simpleEntry  )* EOF // col1, col2
    | indexedEntry ( ',' indexedEntry )* EOF // 0 = col1, 1 = col2, now() = col3
    | mappedEntry  ( ',' mappedEntry  )* EOF // fieldA = col1, fieldB = col2, now() = col3
    ;

simpleEntry
    : variableOrFunction
    ;

mappedEntry
    : regularMappedEntry
    | inferredMappedEntry
    ;

regularMappedEntry
    : fieldOrFunction ( ':' | '=' ) variableOrFunction
    ;

inferredMappedEntry
    : '*' ( ':' | '=' ) '*'
    | '*' ( ':' | '=' ) '-' variable
    | '*' ( ':' | '=' ) '[' '-' variable ( ',' '-' variable )* ']'
    ;

indexedEntry
    : indexOrFunction ( ':' | '=' ) variableOrFunction
    ;

indexOrFunction
    : index
    | function
    ;

index
    : INTEGER
    ;

fieldOrFunction
    : field
    | function
    ;

field
    : UNQUOTED_IDENTIFIER
    | QUOTED_IDENTIFIER
    ;

variableOrFunction
    : variable
    | function
    ;

variable
    : UNQUOTED_IDENTIFIER
    | QUOTED_IDENTIFIER
    ;

function
    : WRITETIME '(' functionArg ')'
    | qualifiedFunctionName '(' ')'
    | qualifiedFunctionName '(' functionArgs ')'
    ;

qualifiedFunctionName
    : ( keyspaceName '.' )? functionName
    ;

keyspaceName
    : UNQUOTED_IDENTIFIER
    | QUOTED_IDENTIFIER
    ;

functionName
    : UNQUOTED_IDENTIFIER
    | QUOTED_IDENTIFIER
    ;

functionArgs
    :  functionArg ( ',' functionArg )*
    ;

functionArg
    : INTEGER
    | FLOAT
    | BOOLEAN
    | DURATION
    | UUID
    | HEXNUMBER
    | STRING_LITERAL
    | ( '-' )? ( K_NAN | K_INFINITY )
    | QUOTED_IDENTIFIER
    | UNQUOTED_IDENTIFIER
    ;

// Case-insensitive alpha characters
fragment A: ('a'|'A');
fragment B: ('b'|'B');
fragment C: ('c'|'C');
fragment D: ('d'|'D');
fragment E: ('e'|'E');
fragment F: ('f'|'F');
fragment G: ('g'|'G');
fragment H: ('h'|'H');
fragment I: ('i'|'I');
fragment J: ('j'|'J');
fragment K: ('k'|'K');
fragment L: ('l'|'L');
fragment M: ('m'|'M');
fragment N: ('n'|'N');
fragment O: ('o'|'O');
fragment P: ('p'|'P');
fragment Q: ('q'|'Q');
fragment R: ('r'|'R');
fragment S: ('s'|'S');
fragment T: ('t'|'T');
fragment U: ('u'|'U');
fragment V: ('v'|'V');
fragment W: ('w'|'W');
fragment X: ('x'|'X');
fragment Y: ('y'|'Y');
fragment Z: ('z'|'Z');

fragment DIGIT
    : '0'..'9'
    ;

fragment LETTER
    : ('A'..'Z' | 'a'..'z')
    ;

fragment HEX
    : ('A'..'F' | 'a'..'f' | '0'..'9')
    ;

fragment EXPONENT
    : E ('+' | '-')? DIGIT+
    ;

fragment ALPHANUMERIC
    : ( LETTER | DIGIT | '_' )
    ;

fragment DURATION_UNIT
    : Y
    | M O
    | W
    | D
    | H
    | M
    | S
    | M S
    | U S
    | '\u00B5' S
    | N S
    ;

WRITETIME
    : W R I T E T I M E
    ;

BOOLEAN
    : T R U E | F A L S E
    ;

K_NAN
    : N A N
    ;

K_INFINITY
    : I N F I N I T Y
    ;

INTEGER
    : '-'? DIGIT+
    ;

FLOAT
    : INTEGER EXPONENT
    | INTEGER '.' DIGIT* EXPONENT?
    ;

DURATION
    : '-'? DIGIT+ DURATION_UNIT (DIGIT+ DURATION_UNIT)*
    | '-'? 'P' (DIGIT+ 'Y')? (DIGIT+ 'M')? (DIGIT+ 'D')? ('T' (DIGIT+ 'H')? (DIGIT+ 'M')? (DIGIT+ 'S')?)? // ISO 8601 "format with designators"
    | '-'? 'P' DIGIT+ 'W'
    | '-'? 'P' DIGIT DIGIT DIGIT DIGIT '-' DIGIT DIGIT '-' DIGIT DIGIT 'T' DIGIT DIGIT ':' DIGIT DIGIT ':' DIGIT DIGIT // ISO 8601 "alternative format"
    ;

HEXNUMBER
    : '0' X HEX*
    ;

UUID
    : HEX HEX HEX HEX HEX HEX HEX HEX '-'
      HEX HEX HEX HEX '-'
      HEX HEX HEX HEX '-'
      HEX HEX HEX HEX '-'
      HEX HEX HEX HEX HEX HEX HEX HEX HEX HEX HEX HEX
    ;

STRING_LITERAL
    : '\'' ( ~'\'' | '\'' '\'' )* '\''
    ;

UNQUOTED_IDENTIFIER
    : ALPHANUMERIC ( ALPHANUMERIC )*
    ;

QUOTED_IDENTIFIER
    : '"' ( ~'"' | '"' '"' )+ '"'
    ;

WS
    : ( ' ' | '\t' | '\n' | '\r' )+ -> channel(HIDDEN)
    ;

