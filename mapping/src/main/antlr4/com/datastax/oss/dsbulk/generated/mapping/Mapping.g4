/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
    : STAR ( ':' | '=' ) STAR
    | STAR ( ':' | '=' ) '-' variable
    | STAR ( ':' | '=' ) '[' '-' variable ( ',' '-' variable )* ']'
    ;

indexedEntry
    : indexOrFunction ( ':' | '=' ) variableOrFunction
    ;

indexOrFunction
    : index
    | function
    | typedLiteral
    ;

index
    : INTEGER
    ;

fieldOrFunction
    : field
    | function
    | typedLiteral
    ;

variableOrFunction
    : variable
    | function
    | typedLiteral
    ;

field
    : identifier
    ;

variable
    : identifier
    | legacyTokens
    ;

legacyTokens
    : LEGACY_WRITETIME
    | LEGACY_TTL
    ;

keyspaceName
    : identifier
    ;

functionName
    : identifier
    ;

columnName
    : identifier
    ;

identifier
    : UNQUOTED_IDENTIFIER
    | QUOTED_IDENTIFIER
    // also valid as identifiers:
    | K_WRITETIME
    | K_TTL
    | nativeType
    ;

function
    : writetime
    | ttl
    | qualifiedFunctionName '(' functionArgs? ')'
    ;

writetime
    : K_WRITETIME '(' STAR ')'
    | K_WRITETIME '(' columnName ( ',' columnName )* ')'
    ;

ttl
    : K_TTL '(' STAR ')'
    | K_TTL '(' columnName ( ',' columnName )* ')'
    ;

qualifiedFunctionName
    : ( keyspaceName '.' )? functionName
    ;

functionArgs
    :  functionArg ( ',' functionArg )*
    ;

functionArg
    : columnName
    | literal
    | function
    ;

typedLiteral
    : '(' nativeType ')' literal
    ;

nativeType
    : K_ASCII
    | K_BIGINT
    | K_BLOB
    | K_BOOLEAN
    | K_COUNTER
    | K_DECIMAL
    | K_DOUBLE
    | K_FLOAT
    | K_INET
    | K_INT
    | K_TEXT
    | K_VARCHAR
    | K_TIMESTAMP
    | K_DATE
    | K_TIME
    | K_UUID
    | K_VARINT
    | K_TIMEUUID
    | K_TINYINT
    | K_SMALLINT
    | K_DURATION
    ;

literal
    : INTEGER
    | FLOAT
    | BOOLEAN
    | DURATION
    | UUID
    | HEXNUMBER
    | STRING_LITERAL
    | ( '-' )? ( K_NAN | K_INFINITY )
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

K_ASCII : A S C I I;
K_BIGINT : B I G I N T;
K_BLOB : B L O B;
K_BOOLEAN : B O O L E A N;
K_COUNTER : C O U N T E R;
K_DECIMAL : D E C I M A L;
K_DOUBLE : D O U B L E;
K_FLOAT : F L O A T;
K_INET : I N E T;
K_INT : I N T;
K_TEXT : T E X T;
K_VARCHAR : V A R C H A R;
K_TIMESTAMP : T I M E S T A M P;
K_DATE : D A T E;
K_TIME : T I M E;
K_UUID : U U I D;
K_VARINT : V A R I N T;
K_TIMEUUID : T I M E U U I D;
K_TINYINT : T I N Y I N T;
K_SMALLINT : S M A L L I N T;
K_DURATION : D U R A T I O N;

K_WRITETIME
    : W R I T E T I M E
    ;

K_TTL
    : T T L
    ;

LEGACY_WRITETIME
    : '__timestamp'
    ;

LEGACY_TTL
    : '__ttl'
    ;

STAR
  : '*'
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
    : LETTER ( ALPHANUMERIC )*
    ;

QUOTED_IDENTIFIER
    : '"' ( ~'"' | '"' '"' )+ '"'
    ;

WS
    : ( ' ' | '\t' | '\n' | '\r' )+ -> channel(HIDDEN)
    ;
