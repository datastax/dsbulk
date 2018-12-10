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
    : mappedEntry  ( ',' mappedEntry  )* EOF
    | indexedEntry ( ',' indexedEntry )* EOF
    ;

mappedEntry
    : regularMappedEntry
    | inferredMappedEntry
    ;

regularMappedEntry
    : field ( ':' | '=' ) variableWithFunction
    ;

inferredMappedEntry
    : '*' ( ':' | '=' ) '*'
    | '*' ( ':' | '=' ) '-' variableWithFunction
    | '*' ( ':' | '=' ) '[' '-' variableWithFunction ( ',' '-' variableWithFunction )* ']'
    ;

indexedEntry
    : variableWithFunction
    ;

field
    : UNQUOTED_STRING
    | QUOTED_STRING
    | function
    ;

variableWithFunction
    : variable
    | function
    ;

variable
    : UNQUOTED_STRING
    | QUOTED_STRING
    ;

function
    : functionName '(' ')'
    | functionName '(' functionArgs ')'
    ;

functionName
    : ( identifier '.' )? identifier
    ;

functionArgs
    :  identifier ( ',' identifier )*
    ;

identifier
    : UNQUOTED_STRING
    | QUOTED_STRING
    ;

fragment ALPHANUMERIC
    : ( 'A'..'Z' | 'a'..'z' | '0'..'9' | '_' )
    ;

UNQUOTED_STRING
    : ALPHANUMERIC ( ALPHANUMERIC )*
    ;

QUOTED_STRING
    : '"' ( ~'"' | '"' '"' )+ '"'
    ;

WS
    : ( ' ' | '\t' | '\n' | '\r' )+ -> channel(HIDDEN)
    ;

