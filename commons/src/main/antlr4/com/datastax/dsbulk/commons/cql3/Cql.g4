/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */


/*
 * This is a simplified ANTLR4 version of the full grammar extracted from Apache Cassandra (TM) version 3.11.1.
*/

grammar Cql;

// PARSER

/** STATEMENTS **/

cqlStatement
    : selectStatement                 #cqlStatementSelectStatement
    | insertStatement                 #cqlStatementInsertStatement
    | updateStatement                 #cqlStatementUpdateStatement
    | deleteStatement                 #cqlStatementDeleteStatement
    | batchStatement                  #cqlStatementBatchStatement
    ;

/**
 * SELECT <expression>
 * FROM <CF>
 * WHERE KEY = "key1" AND COL > 1 AND COL < 100
 * LIMIT <NUMBER>;
 */
selectStatement
    : K_SELECT
      ( K_JSON )?
      ( ( K_DISTINCT )? selectClause )
      K_FROM columnFamilyName
      ( K_WHERE whereClause )?
      ( K_GROUP K_BY groupByClause ( ',' groupByClause )* )?
      ( K_ORDER K_BY orderByClause ( ',' orderByClause )* )?
      ( K_PER K_PARTITION K_LIMIT intValue )?
      ( K_LIMIT intValue )?
      ( K_ALLOW K_FILTERING )?
    ;

selectClause
    : selector (',' selector)*
    | '*'
    ;

selector
    : unaliasedSelector (K_AS noncolIdent)?
    ;

/*
 * A single selection. The core of it is selecting a column, but we also allow any term and function, as well as
 * sub-element selection for UDT.
 */
unaliasedSelector
    :  ( cident
       | value
       | '(' comparatorType ')' value
       | K_COUNT '(' '*' ')'
       | K_WRITETIME '(' cident ')'
       | K_TTL       '(' cident ')'
       | K_CAST      '(' unaliasedSelector K_AS nativeType ')'
       | functionName selectionFunctionArgs
       ) ( '.' fident )*
    ;

selectionFunctionArgs
    : '(' ')'
    | '(' unaliasedSelector ( ',' unaliasedSelector )*  ')'
    ;

whereClause
    : relationOrExpression (K_AND relationOrExpression)*
    ;

relationOrExpression
    : relation               #relationOrExpressionRelation
    | customIndexExpression  #relationOrExpressionExpression
    ;

customIndexExpression
    : 'expr(' idxName ',' term ')'
    ;

orderByClause
    : cident (K_ASC | K_DESC)?
    ;

groupByClause
    : cident
    ;

/**
 * INSERT INTO <CF> (<column>, <column>, <column>, ...)
 * VALUES (<value>, <value>, <value>, ...)
 * USING TIMESTAMP <long>;
 *
 */
insertStatement
    : K_INSERT K_INTO columnFamilyName
        ( normalInsertStatement
        | K_JSON jsonInsertStatement)
    ;

normalInsertStatement
    : '(' cident  ( ',' cident )* ')'
      K_VALUES
      '(' term ( ',' term )* ')'
      ( K_IF K_NOT K_EXISTS )?
      ( usingClause )?
    ;

jsonInsertStatement
    : jsonValue
      ( K_DEFAULT ( K_NULL | K_UNSET ) )?
      ( K_IF K_NOT K_EXISTS )?
      ( usingClause )?
    ;

jsonValue
    : STRING_LITERAL    #jsonValueStringLiteral
    | ':' noncolIdent   #jsonValueNonColIdent
    | QMARK             #jsonValueQuestionMark
    ;

usingClause
    : K_USING usingClauseObjective ( K_AND usingClauseObjective )*
    ;

usingClauseObjective
    : K_TIMESTAMP intValue #usingTimestamp
    | K_TTL intValue       #usingTtl
    ;

/**
 * UPDATE <CF>
 * USING TIMESTAMP <long>
 * SET name1 = value1, name2 = value2
 * WHERE key = value;
 * [IF (EXISTS | name = value, ...)];
 */
updateStatement
    : K_UPDATE columnFamilyName
      ( usingClause )?
      K_SET columnOperation (',' columnOperation)*
      K_WHERE whereClause
      ( K_IF ( K_EXISTS | updateConditions ))?
    ;

updateConditions
    : columnCondition ( K_AND columnCondition )*
    ;


/**
 * DELETE name1, name2
 * FROM <CF>
 * USING TIMESTAMP <long>
 * WHERE KEY = keyname
   [IF (EXISTS | name = value, ...)];
 */
deleteStatement
    : K_DELETE ( deleteSelection )?
      K_FROM columnFamilyName
      ( usingClauseDelete )?
      K_WHERE whereClause
      ( K_IF ( K_EXISTS | updateConditions ))?
    ;

deleteSelection
    : deleteOp (',' deleteOp)*
    ;

deleteOp
    : cident                #deleteOpIdentSimple
    | cident '[' term ']'   #deleteOpIdentArray
    | cident '.' fident     #deleteOpIdentField
    ;

usingClauseDelete
    : K_USING K_TIMESTAMP intValue
    ;

/**
 * BEGIN BATCH
 *   UPDATE <CF> SET name1 = value1 WHERE KEY = keyname1;
 *   UPDATE <CF> SET name2 = value2 WHERE KEY = keyname2;
 *   UPDATE <CF> SET name3 = value3 WHERE KEY = keyname3;
 *   ...
 * APPLY BATCH
 *
 * OR
 *
 * BEGIN BATCH
 *   INSERT INTO <CF> (KEY, <name>) VALUES ('<key>', '<value>');
 *   INSERT INTO <CF> (KEY, <name>) VALUES ('<key>', '<value>');
 *   ...
 * APPLY BATCH
 *
 * OR
 *
 * BEGIN BATCH
 *   DELETE name1, name2 FROM <CF> WHERE key = <key>
 *   DELETE name3, name4 FROM <CF> WHERE key = <key>
 *   ...
 * APPLY BATCH
 */
batchStatement
    : K_BEGIN
      ( K_UNLOGGED | K_COUNTER )?
      K_BATCH ( usingClause )?
          ( batchStatementObjective EOS? )*
      K_APPLY K_BATCH
    ;

batchStatementObjective
    : insertStatement  #batchStatementObjectiveInsertStatement
    | updateStatement  #batchStatementObjectiveUpdateStatement
    | deleteStatement  #batchStatementObjectiveDeleteStatement
    ;



/** DEFINITIONS **/

// Column Identifiers.  These need to be treated differently from other
// identifiers because the underlying comparator is not necessarily text. See
// CASSANDRA-8178 for details.
cident
    : IDENT               #cidentSimple
    | QUOTED_NAME         #cidentQuoted
    | unreservedKeyword   #cidentUnreservedKeyword
    ;


fident
    : IDENT               #fidentSimple
    | QUOTED_NAME         #fidentQuoted
    | unreservedKeyword   #fidentUnreservedKeyword
    ;

// Identifiers that do not refer to columns
noncolIdent
    : IDENT               #nonColIdentSimple
    | QUOTED_NAME         #nonColIdentQuoted
    | unreservedKeyword   #nonColIdentUnreservedKeyword
    ;

// Keyspace & Column family names
keyspaceName
    : ksName
    ;

columnFamilyName
    : (ksName '.')? cfName
    ;

userTypeName
    : (noncolIdent '.')? nonTypeIdent
    ;

ksName
    : IDENT                #ksNameSimple
    | QUOTED_NAME          #ksNameQuoted
    | unreservedKeyword   #ksNameUnreservedKeyword
    | QMARK                #ksNameQuestionMark
    ;

cfName
    : IDENT               #cfNameSimple
    | QUOTED_NAME         #cfNameQuoted
    | unreservedKeyword   #cfNameUnreservedKeyword
    | QMARK               #cfNameQuestionMark
    ;

idxName
    : IDENT               #idxNameSimple
    | QUOTED_NAME         #idxNameQuoted
    | unreservedKeyword   #idxNameUnreservedKeyword
    | QMARK               #idxNameQuestionMark
    ;

constant
    : STRING_LITERAL                       #constantStringLiteral
    | INTEGER                              #constantInteger
    | FLOAT                                #constantFloat
    | BOOLEAN                              #constantBoolean
    | DURATION                             #constantDuration
    | UUID                                 #constantUUID
    | HEXNUMBER                            #constantHexNumber
    | ('-' )? (K_NAN | K_INFINITY)         #constantNanInfinity
    ;

setOrMapLiteral
    : ':' term ( ',' term ':' term )*
    | ( ',' term )*
    ;

collectionLiteral
    : '[' ( term ( ',' term )* )? ']'  #collectionLiteralList
    | '{' term setOrMapLiteral '}'     #collectionLiteralSetOrMap
    // Note that we have an ambiguity between maps and set for "{}". So we force it to a set literal,
    // and deal with it later based on the type of the column (SetLiteral.java).
    | '{' '}'                          #collectionLiteralSetOrMapEmpty
    ;

usertypeLiteral
    // We don't allow empty literals because that conflicts with sets/maps and is currently useless since we don't allow empty user types
    : '{' fident ':' term ( ',' fident ':' term )* '}'
    ;

tupleLiteral
    : '(' term ( ',' term )* ')'
    ;

value
    : constant              #valueConstant
    | collectionLiteral     #valueCollectionLiteral
    | usertypeLiteral       #valueUserTypeLiteral
    | tupleLiteral          #valueTupleLiteral
    | K_NULL                #valueNull
    | ':' noncolIdent       #valueNonColIdent
    | QMARK                 #valueQuestionMark
    ;

intValue
    : INTEGER            #intValueSimple
    | ':' noncolIdent    #intValueNonColIdent
    | QMARK              #intValueQuestionMark
    ;

functionName
    : (keyspaceName '.')? allowedFunctionName
    ;

allowedFunctionName
    : IDENT                        #allowedFunctionNameIdent
    | QUOTED_NAME                  #allowedFunctionNameQuotedName
    | unreservedFunctionKeyword    #allowedFunctionNameUnreservedKeyword
    | K_TOKEN                      #allowedFunctionNameToken
    | K_COUNT                      #allowedFunctionNameCount
    ;

function
    : functionName '(' ')'              #functionNameWithoutArgs
    | functionName '(' functionArgs ')' #functionNameWithArgs
    ;

functionArgs
    : term ( ',' term )*
    ;

term
    : value                       #termValue
    | function                    #termFunction
    | '(' comparatorType ')' term #termTyped
    ;

columnOperation
    : cident columnOperationDifferentiator
    ;

columnOperationDifferentiator
    : '=' normalColumnOperation                #columnOperationDifferentiatorEquals
    | shorthandColumnOperation                 #columnOperationDifferentiatorShortHand
    | '[' term ']' collectionColumnOperation   #columnOperationDifferentiatorArray
    | '.' fident udtColumnOperation            #columnOperationDifferentiatorField
    ;

normalColumnOperation
    : term ('+' cident )?      #normalColumnOperationTerm
    | cident ('+' | '-') term  #normalColumnOperationCidentTerm
    | cident INTEGER           #normalColumnOperationCidentInteger
    ;

shorthandColumnOperation
    : ('+=' | '-=') term
    ;

collectionColumnOperation
    : '=' term
    ;

udtColumnOperation
    : '=' term
    ;

columnCondition
    // Note: we'll reject duplicates later
    : cident
        ( relationType term
        | K_IN
            ( singleColumnInValues
            | inMarker
            )
        | '[' term ']'
            ( relationType term
            | K_IN
                ( singleColumnInValues
                | inMarker
                )
            )
        | '.' fident
            ( relationType term
            | K_IN
                ( singleColumnInValues
                | inMarker
                )
            )
        )
    ;

relationType
    : '='
    | '<'
    | '<='
    | '>'
    | '>='
    | '!='
    ;

relation
    : cident relationType term                     #relationCidentRelationTypeTerm
    | cident K_LIKE term                           #relationCidentLikeTerm
    | cident K_IS K_NOT K_NULL                     #relationCidentIsNotNull
    | K_TOKEN tupleOfIdentifiers relationType term #relationToken
    | cident K_IN inMarker                         #relationInMarker
    | cident K_IN singleColumnInValues             #relationInSingle
    | cident K_CONTAINS (K_KEY)? term              #relationContains
    | cident '[' term ']' relationType term        #relationCidentArrayRelationTypeTerm
    | tupleOfIdentifiers
      ( K_IN
          ( '(' ')'
          | inMarkerForTuple /* (a, b, c) IN ? */
          | tupleOfTupleLiterals /* (a, b, c) IN ((1, 2, 3), (4, 5, 6), ...) */
          | tupleOfMarkersForTuples /* (a, b, c) IN (?, ?, ...) */
          )
      | relationType tupleLiteral /* (a, b, c) > (1, 2, 3) or (a, b, c) > (?, ?, ?) */
      | relationType markerForTuple /* (a, b, c) >= ? */
      )                                            #relationInMultiple
    | '(' relation ')'                             #relationParentheses
    ;

inMarker
    : QMARK             #inMarkerQuestionMark
    | ':' noncolIdent   #inMarkerNonColIdent
    ;

tupleOfIdentifiers
    : '(' cident (',' cident)* ')'
    ;

singleColumnInValues
    : '(' ( term (',' term)* )? ')'
    ;

tupleOfTupleLiterals
    : '(' tupleLiteral (',' tupleLiteral)* ')'
    ;

markerForTuple
    : QMARK            #markerForTupleQuestionMark
    | ':' noncolIdent  #markerForTupleNonColIdent
    ;

tupleOfMarkersForTuples
    : '(' markerForTuple (',' markerForTuple)* ')'
    ;

inMarkerForTuple
    : QMARK            #inMarkerForTupleQuestionMark
    | ':' noncolIdent  #inMarkerForTupleNonColIdent
    ;

comparatorType
    : nativeType                      #comparatorTypeNativeType
    | collectionType                  #comparatorTypeCollectionType
    | tupleType                       #comparatorTypeTupleType
    | userTypeName                    #comparatorTypeUserType
    | K_FROZEN '<' comparatorType '>' #comparatorTypeFrozenType
    ;

nativeType
    : K_ASCII
    | K_BIGINT
    | K_BLOB
    | K_BOOLEAN
    | K_COUNTER
    | K_DECIMAL
    | K_DOUBLE
    | K_DURATION
    | K_FLOAT
    | K_INET
    | K_INT
    | K_SMALLINT
    | K_TEXT
    | K_TIMESTAMP
    | K_TINYINT
    | K_UUID
    | K_VARCHAR
    | K_VARINT
    | K_TIMEUUID
    | K_DATE
    | K_TIME
    ;

collectionType
    : K_MAP  '<' comparatorType ',' comparatorType '>' #collectionTypeMap
    | K_LIST '<' comparatorType '>'                    #collectionTypeList
    | K_SET  '<' comparatorType '>'                    #collectionTypeSet
    ;

tupleType
    : K_TUPLE '<' comparatorType (',' comparatorType)* '>'
    ;


// Basically the same as cident, but we need to exlude existing CQL3 types
// (which for some reason are not reserved otherwise)
nonTypeIdent
    : IDENT                    #nonTypeIdentIdent
    | QUOTED_NAME              #nonTypeIdentIdentQuotedName
    | basicUnreservedKeyword   #nonTypeIdentBasicUnreservedKeyword
    | K_KEY                    #nonTypeIdentKey
    ;

unreservedKeyword
    : unreservedFunctionKeyword                                               #unreservedKeywordFunction
    | (K_TTL | K_COUNT | K_WRITETIME | K_KEY | K_CAST | K_JSON | K_DISTINCT)  #unreservedKeywordConstant
    ;

unreservedFunctionKeyword
    : basicUnreservedKeyword  #unreservedFunctionKeywordBasic
    | nativeType              #unreservedFunctionKeywordNativeType
    ;

basicUnreservedKeyword
    : ( K_AS
        | K_CLUSTERING
        | K_TYPE
        | K_VALUES
        | K_MAP
        | K_LIST
        | K_FILTERING
        | K_EXISTS
        | K_CONTAINS
        | K_FROZEN
        | K_TUPLE
        | K_LIKE
        | K_PER
        | K_PARTITION
        | K_GROUP
        )
    ;


// LEXER

// Case-insensitive keywords
K_SELECT:      S E L E C T;
K_FROM:        F R O M;
K_AS:          A S;
K_WHERE:       W H E R E;
K_AND:         A N D;
K_KEY:         K E Y;
K_INSERT:      I N S E R T;
K_UPDATE:      U P D A T E;
K_LIMIT:       L I M I T;
K_PER:         P E R;
K_PARTITION:   P A R T I T I O N;
K_USING:       U S I N G;
K_DISTINCT:    D I S T I N C T;
K_COUNT:       C O U N T;
K_SET:         S E T;
K_BEGIN:       B E G I N;
K_UNLOGGED:    U N L O G G E D;
K_BATCH:       B A T C H;
K_APPLY:       A P P L Y;
K_DELETE:      D E L E T E;
K_IN:          I N;
K_INTO:        I N T O;
K_VALUES:      V A L U E S;
K_TIMESTAMP:   T I M E S T A M P;
K_TTL:         T T L;
K_CAST:        C A S T;
K_TYPE:        T Y P E;
K_ORDER:       O R D E R;
K_BY:          B Y;
K_ASC:         A S C;
K_DESC:        D E S C;
K_ALLOW:       A L L O W;
K_FILTERING:   F I L T E R I N G;
K_IF:          I F;
K_IS:          I S;
K_CONTAINS:    C O N T A I N S;
K_GROUP:       G R O U P;

K_CLUSTERING:  C L U S T E R I N G;
K_ASCII:       A S C I I;
K_BIGINT:      B I G I N T;
K_BLOB:        B L O B;
K_BOOLEAN:     B O O L E A N;
K_COUNTER:     C O U N T E R;
K_DECIMAL:     D E C I M A L;
K_DOUBLE:      D O U B L E;
K_DURATION:    D U R A T I O N;
K_FLOAT:       F L O A T;
K_INET:        I N E T;
K_INT:         I N T;
K_SMALLINT:    S M A L L I N T;
K_TINYINT:     T I N Y I N T;
K_TEXT:        T E X T;
K_UUID:        U U I D;
K_VARCHAR:     V A R C H A R;
K_VARINT:      V A R I N T;
K_TIMEUUID:    T I M E U U I D;
K_TOKEN:       T O K E N;
K_WRITETIME:   W R I T E T I M E;
K_DATE:        D A T E;
K_TIME:        T I M E;

K_NULL:        N U L L;
K_NOT:         N O T;
K_EXISTS:      E X I S T S;

K_MAP:         M A P;
K_LIST:        L I S T;
K_NAN:         N A N;
K_INFINITY:    I N F I N I T Y;
K_TUPLE:       T U P L E;

K_FROZEN:      F R O Z E N;

K_JSON:        J S O N;
K_DEFAULT:     D E F A U L T;
K_UNSET:       U N S E T;
K_LIKE:        L I K E;

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

STRING_LITERAL
    : /* pg-style string literal */
      '$' '$' ( ~'$' | '$' ~'$' )* '$' '$'
    | /* conventional quoted string literal */
      '\'' ( ~'\'' | '\'' '\'' )* '\''
    ;

QUOTED_NAME
    : '"' ( ~'"' | '"' '"' )+ '"'
    ;

EMPTY_QUOTED_NAME
    : '"' '"'
    ;

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

INTEGER
    : '-'? DIGIT+
    ;

QMARK
    : '?'
    ;

FLOAT
    : INTEGER EXPONENT
    | INTEGER '.' DIGIT* EXPONENT?
    ;

/*
 * This has to be before IDENT so it takes precendence over it.
 */
BOOLEAN
    : T R U E | F A L S E
    ;

DURATION
    : '-'? DIGIT+ DURATION_UNIT (DIGIT+ DURATION_UNIT)*
    | '-'? 'P' (DIGIT+ 'Y')? (DIGIT+ 'M')? (DIGIT+ 'D')? ('T' (DIGIT+ 'H')? (DIGIT+ 'M')? (DIGIT+ 'S')?)? // ISO 8601 "format with designators"
    | '-'? 'P' DIGIT+ 'W'
    | '-'? 'P' DIGIT DIGIT DIGIT DIGIT '-' DIGIT DIGIT '-' DIGIT DIGIT 'T' DIGIT DIGIT ':' DIGIT DIGIT ':' DIGIT DIGIT // ISO 8601 "alternative format"
    ;

IDENT
    : LETTER (LETTER | DIGIT | '_')*
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

WS
    : (' ' | '\t' | '\n' | '\r')+ -> channel(HIDDEN)
    ;

COMMENT
    : ('--' | '//') .*? ('\n'|'\r') -> channel(HIDDEN)
    ;

MULTILINE_COMMENT
    : '/*' .*? '*/' -> channel(HIDDEN)
    ;

// End of statement
EOS
    : ';'
    ;
