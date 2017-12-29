/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
grammar Cql;

file
    : cqlStatement (EOS cqlStatement)* EOS? EOF
    ;

query
    : cqlStatement EOS? EOF
    ;

resource
    : cassandraResource
    ;

// PARSER

/** STATEMENTS **/

cqlStatement
    : selectStatement                 #cqlStatementSelectStatement
    | insertStatement                 #cqlStatementInsertStatement
    | updateStatement                 #cqlStatementUpdateStatement
    | batchStatement                  #cqlStatementBatchStatement
    | deleteStatement                 #cqlStatementDeleteStatement
    | useStatement                    #cqlStatementUseStatement
    | truncateStatement               #cqlStatementTruncateStatement
    | createKeyspaceStatement         #cqlStatementCreateKeyspaceStatement
    | createTableStatement            #cqlStatementCreateTableStatement
    | createIndexStatement            #cqlStatementCreateIndexStatement
    | dropKeyspaceStatement           #cqlStatementDropKeyspaceStatement
    | dropTableStatement              #cqlStatementDropTableStatement
    | dropIndexStatement              #cqlStatementDropIndexStatement
    | alterTableStatement             #cqlStatementAlterTableStatement
    | alterKeyspaceStatement          #cqlStatementAlterKeyspaceStatement
    | grantPermissionsStatement       #cqlStatementGrantPermissionsStatement
    | revokePermissionsStatement      #cqlStatementRevokePermissionsStatement
    | listPermissionsStatement        #cqlStatementListPermissionsStatement
    | createUserStatement             #cqlStatementCreateUserStatement
    | alterUserStatement              #cqlStatementAlterUserStatement
    | dropUserStatement               #cqlStatementDropUserStatement
    | listUsersStatement              #cqlStatementListUsersStatement
    | createTriggerStatement          #cqlStatementCreateTriggerStatement
    | dropTriggerStatement            #cqlStatementDropTriggerStatement
    | createTypeStatement             #cqlStatementCreateTypeStatement
    | alterTypeStatement              #cqlStatementAlterTypeStatement
    | dropTypeStatement               #cqlStatementDropTypeStatement
    | createFunctionStatement         #cqlStatementCreateFunctionStatement
    | dropFunctionStatement           #cqlStatementDropFunctionStatement
    | createAggregateStatement        #cqlStatementCreateAggregateStatement
    | dropAggregateStatement          #cqlStatementDropAggregateStatement
    | createRoleStatement             #cqlStatementCreateRoleStatement
    | alterRoleStatement              #cqlStatementAlterRoleStatement
    | dropRoleStatement               #cqlStatementDropRoleStatement
    | listRolesStatement              #cqlStatementListRolesStatement
    | grantRoleStatement              #cqlStatementGrantRoleStatement
    | revokeRoleStatement             #cqlStatementRevokeRoleStatement
    | createMaterializedViewStatement #cqlStatementCreateMaterializedViewStatement
    | dropMaterializedViewStatement   #cqlStatementDropMaterializedViewStatement
    | alterMaterializedViewStatement  #cqlStatementAlterMaterializedViewStatement
    | restrictPermissionsStatement    #cqlStatementRestrictPermissionsStatement
    | unrestrictPermissionsStatement  #cqlStatementUnrestrictPermissionsStatement
    ;

/*
 * USE <KEYSPACE>;
 */
useStatement
    : K_USE keyspaceName
    ;

/**
 * SELECT <expression>
 * FROM <CF>
 * WHERE KEY = "key1" AND COL > 1 AND COL < 100
 * LIMIT <NUMBER>;
 */
selectStatement
    : K_SELECT
        // json is a valid column name. By consequence, we need to resolve the ambiguity for "json - json"
      ( /*(K_JSON selectClause)=>*/ K_JSON  )? selectClause
      K_FROM columnFamilyName
      ( K_WHERE whereClause )?
      ( K_GROUP K_BY groupByClause ( ',' groupByClause )* )?
      ( K_ORDER K_BY orderByClause ( ',' orderByClause )* )?
      ( K_PER K_PARTITION K_LIMIT intValue  )?
      ( K_LIMIT intValue  )?
      ( K_ALLOW K_FILTERING   )?
    ;

selectClause
    // distinct is a valid column name. By consequence, we need to resolve the ambiguity for "distinct - distinct"
    : /*(K_DISTINCT selectors)=>*/ K_DISTINCT selectors  #selectClauseDistinct
    | selectors                                          #selectClauseSimple
    ;

selectors
    : selector (',' selector )* #selectorsSimple
    | '*'                       #selectorsAll
    ;

selector
    : unaliasedSelector (K_AS noncol_ident )?
    ;

unaliasedSelector
    : selectionAddition
    ;

selectionAddition
    :   selectionMultiplication
        ( '+' selectionMultiplication
        | '-' selectionMultiplication
        )*
    ;

selectionMultiplication
    :   selectionGroup
        ( '*' selectionGroup
        | '/' selectionGroup
        | '%' selectionGroup
        )*
    ;

selectionGroup
    : /*(selectionGroupWithField)=>*/ selectionGroupWithField
    | selectionGroupWithoutField
    | '-' selectionGroup
    ;

selectionGroupWithField
    : selectionGroupWithoutField selectorModifier
    ;

selectorModifier
    : fieldSelectorModifier selectorModifier
    | '[' collectionSubSelection ']' selectorModifier
    |
    ;

fieldSelectorModifier
    : '.' fident
    ;

collectionSubSelection
    : ( term (  RANGE (term)? )?
      | RANGE  term
      )
     ;

selectionGroupWithoutField
    : simpleUnaliasedSelector
    | /*(selectionTypeHint)=>*/ selectionTypeHint
    | selectionTupleOrNestedSelector
    | selectionList
    | selectionMapOrSet
    // UDTs are equivalent to maps from the syntax point of view, so the final decision will be done in Selectable.WithMapOrUdt
    ;

selectionTypeHint
    : '(' comparatorType ')' selectionGroupWithoutField
    ;

selectionList
    : '[' ( unaliasedSelector ( ',' unaliasedSelector )* )? ']'
    ;

selectionMapOrSet
    : '{' unaliasedSelector ( selectionMap | selectionSet ) '}'
    | '{' '}'
    ;

selectionMap
      : ':' unaliasedSelector ( ',' unaliasedSelector ':' unaliasedSelector )*
      ;

selectionSet
      : ( ',' unaliasedSelector )*
      ;

selectionTupleOrNestedSelector
    : '(' unaliasedSelector (',' unaliasedSelector )* ')'
    ;

/*
 * A single selection. The core of it is selecting a column, but we also allow any term and function, as well as
 * sub-element selection for UDT.
 */
simpleUnaliasedSelector
    : sident
    | selectionLiteral
    | selectionFunction
    ;

selectionFunction
    : K_COUNT '(' '*' ')'                                    #selectionFunctionCount
    | K_WRITETIME '(' cident ')'                             #selectionFunctionWriteTime
    | K_TTL       '(' cident ')'                             #selectionFunctionTtl
    | K_CAST      '(' unaliasedSelector K_AS native_type ')' #selectionFunctionCast
    | functionName selectionFunctionArgs                     #selectionFunctionOther
    ;

selectionLiteral
    : constant         #selectionLiteralConstant
    | K_NULL           #selectionLiteralNull
    | ':' noncol_ident #selectionLiteralNonColIdent
    | QMARK            #selectionLiteralQuestionMark
    ;

selectionFunctionArgs
    : '(' (unaliasedSelector ( ',' unaliasedSelector )* )? ')'
    ;

sident
    : IDENT               #sidentSimple
    | QUOTED_NAME         #sidentQuoted
    | unreserved_keyword  #sidentUnreservedKeyword
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
    : unaliasedSelector
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
      ( K_DEFAULT ( K_NULL | ( K_UNSET) ) )?
      ( K_IF K_NOT K_EXISTS )?
      ( usingClause )?
    ;

jsonValue
    : STRING_LITERAL    #jsonValueStringLiteral
    | ':' noncol_ident  #jsonValueNonColIdent
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
          ( batchStatementObjective ';'? )*
      K_APPLY K_BATCH
    ;

batchStatementObjective
    : insertStatement  #batchStatementObjectiveInsertStatement
    | updateStatement  #batchStatementObjectiveUpdateStatement
    | deleteStatement  #batchStatementObjectiveDeleteStatement
    ;

createAggregateStatement
    : K_CREATE (K_OR K_REPLACE)?
      K_AGGREGATE
      (K_IF K_NOT K_EXISTS)?
      functionName
      '(' ( comparatorType ( ',' comparatorType )* )? ')'
      K_SFUNC allowedFunctionName
      K_STYPE comparatorType
      ( K_FINALFUNC allowedFunctionName )?
      ( K_INITCOND term )?
    ;

dropAggregateStatement
    : K_DROP K_AGGREGATE
      (K_IF K_EXISTS )?
      functionName
      ( '(' ( comparatorType ( ',' comparatorType )* )? ')' )?
    ;

createFunctionStatement
    : K_CREATE (K_OR K_REPLACE)?
      K_FUNCTION
      (K_IF K_NOT K_EXISTS)?
      functionName
      '(' ( noncol_ident comparatorType ( ',' noncol_ident comparatorType )* )? ')'
      ( (K_RETURNS K_NULL) | (K_CALLED)) K_ON K_NULL K_INPUT
      K_RETURNS comparatorType
      K_LANGUAGE IDENT
      K_AS STRING_LITERAL
    ;

dropFunctionStatement
    : K_DROP K_FUNCTION
      (K_IF K_EXISTS )?
      functionName
      ( '(' ( comparatorType ( ',' comparatorType )* )? ')' )?
    ;

/**
 * CREATE KEYSPACE [IF NOT EXISTS] <KEYSPACE> WITH attr1 = value1 AND attr2 = value2;
 */
createKeyspaceStatement
    : K_CREATE K_KEYSPACE (K_IF K_NOT K_EXISTS )? keyspaceName
      K_WITH properties
    ;

/**
 * CREATE COLUMNFAMILY [IF NOT EXISTS] <CF> (
 *     <name1> <type>,
 *     <name2> <type>,
 *     <name3> <type>
 * ) WITH <property> = <value> AND ...;
 */
createTableStatement
    : K_CREATE K_COLUMNFAMILY (K_IF K_NOT K_EXISTS )?
      columnFamilyName
      cfamDefinition
    ;

cfamDefinition
    : '(' cfamColumns ( ',' cfamColumns? )* ')'
      ( K_WITH cfamProperty ( K_AND cfamProperty )* )?
    ;

cfamColumns
    : ident comparatorType (K_STATIC)? (K_PRIMARY K_KEY)?  #cfamColumnsSimple
    | K_PRIMARY K_KEY '(' pkDef (',' ident )* ')'          #cfamColumnsPrimaryKey
    ;

pkDef
    : ident                         #pkDefSimple
    | '(' ident ( ',' ident )* ')'  #pkDefComposite
    ;

cfamProperty
    : property                                                           #cfamPropertySimple
    | K_COMPACT K_STORAGE                                                #cfamPropertyCompactStorage
    | K_CLUSTERING K_ORDER K_BY '(' cfamOrdering (',' cfamOrdering)* ')' #cfamPropertyClusteringOrderBy
    ;

cfamOrdering
    : ident (K_ASC | K_DESC )
    ;


/**
 * CREATE TYPE foo (
 *    <name1> <type1>,
 *    <name2> <type2>,
 *    ....
 * )
 */
createTypeStatement
    : K_CREATE K_TYPE (K_IF K_NOT K_EXISTS )?
         userTypeName
         '(' typeColumns ( ',' typeColumns )* ')'
    ;

typeColumns
    : fident comparatorType
    ;


/**
 * CREATE INDEX [IF NOT EXISTS] [indexName] ON <columnFamily> (<columnName>);
 * CREATE CUSTOM INDEX [IF NOT EXISTS] [indexName] ON <columnFamily> (<columnName>) USING <indexClass>;
 */
createIndexStatement
    : K_CREATE (K_CUSTOM)? K_INDEX (K_IF K_NOT K_EXISTS )?
        (idxName)? K_ON columnFamilyName '(' (indexIdent (',' indexIdent)*)? ')'
        (K_USING STRING_LITERAL)?
        (K_WITH properties)?
    ;

indexIdent
    : cident                     #indexIdentSimple
    | K_VALUES '(' cident ')'    #indexIdentValues
    | K_KEYS '(' cident ')'      #indexIdentKeys
    | K_ENTRIES '(' cident ')'   #indexIdentEntries
    | K_FULL '(' cident ')'      #indexIdentFull
    ;

/**
 * CREATE MATERIALIZED VIEW <viewName> AS
 *  SELECT <columns>
 *  FROM <CF>
 *  WHERE <pkColumns> IS NOT NULL
 *  PRIMARY KEY (<pkColumns>)
 *  WITH <property> = <value> AND ...;
 */
createMaterializedViewStatement
    : K_CREATE K_MATERIALIZED K_VIEW (K_IF K_NOT K_EXISTS)? columnFamilyName K_AS
        K_SELECT selectors K_FROM columnFamilyName
        (K_WHERE whereClause)?
        K_PRIMARY K_KEY (
        '(' '(' cident ( ',' cident )* ')' ( ',' cident )* ')'
    |   '(' cident ( ',' cident )* ')'
        )
        ( K_WITH cfamProperty ( K_AND cfamProperty )*)?
    ;

/**
 * CREATE TRIGGER triggerName ON columnFamily USING 'triggerClass';
 */
createTriggerStatement
    : K_CREATE K_TRIGGER (K_IF K_NOT K_EXISTS )? (ident)
        K_ON columnFamilyName K_USING STRING_LITERAL
    ;

/**
 * DROP TRIGGER [IF EXISTS] triggerName ON columnFamily;
 */
dropTriggerStatement
    : K_DROP K_TRIGGER (K_IF K_EXISTS )? (ident) K_ON columnFamilyName
    ;

/**
 * ALTER KEYSPACE <KS> WITH <property> = <value>;
 */
alterKeyspaceStatement
    : K_ALTER K_KEYSPACE keyspaceName
        K_WITH properties
    ;

/**
 * ALTER COLUMN FAMILY <CF> ALTER <column> TYPE <newtype>;
 * ALTER COLUMN FAMILY <CF> ADD <column> <newtype>; | ALTER COLUMN FAMILY <CF> ADD (<column> <newtype>,<column1> <newtype1>..... <column n> <newtype n>)
 * ALTER COLUMN FAMILY <CF> DROP <column>; | ALTER COLUMN FAMILY <CF> DROP ( <column>,<column1>.....<column n>)
 * ALTER COLUMN FAMILY <CF> WITH <property> = <value>;
 * ALTER COLUMN FAMILY <CF> RENAME <column> TO <column>;
 */
alterTableStatement
    : K_ALTER K_COLUMNFAMILY columnFamilyName
          ( K_ALTER schema_cident  K_TYPE comparatorType
          | K_ADD  ( ( schema_cident  comparatorType   cfisStatic)
                     | ('('  schema_cident  comparatorType  cfisStatic
                       ( ',' schema_cident  comparatorType  cfisStatic )* ')' ) )
          | K_DROP ( ( schema_cident
                      | ('('  schema_cident
                        ( ',' schema_cident )* ')') )
                     ( K_USING K_TIMESTAMP INTEGER)? )
          | K_WITH  properties
          | K_RENAME
               schema_cident K_TO schema_cident
               ( K_AND schema_cident K_TO schema_cident )*
          )
    ;

cfisStatic
    : (K_STATIC)?
    ;

alterMaterializedViewStatement
    : K_ALTER K_MATERIALIZED K_VIEW columnFamilyName
          K_WITH properties
    ;


/**
 * ALTER TYPE <name> ALTER <field> TYPE <newtype>;
 * ALTER TYPE <name> ADD <field> <newtype>;
 * ALTER TYPE <name> RENAME <field> TO <newtype> AND ...;
 */
alterTypeStatement
    : K_ALTER K_TYPE userTypeName
          ( K_ALTER fident K_TYPE comparatorType
          | K_ADD   fident comparatorType
          | K_RENAME renamedColumns
          )
    ;

renamedColumns
    : fident K_TO fident ( K_AND fident K_TO fident )*
    ;

/**
 * DROP KEYSPACE [IF EXISTS] <KSP>;
 */
dropKeyspaceStatement
    : K_DROP K_KEYSPACE (K_IF K_EXISTS )? keyspaceName
    ;

/**
 * DROP COLUMNFAMILY [IF EXISTS] <CF>;
 */
dropTableStatement
    : K_DROP K_COLUMNFAMILY (K_IF K_EXISTS)? columnFamilyName
    ;

/**
 * DROP TYPE <name>;
 */
dropTypeStatement
    : K_DROP K_TYPE (K_IF K_EXISTS)? userTypeName
    ;

/**
 * DROP INDEX [IF EXISTS] <INDEX_NAME>
 */
dropIndexStatement
    : K_DROP K_INDEX (K_IF K_EXISTS )? indexName
    ;

/**
 * DROP MATERIALIZED VIEW [IF EXISTS] <view_name>
 */
dropMaterializedViewStatement
    : K_DROP K_MATERIALIZED K_VIEW (K_IF K_EXISTS )? columnFamilyName
    ;

/**
  * TRUNCATE <CF>;
  */
truncateStatement
    : K_TRUNCATE (K_COLUMNFAMILY)? columnFamilyName
    ;

/**
 * GRANT <permission> ON <resource> TO <rolename>
 */
grantPermissionsStatement
    : K_GRANT
          ( K_AUTHORIZE K_FOR )?
          permissionOrAll
      K_ON
          ( resourceFromInternalName | resource )
      K_TO
          userOrRoleName
    ;

/**
 * REVOKE <permission> ON <resource> FROM <rolename>
 */
revokePermissionsStatement
    : K_REVOKE
          ( K_AUTHORIZE K_FOR )?
          permissionOrAll
      K_ON
          ( resourceFromInternalName | resource )
      K_FROM
          userOrRoleName
    ;

/**
 * GRANT <permission> ON <resource> TO <rolename>
 */
restrictPermissionsStatement
    : K_RESTRICT
          permissionOrAll
      K_ON
          ( resourceFromInternalName | resource )
      K_TO
          userOrRoleName
    ;

/**
 * REVOKE <permission> ON <resource> FROM <rolename>
 */
unrestrictPermissionsStatement
    : K_UNRESTRICT
          permissionOrAll
      K_ON
          ( resourceFromInternalName | resource )
      K_FROM
          userOrRoleName
    ;

/**
 * GRANT ROLE <rolename> TO <grantee>
 */
grantRoleStatement
    : K_GRANT
          userOrRoleName
      K_TO
          userOrRoleName
    ;

/**
 * REVOKE ROLE <rolename> FROM <revokee>
 */
revokeRoleStatement
    : K_REVOKE
          userOrRoleName
      K_FROM
          userOrRoleName
    ;

listPermissionsStatement
    : K_LIST
          permissionOrAll
      ( K_ON resource )?
      ( K_OF roleName )?
      ( K_NORECURSIVE )?
    ;

permissionDomain
    : IDENT               #permissionDomainIdent
    | STRING_LITERAL      #permissionDomainStringLiteral
    | QUOTED_NAME         #permissionDomainQuotedName
    | unreserved_keyword  #permissionDomainUnreservedKeyword
    ;

permissionName
    : IDENT              #permissionNameIdent
    | STRING_LITERAL     #permissionNameStringLiteral
    | QUOTED_NAME        #permissionNameQuotedName
    | corePermissionName #permissionNameCorePermissionName
    | unreserved_keyword #permissionNameUnreservedKeyword
    ;

corePermissionName
    : K_CREATE       #corePermissionNameCreate
    | K_ALTER        #corePermissionNameAlter
    | K_DROP         #corePermissionNameDrop
    | K_SELECT       #corePermissionNameSelect
    | K_MODIFY       #corePermissionNameModify
    | K_AUTHORIZE    #corePermissionNameAuthorize
    | K_DESCRIBE     #corePermissionNameDescribe
    | K_EXECUTE      #corePermissionNameExecute
    ;

permission
    // unnamespaced permissions default to the SYSTEM namespace
    : corePermissionName                    #permissionCorePermissionName
    | permissionDomain '.' permissionName   #permissionDomainName
    ;

permissionOrAll
    : K_ALL ( K_PERMISSIONS )?                                           #permissionOrAllAll
    | K_PERMISSIONS                                                      #permissionOrAllPermissions
    | permission ( K_PERMISSION )? ( ',' permission ( K_PERMISSION )? )* #permissionOrAllPermission
    ;

resourceFromInternalName
    : K_RESOURCE '(' STRING_LITERAL ')'
    ;

cassandraResource
    : dataResource     #resourceData
    | roleResource     #resourceRole
    | functionResource #resourceFunction
    | jmxResource      #resourceJmx
    ;

dataResource
    : K_ALL K_KEYSPACES                    #dataResourceAllKeyspaces
    | K_KEYSPACE keyspaceName              #dataResourceKeyspace
    | ( K_COLUMNFAMILY )? columnFamilyName #dataResourceColumnFamily
    ;

jmxResource
    : K_ALL K_MBEANS #jmxResourceAllMbeans
    // when a bean name (or pattern) is supplied, validate that it's a legal ObjectName
    // also, just to be picky, if the "MBEANS" form is used, only allow a pattern style names
    | K_MBEAN mbean  #jmxResourceMbean
    | K_MBEANS mbean #jmxResourceMbeans
    ;

roleResource
    : K_ALL K_ROLES           #roleResourceAllRoles
    | K_ROLE userOrRoleName   #roleResourceRole
    ;

functionResource
    : K_ALL K_FUNCTIONS                                        #functionResourceAllFunctions
    | K_ALL K_FUNCTIONS K_IN K_KEYSPACE keyspaceName           #functionResourceAllFunctionsInKeyspace
    // Arg types are mandatory for DCL statements on Functions
    | K_FUNCTION functionName
      ( '(' ( comparatorType ( ',' comparatorType )* )? ')' )  #functionResourceFunction
    ;

/**
 * CREATE USER [IF NOT EXISTS] <username> [WITH PASSWORD <password>] [SUPERUSER|NOSUPERUSER]
 */
createUserStatement
    : K_CREATE K_USER (K_IF K_NOT K_EXISTS)? username
      ( K_WITH userPassword )?
      ( K_SUPERUSER | K_NOSUPERUSER )?
    ;

/**
 * ALTER USER <username> [WITH PASSWORD <password>] [SUPERUSER|NOSUPERUSER]
 */
alterUserStatement
    : K_ALTER K_USER username
      ( K_WITH userPassword )?
      ( K_SUPERUSER
        | K_NOSUPERUSER ) ?
    ;

/**
 * DROP USER [IF EXISTS] <username>
 */
dropUserStatement
    : K_DROP K_USER (K_IF K_EXISTS)? username
    ;

/**
 * LIST USERS
 */
listUsersStatement
    : K_LIST K_USERS
    ;

/**
 * CREATE ROLE [IF NOT EXISTS] <rolename> [ [WITH] option [ [AND] option ]* ]
 *
 * where option can be:
 *  PASSWORD = '<password>'
 *  SUPERUSER = (true|false)
 *  LOGIN = (true|false)
 *  OPTIONS = { 'k1':'v1', 'k2':'v2'}
 */
createRoleStatement
    : K_CREATE K_ROLE (K_IF K_NOT K_EXISTS)? userOrRoleName
      ( K_WITH roleOptions )?
    ;

/**
 * ALTER ROLE <rolename> [ [WITH] option [ [AND] option ]* ]
 *
 * where option can be:
 *  PASSWORD = '<password>'
 *  SUPERUSER = (true|false)
 *  LOGIN = (true|false)
 *  OPTIONS = { 'k1':'v1', 'k2':'v2'}
 */
alterRoleStatement
    : K_ALTER K_ROLE userOrRoleName
      ( K_WITH roleOptions )?
    ;

/**
 * DROP ROLE [IF EXISTS] <rolename>
 */
dropRoleStatement
    : K_DROP K_ROLE (K_IF K_EXISTS)? userOrRoleName
    ;

/**
 * LIST ROLES [OF <rolename>] [NORECURSIVE]
 */
listRolesStatement
    : K_LIST K_ROLES
      ( K_OF roleName)?
      ( K_NORECURSIVE )?
    ;

roleOptions
    : roleOption (K_AND roleOption)*
    ;

roleOption
    :  K_PASSWORD '=' STRING_LITERAL  #roleOptionPassword
    |  K_OPTIONS '=' fullMapLiteral   #roleOptionOptions
    |  K_SUPERUSER '=' BOOLEAN        #roleOptionSuperuser
    |  K_LOGIN '=' BOOLEAN            #roleOptionLogin
    ;

// for backwards compatibility in CREATE/ALTER USER, this has no '='
userPassword
    :  K_PASSWORD STRING_LITERAL
    ;

/** DEFINITIONS **/

// Column Identifiers.  These need to be treated differently from other
// identifiers because the underlying comparator is not necessarily text. See
// CASSANDRA-8178 for details.
// Also, we need to support the internal of the super column map (for backward
// compatibility) which is empty (we only want to allow this is in data manipulation
// queries, not in schema defition etc).
cident
    : EMPTY_QUOTED_NAME   #cidentEmpty
    | IDENT               #cidentSimple
    | QUOTED_NAME         #cidentQuoted
    | unreserved_keyword  #cidentUnreservedKeyword
    ;

schema_cident
    : IDENT               #schemaCidentSimple
    | QUOTED_NAME         #schemaCidentQuoted
    | unreserved_keyword  #schemaCidentUnreservedKeyword
    ;

// Column identifiers where the comparator is known to be text
ident
    : IDENT               #identSimple
    | QUOTED_NAME         #identQuoted
    | unreserved_keyword  #identUnreservedKeyword
    ;

fident
    : IDENT               #fidentSimple
    | QUOTED_NAME         #fidentQuoted
    | unreserved_keyword  #fidentUnreservedKeyword
    ;

// Identifiers that do not refer to columns
noncol_ident
    : IDENT               #nonColIdentSimple
    | QUOTED_NAME         #nonColIdentQuoted
    | unreserved_keyword  #nonColIdentUnreservedKeyword
    ;

// Keyspace & Column family names
keyspaceName
        : ksName
    ;

indexName
        : (ksName '.')? idxName
    ;

columnFamilyName
        : (ksName '.')? cfName
    ;

userTypeName
    : (noncol_ident '.')? non_type_ident
    ;

userOrRoleName
        : roleName
    ;

ksName
    : IDENT                #ksNameSimple
    | QUOTED_NAME          #ksNameQuoted
    | unreserved_keyword   #ksNameUnreservedKeyword
    | QMARK                #ksNameQuestionMark
    ;

cfName
    : IDENT                #cfNameSimple
    | QUOTED_NAME          #cfNameQuoted
    | unreserved_keyword   #cfNameUnreservedKeyword
    | QMARK                #cfNameQuestionMark
    ;

idxName
    : IDENT                #idxNameSimple
    | QUOTED_NAME          #idxNameQuoted
    | unreserved_keyword   #idxNameUnreservedKeyword
    | QMARK                #idxNameQuestionMark
    ;

roleName
    : IDENT                #roleNameSimple
    | STRING_LITERAL       #roleNameStringLiteral
    | QUOTED_NAME          #roleNameQuoted
    | unreserved_keyword   #roleNameUnreservedKeyword
    | QMARK                #roleNameQuestionMark
    ;

constant
    : STRING_LITERAL                       #constantStringLiteral
    | INTEGER                              #constantInteger
    | FLOAT                                #constantFloat
    | BOOLEAN                              #constantBoolean
    | DURATION                             #constantDuration
    | UUID                                 #constantUUID
    | HEXNUMBER                            #constantHexNumber
    | ((K_POSITIVE_NAN | K_NEGATIVE_NAN)
        | K_POSITIVE_INFINITY
        | K_NEGATIVE_INFINITY)             #constantNanInfinity
    ;

fullMapLiteral
    : '{' ( term ':' term ( ',' term ':' term )* )? '}'
    ;

setOrMapLiteral
    : mapLiteral  #setOrMapLiteralMap
    | setLiteral  #setOrMapLiteralSet
    ;

setLiteral
    : ( ',' term )*
    ;

mapLiteral
    : ':' term ( ',' term ':' term )*
    ;

collectionLiteral
    : listLiteral                     #collectionLiteralList
    | '{' term setOrMapLiteral '}'    #collectionLiteralSetOrMap
    // Note that we have an ambiguity between maps and set for "{}". So we force it to a set literal,
    // and deal with it later based on the type of the column (SetLiteral.java).
    | '{' '}'                         #collectionLiteralSetOrMapEmpty
    ;

listLiteral
    : '[' ( term ( ',' term )* )? ']'
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
    | ':' noncol_ident      #valueNonColIdent
    | QMARK                 #valueQuestionMark
    ;

intValue
    : INTEGER            #intValueSimple
    | ':' noncol_ident   #intValueNonColIdent
    | QMARK              #intValueQuestionMark
    ;

functionName
     // antlr might try to recover and give a null for f. It will still error out in the end, but FunctionName
     // wouldn't be happy with that so we should bypass this for now or we'll have a weird user-facing error
    : (keyspaceName '.')? allowedFunctionName
    ;

allowedFunctionName
    : IDENT                        #allowedFunctionNameIdent
    | QUOTED_NAME                  #allowedFunctionNameQuotedName
    | unreserved_function_keyword  #allowedFunctionNameUnreservedKeywork
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
    : termAddition
    ;

termAddition
    :   termMultiplication
        ( '+' termMultiplication
        | '-' termMultiplication
        )*
    ;

termMultiplication
    :   termGroup
        ( '*' termGroup
        | '/' termGroup
        | '%' termGroup
        )*
    ;

termGroup
    : simpleTerm       #termGroupSimple
    | '-'  simpleTerm  #termGroupNegative
    ;

simpleTerm
    : value                             #simpleTermValue
    | function                          #simpleTermFunction
    | '(' comparatorType ')' simpleTerm #simpleTermTyped
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

properties
    : property (K_AND property)*
    ;

property
    : noncol_ident '=' propertyValue  #propertyPropertyValue
    | noncol_ident '=' fullMapLiteral #propertyFullMapLiteral
    ;

propertyValue
    : constant           #propertyValueConstant
    | unreserved_keyword #propertyValueUnreservedKeyword
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
    | cident containsOperator term                 #relationContains
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

containsOperator
    : K_CONTAINS (K_KEY)?
    ;

inMarker
    : QMARK
    | ':' noncol_ident
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
    : QMARK
    | ':' noncol_ident
    ;

tupleOfMarkersForTuples
        : '(' markerForTuple (',' markerForTuple)* ')'
    ;

inMarkerForTuple
    : QMARK
    | ':' noncol_ident
    ;

comparatorType
    : native_type
    | collection_type
    | tuple_type
    | userTypeName
    | K_FROZEN '<' comparatorType '>'
    | STRING_LITERAL
    ;

native_type
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

collection_type
    : K_MAP  '<' comparatorType ',' comparatorType '>'
    | K_LIST '<' comparatorType '>'
    | K_SET  '<' comparatorType '>'
    ;

tuple_type
    : K_TUPLE '<' comparatorType (',' comparatorType)* '>'
    ;

username
    : IDENT
    | STRING_LITERAL
    | QUOTED_NAME
    ;

mbean
    : STRING_LITERAL
    ;

// Basically the same as cident, but we need to exlude existing CQL3 types
// (which for some reason are not reserved otherwise)
non_type_ident
    : IDENT
    | QUOTED_NAME
    | basic_unreserved_keyword
    | K_KEY
    ;

unreserved_keyword
    : unreserved_function_keyword
    | (K_TTL | K_COUNT | K_WRITETIME | K_KEY | K_CAST | K_JSON | K_DISTINCT)
    ;

unreserved_function_keyword
    : basic_unreserved_keyword
    | native_type
    ;

basic_unreserved_keyword
    : ( K_KEYS
        | K_AS
        | K_CLUSTERING
        | K_COMPACT
        | K_STORAGE
        | K_TYPE
        | K_VALUES
        | K_MAP
        | K_LIST
        | K_FILTERING
        | K_PERMISSION
        | K_PERMISSIONS
        | K_KEYSPACES
        | K_ALL
        | K_USER
        | K_USERS
        | K_ROLE
        | K_ROLES
        | K_SUPERUSER
        | K_NOSUPERUSER
        | K_LOGIN
        | K_NOLOGIN
        | K_OPTIONS
        | K_PASSWORD
        | K_EXISTS
        | K_CUSTOM
        | K_TRIGGER
        | K_CONTAINS
        | K_STATIC
        | K_FROZEN
        | K_TUPLE
        | K_FUNCTION
        | K_FUNCTIONS
        | K_AGGREGATE
        | K_SFUNC
        | K_STYPE
        | K_FINALFUNC
        | K_INITCOND
        | K_RETURNS
        | K_LANGUAGE
        | K_CALLED
        | K_INPUT
        | K_LIKE
        | K_PER
        | K_PARTITION
        | K_GROUP
        | K_RESOURCE
        )
    ;


// LEXER


// Case-insensitive keywords
// When adding a new reserved keyword, add entry to o.a.c.cql3.ReservedKeywords as well
// When adding a new unreserved keyword, add entry to unreserved keywords in Parser.g
K_SELECT:      S E L E C T;
K_FROM:        F R O M;
K_AS:          A S;
K_WHERE:       W H E R E;
K_AND:         A N D;
K_KEY:         K E Y;
K_KEYS:        K E Y S;
K_ENTRIES:     E N T R I E S;
K_FULL:        F U L L;
K_INSERT:      I N S E R T;
K_UPDATE:      U P D A T E;
K_WITH:        W I T H;
K_LIMIT:       L I M I T;
K_PER:         P E R;
K_PARTITION:   P A R T I T I O N;
K_USING:       U S I N G;
K_USE:         U S E;
K_DISTINCT:    D I S T I N C T;
K_COUNT:       C O U N T;
K_SET:         S E T;
K_BEGIN:       B E G I N;
K_UNLOGGED:    U N L O G G E D;
K_BATCH:       B A T C H;
K_APPLY:       A P P L Y;
K_TRUNCATE:    T R U N C A T E;
K_DELETE:      D E L E T E;
K_IN:          I N;
K_CREATE:      C R E A T E;
K_KEYSPACE:    ( K E Y S P A C E
                 | S C H E M A );
K_KEYSPACES:   K E Y S P A C E S;
K_COLUMNFAMILY:( C O L U M N F A M I L Y
                 | T A B L E );
K_MATERIALIZED:M A T E R I A L I Z E D;
K_VIEW:        V I E W;
K_INDEX:       I N D E X;
K_CUSTOM:      C U S T O M;
K_ON:          O N;
K_TO:          T O;
K_DROP:        D R O P;
K_PRIMARY:     P R I M A R Y;
K_INTO:        I N T O;
K_VALUES:      V A L U E S;
K_TIMESTAMP:   T I M E S T A M P;
K_TTL:         T T L;
K_CAST:        C A S T;
K_ALTER:       A L T E R;
K_RENAME:      R E N A M E;
K_ADD:         A D D;
K_TYPE:        T Y P E;
K_COMPACT:     C O M P A C T;
K_STORAGE:     S T O R A G E;
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

K_GRANT:       G R A N T;
K_ALL:         A L L;
K_PERMISSION:  P E R M I S S I O N;
K_PERMISSIONS: P E R M I S S I O N S;
K_OF:          O F;
K_REVOKE:      R E V O K E;
K_MODIFY:      M O D I F Y;
K_AUTHORIZE:   A U T H O R I Z E;
K_DESCRIBE:    D E S C R I B E;
K_EXECUTE:     E X E C U T E;
K_NORECURSIVE: N O R E C U R S I V E;
K_MBEAN:       M B E A N;
K_MBEANS:      M B E A N S;
K_RESOURCE:    R E S O U R C E;
K_FOR:         F O R;
K_RESTRICT:    R E S T R I C T;
K_UNRESTRICT:  U N R E S T R I C T;

K_USER:        U S E R;
K_USERS:       U S E R S;
K_ROLE:        R O L E;
K_ROLES:       R O L E S;
K_SUPERUSER:   S U P E R U S E R;
K_NOSUPERUSER: N O S U P E R U S E R;
K_PASSWORD:    P A S S W O R D;
K_LOGIN:       L O G I N;
K_NOLOGIN:     N O L O G I N;
K_OPTIONS:     O P T I O N S;

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
K_POSITIVE_NAN: N A N;
K_NEGATIVE_NAN: '-' N A N;
K_POSITIVE_INFINITY:    I N F I N I T Y;
K_NEGATIVE_INFINITY: '-' I N F I N I T Y;
K_TUPLE:       T U P L E;

K_TRIGGER:     T R I G G E R;
K_STATIC:      S T A T I C;
K_FROZEN:      F R O Z E N;

K_FUNCTION:    F U N C T I O N;
K_FUNCTIONS:   F U N C T I O N S;
K_AGGREGATE:   A G G R E G A T E;
K_SFUNC:       S F U N C;
K_STYPE:       S T Y P E;
K_FINALFUNC:   F I N A L F U N C;
K_INITCOND:    I N I T C O N D;
K_RETURNS:     R E T U R N S;
K_CALLED:      C A L L E D;
K_INPUT:       I N P U T;
K_LANGUAGE:    L A N G U A G E;
K_OR:          O R;
K_REPLACE:     R E P L A C E;

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

RANGE
    : '..'
    ;

/*
 * Normally a lexer only emits one token at a time, but ours is tricked out
 * to support multiple (see @lexer::members near the top of the grammar).
 */
FLOAT
    : /*(INTEGER '.' RANGE) =>*/ INTEGER '.'
    | /*(INTEGER RANGE) =>*/ INTEGER
    | INTEGER ('.' DIGIT*)? EXPONENT?
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
    : (' ' | '\t' | '\n' | '\r')+  -> channel(HIDDEN)
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
