grammar SQLBase;
import SQLLexerRules;

statements
    : statement SEMICOLON
    ;

statement
    : query                                                        #statementDefault
//    | CREATE TABLE (IF NOT EXISTS)? qualifiedName
//        '(' tableElement (',' tableElement)* ')'
//        (WITH tableProperties)?                                    #createTable
//    | CREATE TABLE (IF NOT EXISTS)? qualifiedName
//        (WITH tableProperties)? AS query                           #createTableAsSelect
//    | DROP TABLE (IF EXISTS)? qualifiedName                        #dropTable
    | INSERT INTO qualifiedName columnAliases? query               #insertInto
//    | DELETE FROM qualifiedName (WHERE booleanExpression)?         #delete
//    | ALTER TABLE from=qualifiedName RENAME TO to=qualifiedName    #renameTable
//    | ALTER TABLE tableName=qualifiedName
//        RENAME COLUMN from=identifier TO to=identifier             #renameColumn
//    | ALTER TABLE tableName=qualifiedName
//        ADD COLUMN column=columnDefinition                         #addColumn
//    | CREATE (OR REPLACE)? VIEW qualifiedName (columnAliases)?
//         AS query                                                  #createView
//    | DROP VIEW (IF EXISTS)? qualifiedName                         #dropView
//    | CALL qualifiedName '(' (callArgument
//     (',' callArgument)*)? ')'                                     #call
//    | GRANT
//        (privilege (',' privilege)* | ALL PRIVILEGES)
//        ON TABLE? qualifiedName TO grantee=identifier
//        (WITH GRANT OPTION)?                                       #grant
//    | REVOKE
//        (GRANT OPTION FOR)?
//        (privilege (',' privilege)* | ALL PRIVILEGES)
//        ON TABLE? qualifiedName FROM grantee=identifier            #revoke
    | EXPLAIN ANALYZE?
        ('(' explainOption (',' explainOption)* ')')? statement    #explain
//    | SHOW CREATE TABLE qualifiedName                              #showCreateTable
//    | SHOW CREATE VIEW qualifiedName                               #showCreateView
//    | SHOW TABLES ((FROM | IN) qualifiedName)?
//     (LIKE pattern=STRING)?                                        #showTables
//    | SHOW SCHEMAS ((FROM | IN) identifier)?
//     (LIKE pattern=STRING)?                                        #showSchemas
//    | SHOW CATALOGS (LIKE pattern=STRING)?                         #showCatalogs
//    | SHOW COLUMNS (FROM | IN) qualifiedName                       #showColumns
//    | (DESCRIBE | DESC) qualifiedName                              #showColumns
//    | SHOW FUNCTIONS                                               #showFunctions
//    | SHOW SESSION                                                 #showSession
//    | SET SESSION qualifiedName EQ expression                      #setSession
//    | RESET SESSION qualifiedName                                  #resetSession
//    | START TRANSACTION (transactionMode (',' transactionMode)*)?  #startTransaction
//    | COMMIT WORK?                                                 #commit
//    | ROLLBACK WORK?                                               #rollback
//    | SHOW PARTITIONS (FROM | IN) qualifiedName
//        (WHERE booleanExpression)?
//        (ORDER BY sortItem (',' sortItem)*)?
//        (LIMIT limit=INTEGER_VALUE)?                               #showPartitions
//    | PREPARE identifier FROM statement                            #prepare
//    | DEALLOCATE PREPARE identifier                                #deallocate
//    | EXECUTE identifier (USING expression (',' expression)*)?     #execute
//    | DESCRIBE INPUT identifier                                    #describeInput
//    | DESCRIBE OUTPUT identifier                                   #describeOutput
    ;

query
    :  with? queryNoWith
    ;

with
    : WITH RECURSIVE? namedQuery (',' namedQuery)*
    ;

//tableElement
//    : columnDefinition
//    | likeClause
//    ;

//columnDefinition
//    : identifier type_t (COMMENT STRING)?
//    ;

//likeClause
//    : LIKE qualifiedName (optionType=(INCLUDING | EXCLUDING) PROPERTIES)?
//    ;

//tableProperties
//    : '(' tableProperty (',' tableProperty)* ')'
//    ;

//tableProperty
//    : identifier EQ expression
//    ;

queryNoWith:
      queryTerm
      (ORDER BY sortItem (',' sortItem)*)?
      (LIMIT limit=INTEGER_VALUE)?
    ;

queryTerm
    : queryPrimary                                                 #queryTermDefault
    | left=queryTerm operator=(INTERSECT | UNION | EXCEPT)
     setQuantifier? right=queryTerm                                #setOperation
    ;

queryPrimary
    : querySpecification                                           #queryPrimaryDefault
    | TABLE qualifiedName                                          #table
    | VALUES expression (',' expression)*                          #inlineTable
    | '(' queryNoWith  ')'                                         #subquery
    ;

sortItem
    : expression ordering=(ASC | DESC)? (NULLS nullOrdering=(FIRST | LAST))?
    ;

querySpecification
    : SELECT setQuantifier? selectItem (',' selectItem)*
      (FROM relation (',' relation)*)?
      (WHERE where=booleanExpression)?
      (GROUP BY groupBy)?
      (HAVING having=booleanExpression)?
    ;

groupBy
    : setQuantifier? groupingElement (',' groupingElement)*
    ;

groupingElement
    : groupingExpressions                                          #singleGroupingSet
    | ROLLUP '(' (qualifiedName (',' qualifiedName)*)? ')'         #rollup
    | CUBE '(' (qualifiedName (',' qualifiedName)*)? ')'           #cube
    | GROUPING SETS '(' groupingSet (',' groupingSet)* ')'         #multipleGroupingSets
    ;

groupingExpressions
    : '(' (expression (',' expression)*)? ')'
    | expression
    ;

groupingSet
    : '(' (qualifiedName (',' qualifiedName)*)? ')'
    | qualifiedName
    ;

namedQuery
    : name=identifier (columnAliases)? AS '(' query ')'
    ;

setQuantifier
    : DISTINCT
    | ALL
    ;

selectItem
    : expression (AS? identifier)?  #selectSingle
    | qualifiedName '.' ASTERISK    #selectAll
    | ASTERISK                      #selectAll
    ;

relation
    : left=relation
    ( joinType? JOIN right=relation joinCriteria? )                #joinRelation
    | sampledRelation                                              #relationDefault
    ;

joinType
    : (INNER | LEFT OUTER | RIGHT OUTER | FULL OUTER | CROSS | NATURAL | LEFT)
    ;

joinCriteria
    : ON booleanExpression
    | USING '(' identifier (',' identifier)* ')'
    ;

sampledRelation
    : aliasedRelation (
        TABLESAMPLE sampletype=(BERNOULLI | SYSTEM | POISSONIZED) '(' percentage=expression ')'
      )?
    ;

aliasedRelation
    : relationPrimary (AS? identifier columnAliases?)?
    ;

columnAliases
    : '(' identifier (',' identifier)* ')'
    ;

relationPrimary
    : qualifiedName                                                #tableName
    | '(' query ')'                                                #subqueryRelation
    | UNNEST '(' expression (',' expression)* ')'
     (WITH ORDINALITY)?                                            #unnest
    | '(' relation ')'                                             #parenthesizedRelation
    ;

expression
    : booleanExpression
    | valueExpression
    ;

booleanExpression
    : valueExpression predicate                                    #booleanDefault
    | NOT booleanExpression                                        #logicalNot
    | left=booleanExpression operator=(AND | OR)
     right=expression                                              #logicalBinary
    | booleanliteral                                               #boolLiteralToo
    ;

booleanliteral: (TRUE | FALSE);

predicate
    : comparisonOperator right=valueExpression                     #comparison
    | comparisonOperator comparisonQuantifier '(' query ')'        #quantifiedComparison
    | NOT? BETWEEN lower=valueExpression
     AND upper=valueExpression                                     #between
    | NOT? IN '(' valueExpression (',' valueExpression)* ')'       #inList
    | NOT? IN '(' query ')'                                        #inSubquery
    | NOT? LIKE pattern=valueExpression
     (ESCAPE escape=valueExpression)?                              #like
    | IS NOT? NULL                                                 #nullPredicate
    | IS NOT? DISTINCT FROM right=valueExpression                  #distinctFrom
    ;

valueExpression
    : primaryExpression                                            #valueExpressionDefault
    | valueExpression AT timeZoneSpecifier                         #atTimeZone
    | operator=(MINUS | PLUS) valueExpression                      #arithmeticUnary
    | left=valueExpression
     operator=(ASTERISK | SLASH | PERCENT | PLUS | MINUS)
      right=valueExpression                                        #arithmeticBinary
    | left=valueExpression CONCAT right=valueExpression            #concatenation
    ;

primaryExpression
    : NULL                                                         #nullLiteral
    | STRING                                                       #stringLiteral
    | BINARY_LITERAL                                               #binaryLiteral
    | DECIMAL_VALUE                                                #decimalLiteral
    | INTEGER_VALUE                                                #integerLiteral
    | booleanliteral                                               #boolLiteral
    | baseType STRING                                              #typedLiteral
    | interval                                                     #intervalLiteral
    | '?'                                                          #parameter
    | (identifier | DOUBLE_PRECISION) STRING                       #typeConstructor
    | POSITION '(' valueExpression IN valueExpression ')'          #position
    | '(' expression (',' expression)+ ')'                         #rowConstructor
    | ROW '(' expression (',' expression)* ')'                     #rowConstructor
    | qualifiedName '(' ASTERISK ')' filter? over?                 #functionCall
    | qualifiedName '(' (setQuantifier? expression
     (',' expression)*)? ')' filter? over?                         #functionCall
    | identifier '->' expression                                   #lambda
    | '(' identifier (',' identifier)* ')' '->' expression         #lambda
    | '(' query ')'                                                #subqueryExpression
    | EXISTS '(' query ')'                                         #exists    // This is an extension to ANSI SQL, which considers EXISTS to be a <boolean expression>
    | CASE valueExpression whenClause+
     (ELSE elseExpression=expression)? END                         #simpleCase
    | CASE whenClause+ (ELSE elseExpression=expression)? END       #searchedCase
    | CAST '(' expression AS type_t ')'                            #cast
    | TRY_CAST '(' expression AS type_t ')'                        #cast
    | ARRAY '[' (expression (',' expression)*)? ']'                #arrayConstructor
    | value=primaryExpression '[' index=valueExpression ']'        #subscript
    | identifier                                                   #columnReference
    | base=primaryExpression '.' fieldName=identifier              #dereference
    | name=CURRENT_DATE                                            #specialDateTimeFunction
    | name=CURRENT_TIME ('(' precision=INTEGER_VALUE ')')?         #specialDateTimeFunction
    | name=CURRENT_TIMESTAMP ('(' precision=INTEGER_VALUE ')')?    #specialDateTimeFunction
    | name=LOCALTIME ('(' precision=INTEGER_VALUE ')')?            #specialDateTimeFunction
    | name=LOCALTIMESTAMP ('(' precision=INTEGER_VALUE ')')?       #specialDateTimeFunction
    | SUBSTRING '(' subterm=valueExpression FROM baseterm=valueExpression
     (FOR forterm=valueExpression)? ')'                            #substring
    | NORMALIZE '(' valueExpression
    (',' normalform=(NFD | NFC | NFKD | NFKC))? ')'                #normalize
    | EXTRACT '(' identifier FROM valueExpression ')'              #extract
    | '(' expression ')'                                           #parenthesizedExpression
    ;

timeZoneSpecifier
    : TIME ZONE interval  #timeZoneInterval
    | TIME ZONE STRING    #timeZoneString
    ;

comparisonOperator
    : EQ | NEQ | LT | LTE | GT | GTE
    ;

comparisonQuantifier
    : ALL | SOME | ANY
    ;

interval
    : INTERVAL sign=(PLUS | MINUS)? STRING from=intervalField (TO to=intervalField)?
    ;

intervalField
    : YEAR | MONTH | DAY | HOUR | MINUTE | SECOND
    ;

type_t
    : type_t ARRAY
    | ARRAY '<' type_t '>'
    | MAP '<' mapelem+=type_t ',' mapelem+=type_t '>'
    | ROW '(' rowidelem+=identifier rowelem+=type_t (',' rowidelem+=identifier rowelem+=type_t)* ')'
    | baseType ('(' typeelem+=typeParameter (',' typeelem+=typeParameter)* ')')?
    ;

typeParameter
    : INTEGER_VALUE | type_t
    ;

baseType
    : TIME_WITH_TIME_ZONE
    | TIMESTAMP_WITH_TIME_ZONE
    | DATE
    | DOUBLE_PRECISION
    | identifier
    ;

whenClause
    : WHEN condition=expression THEN result=expression
    ;

filter
    : FILTER '(' WHERE booleanExpression ')'
    ;

over
   : OVER '('
        (PARTITION BY partition+=expression (',' partition+=expression)*)?
        (ORDER BY orderitem+=sortItem (',' orderitem+=sortItem)*)?
        windowFrame?
      ')'
    ;

windowFrame
    : frameType=(RANGE | ROWS) startFrame=frameBound
    | frameType=(RANGE | ROWS) BETWEEN startFrame=frameBound AND endFrame=frameBound
    ;

frameBound
    : UNBOUNDED boundType=(PRECEDING | FOLLOWING)                  #unboundedFrame
    | CURRENT ROW                                                  #currentRowBound
    | expression boundType=(PRECEDING | FOLLOWING)                 #boundedFrame
    ;

explainOption
    : FORMAT value=(TEXT | GRAPHVIZ)                               #explainFormat
    | TYPE value=(LOGICAL | DISTRIBUTED | VALIDATE)                #explainType
    ;

//transactionMode
//    : ISOLATION LEVEL levelOfIsolation                             #isolationLevel
//    | READ accessMode=(ONLY | WRITE)                               #transactionAccessMode
//    ;

//levelOfIsolation
//    : READ UNCOMMITTED                                             #readUncommitted
//    | READ COMMITTED                                               #readCommitted
//    | REPEATABLE READ                                              #repeatableRead
//    | SERIALIZABLE                                                 #serializable
//    ;

//callArgument
//    : expression                                                   #positionalArgument
//    | identifier '=>' expression                                   #namedArgument
//    ;

//privilege
//    : SELECT | DELETE | INSERT | identifier
//    ;

qualifiedName
    : identifier ('.' identifier)*                                 #dotQualifiedName
    ;
//    : identifier ('.' identifier)*                                 #dotQualifiedName


identifier
    : IDENTIFIER                                                   #unquotedIdentifier
    | DIGIT_IDENTIFIER                                             #digitIdentifier
    | QUOTED_IDENTIFIER                                            #quotedIdentifierAlternative
    | BACKQUOTED_IDENTIFIER                                        #backQuotedIdentifier
    | nonReserved                                                  #nonReservedIdentifier
    ;

nonReserved
    : SHOW | TABLES | COLUMNS | COLUMN | PARTITIONS | FUNCTIONS | SCHEMAS | CATALOGS | SESSION
    | ADD
    | FILTER
    | AT
    | OVER | PARTITION | RANGE | ROWS | PRECEDING | FOLLOWING | CURRENT | ROW | MAP | ARRAY
    | TINYINT | SMALLINT | INTEGER | DATE | TIME | TIMESTAMP | INTERVAL | ZONE
    | YEAR | MONTH | DAY | HOUR | MINUTE | SECOND
    | EXPLAIN | ANALYZE | FORMAT | TYPE | TEXT | GRAPHVIZ | LOGICAL | DISTRIBUTED | VALIDATE
    | TABLESAMPLE | SYSTEM | BERNOULLI | POISSONIZED | USE | TO
    | SET | RESET
    | VIEW | REPLACE
    | IF | NULLIF | COALESCE
    | NFD | NFC | NFKD | NFKC
    | POSITION
    | NO | DATA
    | START | TRANSACTION | COMMIT | ROLLBACK | WORK | ISOLATION | LEVEL
    | SERIALIZABLE | REPEATABLE | COMMITTED | UNCOMMITTED | READ | WRITE | ONLY
    | COMMENT
    | CALL
    | GRANT | REVOKE | PRIVILEGES | PUBLIC | OPTION
    | SUBSTRING
    | SCHEMA | CASCADE | RESTRICT
    | INPUT | OUTPUT
    | INCLUDING | EXCLUDING | PROPERTIES
    | ALL | SOME | ANY
    ;
