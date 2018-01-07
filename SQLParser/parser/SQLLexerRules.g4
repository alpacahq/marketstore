lexer grammar SQLLexerRules;

SEMICOLON : ';';

SELECT: [Ss][Ee][Ll][Ee][Cc][Tt];
FROM: [Ff][Rr][Oo][Mm];
ADD:  [Aa][Dd][Dd];
AS:  [Aa][Ss];
ALL:  [Aa][Ll][Ll];
SOME:  [Ss][Oo][Mm][Ee];
ANY:  [Aa][Nn][Yy];
DISTINCT:  [Dd][Ii][Ss][Tt][Ii][Nn][Cc][Tt];
WHERE:  [Ww][Hh][Ee][Rr][Ee];
GROUP:  [Gg][Rr][Oo][Uu][Pp];
BY:  [Bb][Yy];
GROUPING:  [Gg][Rr][Oo][Uu][Pp][Ii][Nn][Gg];
SETS:  [Ss][Ee][Tt][Ss];
CUBE:  [Cc][Uu][Bb][Ee];
ROLLUP:  [Rr][Oo][Ll][Ll][Uu][Pp];
ORDER:  [Oo][Rr][Dd][Ee][Rr];
HAVING:  [Hh][Aa][Vv][Ii][Nn][Gg];
LIMIT:  [Ll][Ii][Mm][Ii][Tt];
AT:  [Aa][Tt];
OR:  [Oo][Rr];
AND:  [Aa][Nn][Dd];
IN:  [Ii][Nn];
NOT:  [Nn][Oo][Tt];
NO:  [Nn][Oo];
EXISTS:  [Ee][Xx][Ii][Ss][Tt][Ss];
BETWEEN:  [Bb][Ee][Tt][Ww][Ee][Ee][Nn];
LIKE:  [Ll][Ii][Kk][Ee];
IS:  [Ii][Ss];
NULL:  [Nn][Uu][Ll][Ll];
TRUE:  [Tt][Rr][Uu][Ee];
FALSE:  [Ff][Aa][Ll][Ss][Ee];
NULLS:  [Nn][Uu][Ll][Ll][Ss];
FIRST:  [Ff][Ii][Rr][Ss][Tt];
LAST:  [Ll][Aa][Ss][Tt];
ESCAPE:  [Ee][Ss][Cc][Aa][Pp][Ee];
ASC:  [Aa][Ss][Cc];
DESC:  [Dd][Ee][Ss][Cc];
SUBSTRING:  [Ss][Uu][Bb][Ss][Tt][Rr][Ii][Nn][Gg];
POSITION:  [Pp][Oo][Ss][Ii][Tt][Ii][Oo][Nn];
FOR:  [Ff][Oo][Rr];
TINYINT:  [Tt][Ii][Nn][Yy][Ii][Nn][Tt];
SMALLINT:  [Ss][Mm][Aa][Ll][Ll][Ii][Nn][Tt];
INTEGER:  [Ii][Nn][Tt][Ee][Gg][Ee][Rr];
DATE:  [Dd][Aa][Tt][Ee];
TIME:  [Tt][Ii][Mm][Ee];
TIMESTAMP:  [Tt][Ii][Mm][Ee][Ss][Tt][Aa][Mm][Pp];
INTERVAL:  [Ii][Nn][Tt][Ee][Rr][Vv][Aa][Ll];
YEAR:  [Yy][Ee][Aa][Rr];
MONTH:  [Mm][Oo][Nn][Tt][Hh];
DAY:  [Dd][Aa][Yy];
HOUR:  [Hh][Oo][Uu][Rr];
MINUTE:  [Mm][Ii][Nn][Uu][Tt][Ee];
SECOND:  [Ss][Ee][Cc][Oo][Nn][Dd];
ZONE:  [Zz][Oo][Nn][Ee];
CURRENT_DATE:  [Cc][Uu][Rr][Rr][Ee][Nn][Tt]'_'[Dd][Aa][Tt][Ee];
CURRENT_TIME:  [Cc][Uu][Rr][Rr][Ee][Nn][Tt]'_'[Tt][Ii][Mm][Ee];
CURRENT_TIMESTAMP:  [Cc][Uu][Rr][Rr][Ee][Nn][Tt]'_'[Tt][Ii][Mm][Ee][Ss][Tt][Aa][Mm][Pp];
LOCALTIME:  [Ll][Oo][Cc][Aa][Ll][Tt][Ii][Mm][Ee];
LOCALTIMESTAMP:  [Ll][Oo][Cc][Aa][Ll][Tt][Ii][Mm][Ee][Ss][Tt][Aa][Mm][Pp];
EXTRACT:  [Ee][Xx][Tt][Rr][Aa][Cc][Tt];
CASE:  [Cc][Aa][Ss][Ee];
WHEN:  [Ww][Hh][Ee][Nn];
THEN:  [Tt][Hh][Ee][Nn];
ELSE:  [Ee][Ll][Ss][Ee];
END:  [Ee][Nn][Dd];
JOIN:  [Jj][Oo][Ii][Nn];
CROSS:  [Cc][Rr][Oo][Ss][Ss];
OUTER:  [Oo][Uu][Tt][Ee][Rr];
INNER:  [Ii][Nn][Nn][Ee][Rr];
LEFT:  [Ll][Ee][Ff][Tt];
RIGHT:  [Rr][Ii][Gg][Hh][Tt];
FULL:  [Ff][Uu][Ll][Ll];
NATURAL:  [Nn][Aa][Tt][Uu][Rr][Aa][Ll];
USING:  [Uu][Ss][Ii][Nn][Gg];
ON:  [Oo][Nn];
FILTER:  [Ff][Ii][Ll][Tt][Ee][Rr];
OVER:  [Oo][Vv][Ee][Rr];
PARTITION:  [Pp][Aa][Rr][Tt][Ii][Tt][Ii][Oo][Nn];
RANGE:  [Rr][Aa][Nn][Gg][Ee];
ROWS:  [Rr][Oo][Ww][Ss];
UNBOUNDED:  [Uu][Nn][Bb][Oo][Uu][Nn][Dd][Ee][Dd];
PRECEDING:  [Pp][Rr][Ee][Cc][Ee][Dd][Ii][Nn][Gg];
FOLLOWING:  [Ff][Oo][Ll][Ll][Oo][Ww][Ii][Nn][Gg];
CURRENT:  [Cc][Uu][Rr][Rr][Ee][Nn][Tt];
ROW:  [Rr][Oo][Ww];
WITH:  [Ww][Ii][Tt][Hh];
RECURSIVE:  [Rr][Ee][Cc][Uu][Rr][Ss][Ii][Vv][Ee];
VALUES:  [Vv][Aa][Ll][Uu][Ee][Ss];
CREATE:  [Cc][Rr][Ee][Aa][Tt][Ee];
SCHEMA:  [Ss][Cc][Hh][Ee][Mm][Aa];
TABLE:  [Tt][Aa][Bb][Ll][Ee];
COMMENT:  [Cc][Oo][Mm][Mm][Ee][Nn][Tt];
VIEW:  [Vv][Ii][Ee][Ww];
REPLACE:  [Rr][Ee][Pp][Ll][Aa][Cc][Ee];
INSERT:  [Ii][Nn][Ss][Ee][Rr][Tt];
DELETE:  [Dd][Ee][Ll][Ee][Tt][Ee];
INTO:  [Ii][Nn][Tt][Oo];
CONSTRAINT:  [Cc][Oo][Nn][Ss][Tt][Rr][Aa][Ii][Nn][Tt];
DESCRIBE:  [Dd][Ee][Ss][Cc][Rr][Ii][Bb][Ee];
GRANT:  [Gg][Rr][Aa][Nn][Tt];
REVOKE:  [Rr][Ee][Vv][Oo][Kk][Ee];
PRIVILEGES:  [Pp][Rr][Ii][Vv][Ii][Ll][Ee][Gg][Ee][Ss];
PUBLIC:  [Pp][Uu][Bb][Ll][Ii][Cc];
OPTION:  [Oo][Pp][Tt][Ii][Oo][Nn];
EXPLAIN:  [Ee][Xx][Pp][Ll][Aa][Ii][Nn];
ANALYZE:  [Aa][Nn][Aa][Ll][Yy][Zz][Ee];
FORMAT:  [Ff][Oo][Rr][Mm][Aa][Tt];
TYPE:  [Tt][Yy][Pp][Ee];
TEXT:  [Tt][Ee][Xx][Tt];
GRAPHVIZ:  [Gg][Rr][Aa][Pp][Hh][Vv][Ii][Zz];
LOGICAL:  [Ll][Oo][Gg][Ii][Cc][Aa][Ll];
DISTRIBUTED:  [Dd][Ii][Ss][Tt][Rr][Ii][Bb][Uu][Tt][Ee][Dd];
VALIDATE:  [Vv][Aa][Ll][Ii][Dd][Aa][Tt][Ee];
CAST:  [Cc][Aa][Ss][Tt];
TRY_CAST:  [Tt][Rr][Yy]'_'[Cc][Aa][Ss][Tt];
SHOW:  [Ss][Hh][Oo][Ww];
TABLES:  [Tt][Aa][Bb][Ll][Ee][Ss];
SCHEMAS:  [Ss][Cc][Hh][Ee][Mm][Aa][Ss];
CATALOGS:  [Cc][Aa][Tt][Aa][Ll][Oo][Gg][Ss];
COLUMNS:  [Cc][Oo][Ll][Uu][Mm][Nn][Ss];
COLUMN:  [Cc][Oo][Ll][Uu][Mm][Nn];
USE:  [Uu][Ss][Ee];
PARTITIONS:  [Pp][Aa][Rr][Tt][Ii][Tt][Ii][Oo][Nn][Ss];
FUNCTIONS:  [Ff][Uu][Nn][Cc][Tt][Ii][Oo][Nn][Ss];
DROP:  [Dd][Rr][Oo][Pp];
UNION:  [Uu][Nn][Ii][Oo][Nn];
EXCEPT:  [Ee][Xx][Cc][Ee][Pp][Tt];
INTERSECT:  [Ii][Nn][Tt][Ee][Rr][Ss][Ee][Cc][Tt];
TO:  [Tt][Oo];
SYSTEM:  [Ss][Yy][Ss][Tt][Ee][Mm];
BERNOULLI:  [Bb][Ee][Rr][Nn][Oo][Uu][Ll][Ll][Ii];
POISSONIZED:  [Pp][Oo][Ii][Ss][Ss][Oo][Nn][Ii][Zz][Ee][Dd];
TABLESAMPLE:  [Tt][Aa][Bb][Ll][Ee][Ss][Aa][Mm][Pp][Ll][Ee];
ALTER:  [Aa][Ll][Tt][Ee][Rr];
RENAME:  [Rr][Ee][Nn][Aa][Mm][Ee];
UNNEST:  [Uu][Nn][Nn][Ee][Ss][Tt];
ORDINALITY:  [Oo][Rr][Dd][Ii][Nn][Aa][Ll][Ii][Tt][Yy];
ARRAY:  [Aa][Rr][Rr][Aa][Yy];
MAP:  [Mm][Aa][Pp];
SET:  [Ss][Ee][Tt];
RESET:  [Rr][Ee][Ss][Ee][Tt];
SESSION:  [Ss][Ee][Ss][Ss][Ii][Oo][Nn];
DATA:  [Dd][Aa][Tt][Aa];
START:  [Ss][Tt][Aa][Rr][Tt];
TRANSACTION:  [Tt][Rr][Aa][Nn][Ss][Aa][Cc][Tt][Ii][Oo][Nn];
COMMIT:  [Cc][Oo][Mm][Mm][Ii][Tt];
ROLLBACK:  [Rr][Oo][Ll][Ll][Bb][Aa][Cc][Kk];
WORK:  [Ww][Oo][Rr][Kk];
ISOLATION:  [Ii][Ss][Oo][Ll][Aa][Tt][Ii][Oo][Nn];
LEVEL:  [Ll][Ee][Vv][Ee][Ll];
SERIALIZABLE:  [Ss][Ee][Rr][Ii][Aa][Ll][Ii][Zz][Aa][Bb][Ll][Ee];
REPEATABLE:  [Rr][Ee][Pp][Ee][Aa][Tt][Aa][Bb][Ll][Ee];
COMMITTED:  [Cc][Oo][Mm][Mm][Ii][Tt][Tt][Ee][Dd];
UNCOMMITTED:  [Uu][Nn][Cc][Oo][Mm][Mm][Ii][Tt][Tt][Ee][Dd];
READ:  [Rr][Ee][Aa][Dd];
WRITE:  [Ww][Rr][Ii][Tt][Ee];
ONLY:  [Oo][Nn][Ll][Yy];
CALL:  [Cc][Aa][Ll][Ll];
PREPARE:  [Pp][Rr][Ee][Pp][Aa][Rr][Ee];
DEALLOCATE:  [Dd][Ee][Aa][Ll][Ll][Oo][Cc][Aa][Tt][Ee];
EXECUTE:  [Ee][Xx][Ee][Cc][Uu][Tt][Ee];
INPUT:  [Ii][Nn][Pp][Uu][Tt];
OUTPUT:  [Oo][Uu][Tt][Pp][Uu][Tt];
CASCADE:  [Cc][Aa][Ss][Cc][Aa][Dd][Ee];
RESTRICT:  [Rr][Ee][Ss][Tt][Rr][Ii][Cc][Tt];
INCLUDING:  [Ii][Nn][Cc][Ll][Uu][Dd][Ii][Nn][Gg];
EXCLUDING:  [Ee][Xx][Cc][Ll][Uu][Dd][Ii][Nn][Gg];
PROPERTIES:  [Pp][Rr][Oo][Pp][Ee][Rr][Tt][Ii][Ee][Ss];

NORMALIZE:  [Nn][Oo][Rr][Mm][Aa][Ll][Ii][Zz][Ee];
NFD:  [Nn][Ff][Dd];
NFC:  [Nn][Ff][Cc];
NFKD:  [Nn][Ff][Kk][Dd];
NFKC:  [Nn][Ff][Kk][Cc];

IF:  [Ii][Ff];
NULLIF:  [Nn][Uu][Ll][Ll][Ii][Ff];
COALESCE: [Cc][Oo][Aa][Ll][Ee][Ss][Cc][Ee];

TIME_WITH_TIME_ZONE
    : [Tt][Ii][Mm][Ee] WS [Ww][Ii][Tt][Hh] WS [Tt][Ii][Mm][Ee] WS [Zz][Oo][Nn][Ee]
    ;

TIMESTAMP_WITH_TIME_ZONE
    : [Tt][Ii][Mm][Ee][Ss][Tt][Aa][Mm][Pp] WS [Ww][Ii][Tt][Hh] WS [Tt][Ii][Mm][Ee] WS [Zz][Oo][Nn][Ee]
    ;

DOUBLE_PRECISION
    : [Dd][Oo][Uu][Bb][Ll][Ee] WS [Pp][Rr][Ee][Cc][Ii][Ss][Ii][Oo][Nn]
    ;

EQ  : '=';
NEQ : '<>' | '!=';
LT  : '<';
LTE : '<=';
GT  : '>';
GTE : '>=';

PLUS: '+';
MINUS: '-';
ASTERISK: '*';
SLASH: '/';
PERCENT: '%';
CONCAT: '||';
DOT: '.';


STRING
    : '\'' ( ~'\'' | '\'\'' )* '\''
    ;

// Note: we allow any character inside the binary literal and validate
// its a correct literal when the AST is being constructed. This
// allows us to provide more meaningful error messages to the user
BINARY_LITERAL
    :  'X\'' (~'\'')* '\''
    ;

INTEGER_VALUE
    : DIGIT+
    ;

DECIMAL_VALUE
    : DIGIT+ '.' DIGIT*
    | '.' DIGIT+
    | DIGIT+ ('.' DIGIT*)? EXPONENT
    | '.' DIGIT+ EXPONENT
    ;

IDENTIFIER
    : (LETTER | '_') (LETTER | DIGIT | '_' | '@' | ':' )*
    ;

DIGIT_IDENTIFIER
    : DIGIT (LETTER | DIGIT | '_' | '@' | ':' )+
    ;

QUOTED_IDENTIFIER
    : '"' ( ~'"' | '""' )* '"'
    ;

BACKQUOTED_IDENTIFIER
    : '`' ( ~'`' | '``' )* '`'
    ;

fragment EXPONENT
    : 'E' [+-]? DIGIT+
    ;

fragment DIGIT
    : [0-9]
    ;

fragment LETTER
    : [A-Za-z]
    ;

SIMPLE_COMMENT
    : '--' ~[\r\n]* '\r'? '\n'? -> channel(HIDDEN)
    ;

BRACKETED_COMMENT
    : '/*' .*? '*/' -> channel(HIDDEN)
    ;

WS
    : [ \r\n\t]+ -> channel(HIDDEN)
    ;

// Catch-all for anything we can't recognize.
// We use this to be able to ignore and recover all the text
// when splitting statements with DelimiterLexer
//UNRECOGNIZED
//    : .
//    ;


