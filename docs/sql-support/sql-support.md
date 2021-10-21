# SQL Support

The SQL interpreter uses ANTLR4 in order to generate a parse tree and evaluate the given SQL statements within the CLI application.

In order to achieve this, ANTLR4 requires a lexer file and a grammar file in order to work. The lexer file can be found in `sqlparser/parser/SQLLexerRules.g4` directory and the grammar rules can be found at `sqlparser/parser/SQLBase.g4` directory.

The SQL grammar is mainly based on Facebook Presto DB. Currently the SQL support is limited to the following Data Manipulation Language since market data is a structured data and highly likely it will stay this way (but contribution is highly recommended if you want to expand the current support):

- [SELECT statement](./select-statement.md)
- [INSERT statement](./insert-statement.md)
- 
