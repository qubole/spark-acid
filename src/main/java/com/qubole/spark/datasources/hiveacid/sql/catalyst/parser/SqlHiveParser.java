// Generated from SqlHive.g4 by ANTLR 4.7.2
package com.qubole.spark.datasources.hiveacid.sql.catalyst.parser;
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.*;
import org.antlr.v4.runtime.tree.*;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
public class SqlHiveParser extends Parser {
	static { RuntimeMetaData.checkVersion("4.7.2", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		T__0=1, T__1=2, T__2=3, T__3=4, T__4=5, T__5=6, T__6=7, T__7=8, T__8=9, 
		T__9=10, SELECT=11, FROM=12, ADD=13, AS=14, ALL=15, ANY=16, DISTINCT=17, 
		WHERE=18, GROUP=19, BY=20, GROUPING=21, SETS=22, CUBE=23, ROLLUP=24, ORDER=25, 
		HAVING=26, LIMIT=27, AT=28, OR=29, AND=30, IN=31, NOT=32, NO=33, EXISTS=34, 
		BETWEEN=35, LIKE=36, RLIKE=37, IS=38, NULL=39, TRUE=40, FALSE=41, NULLS=42, 
		ASC=43, DESC=44, FOR=45, INTERVAL=46, CASE=47, WHEN=48, THEN=49, ELSE=50, 
		END=51, JOIN=52, CROSS=53, OUTER=54, INNER=55, LEFT=56, SEMI=57, RIGHT=58, 
		FULL=59, NATURAL=60, ON=61, PIVOT=62, LATERAL=63, WINDOW=64, OVER=65, 
		PARTITION=66, RANGE=67, ROWS=68, UNBOUNDED=69, PRECEDING=70, FOLLOWING=71, 
		CURRENT=72, FIRST=73, AFTER=74, LAST=75, ROW=76, WITH=77, VALUES=78, CREATE=79, 
		TABLE=80, DIRECTORY=81, VIEW=82, REPLACE=83, INSERT=84, UPDATE=85, DELETE=86, 
		INTO=87, DESCRIBE=88, EXPLAIN=89, FORMAT=90, LOGICAL=91, CODEGEN=92, COST=93, 
		CAST=94, SHOW=95, TABLES=96, COLUMNS=97, COLUMN=98, USE=99, PARTITIONS=100, 
		FUNCTIONS=101, DROP=102, UNION=103, EXCEPT=104, SETMINUS=105, INTERSECT=106, 
		TO=107, TABLESAMPLE=108, STRATIFY=109, ALTER=110, RENAME=111, ARRAY=112, 
		MAP=113, STRUCT=114, COMMENT=115, SET=116, RESET=117, DATA=118, START=119, 
		TRANSACTION=120, COMMIT=121, ROLLBACK=122, MACRO=123, IGNORE=124, BOTH=125, 
		LEADING=126, TRAILING=127, IF=128, POSITION=129, EXTRACT=130, EQ=131, 
		NSEQ=132, NEQ=133, NEQJ=134, LT=135, LTE=136, GT=137, GTE=138, PLUS=139, 
		MINUS=140, ASTERISK=141, SLASH=142, PERCENT=143, DIV=144, TILDE=145, AMPERSAND=146, 
		PIPE=147, CONCAT_PIPE=148, HAT=149, PERCENTLIT=150, BUCKET=151, OUT=152, 
		OF=153, SORT=154, CLUSTER=155, DISTRIBUTE=156, OVERWRITE=157, TRANSFORM=158, 
		REDUCE=159, USING=160, SERDE=161, SERDEPROPERTIES=162, RECORDREADER=163, 
		RECORDWRITER=164, DELIMITED=165, FIELDS=166, TERMINATED=167, COLLECTION=168, 
		ITEMS=169, KEYS=170, ESCAPED=171, LINES=172, SEPARATED=173, FUNCTION=174, 
		EXTENDED=175, REFRESH=176, CLEAR=177, CACHE=178, UNCACHE=179, LAZY=180, 
		FORMATTED=181, GLOBAL=182, TEMPORARY=183, OPTIONS=184, UNSET=185, TBLPROPERTIES=186, 
		DBPROPERTIES=187, BUCKETS=188, SKEWED=189, STORED=190, DIRECTORIES=191, 
		LOCATION=192, EXCHANGE=193, ARCHIVE=194, UNARCHIVE=195, FILEFORMAT=196, 
		TOUCH=197, COMPACT=198, CONCATENATE=199, CHANGE=200, CASCADE=201, RESTRICT=202, 
		CLUSTERED=203, SORTED=204, PURGE=205, INPUTFORMAT=206, OUTPUTFORMAT=207, 
		DATABASE=208, DATABASES=209, DFS=210, TRUNCATE=211, ANALYZE=212, COMPUTE=213, 
		LIST=214, STATISTICS=215, PARTITIONED=216, EXTERNAL=217, DEFINED=218, 
		REVOKE=219, GRANT=220, LOCK=221, UNLOCK=222, MSCK=223, REPAIR=224, RECOVER=225, 
		EXPORT=226, IMPORT=227, LOAD=228, ROLE=229, ROLES=230, COMPACTIONS=231, 
		PRINCIPALS=232, TRANSACTIONS=233, INDEX=234, INDEXES=235, LOCKS=236, OPTION=237, 
		ANTI=238, LOCAL=239, INPATH=240, STRING=241, BIGINT_LITERAL=242, SMALLINT_LITERAL=243, 
		TINYINT_LITERAL=244, INTEGER_VALUE=245, DECIMAL_VALUE=246, DOUBLE_LITERAL=247, 
		BIGDECIMAL_LITERAL=248, IDENTIFIER=249, BACKQUOTED_IDENTIFIER=250, SIMPLE_COMMENT=251, 
		BRACKETED_EMPTY_COMMENT=252, BRACKETED_COMMENT=253, WS=254, UNRECOGNIZED=255;
	public static final int
		RULE_singleStatement = 0, RULE_singleExpression = 1, RULE_singleTableIdentifier = 2, 
		RULE_singleFunctionIdentifier = 3, RULE_singleDataType = 4, RULE_singleTableSchema = 5, 
		RULE_statement = 6, RULE_unsupportedHiveNativeCommands = 7, RULE_delete = 8, 
		RULE_update = 9, RULE_updateFieldList = 10, RULE_updateField = 11, RULE_createTableHeader = 12, 
		RULE_bucketSpec = 13, RULE_skewSpec = 14, RULE_locationSpec = 15, RULE_query = 16, 
		RULE_insertInto = 17, RULE_partitionSpecLocation = 18, RULE_partitionSpec = 19, 
		RULE_partitionVal = 20, RULE_describeFuncName = 21, RULE_describeColName = 22, 
		RULE_ctes = 23, RULE_namedQuery = 24, RULE_tableProvider = 25, RULE_tablePropertyList = 26, 
		RULE_tableProperty = 27, RULE_tablePropertyKey = 28, RULE_tablePropertyValue = 29, 
		RULE_constantList = 30, RULE_nestedConstantList = 31, RULE_createFileFormat = 32, 
		RULE_fileFormat = 33, RULE_storageHandler = 34, RULE_resource = 35, RULE_queryNoWith = 36, 
		RULE_queryOrganization = 37, RULE_multiInsertQueryBody = 38, RULE_queryTerm = 39, 
		RULE_queryPrimary = 40, RULE_sortItem = 41, RULE_querySpecification = 42, 
		RULE_hint = 43, RULE_hintStatement = 44, RULE_fromClause = 45, RULE_aggregation = 46, 
		RULE_groupingSet = 47, RULE_pivotClause = 48, RULE_pivotColumn = 49, RULE_pivotValue = 50, 
		RULE_lateralView = 51, RULE_setQuantifier = 52, RULE_relation = 53, RULE_joinRelation = 54, 
		RULE_joinType = 55, RULE_joinCriteria = 56, RULE_sample = 57, RULE_sampleMethod = 58, 
		RULE_identifierList = 59, RULE_identifierSeq = 60, RULE_orderedIdentifierList = 61, 
		RULE_orderedIdentifier = 62, RULE_identifierCommentList = 63, RULE_identifierComment = 64, 
		RULE_relationPrimary = 65, RULE_inlineTable = 66, RULE_functionTable = 67, 
		RULE_tableAlias = 68, RULE_rowFormat = 69, RULE_tableIdentifier = 70, 
		RULE_functionIdentifier = 71, RULE_namedExpression = 72, RULE_namedExpressionSeq = 73, 
		RULE_expression = 74, RULE_booleanExpression = 75, RULE_predicate = 76, 
		RULE_valueExpression = 77, RULE_primaryExpression = 78, RULE_constant = 79, 
		RULE_comparisonOperator = 80, RULE_arithmeticOperator = 81, RULE_predicateOperator = 82, 
		RULE_booleanValue = 83, RULE_interval = 84, RULE_intervalField = 85, RULE_intervalValue = 86, 
		RULE_colPosition = 87, RULE_dataType = 88, RULE_colTypeList = 89, RULE_colType = 90, 
		RULE_complexColTypeList = 91, RULE_complexColType = 92, RULE_whenClause = 93, 
		RULE_windows = 94, RULE_namedWindow = 95, RULE_windowSpec = 96, RULE_windowFrame = 97, 
		RULE_frameBound = 98, RULE_qualifiedName = 99, RULE_identifier = 100, 
		RULE_strictIdentifier = 101, RULE_quotedIdentifier = 102, RULE_number = 103, 
		RULE_nonReserved = 104;
	private static String[] makeRuleNames() {
		return new String[] {
			"singleStatement", "singleExpression", "singleTableIdentifier", "singleFunctionIdentifier", 
			"singleDataType", "singleTableSchema", "statement", "unsupportedHiveNativeCommands", 
			"delete", "update", "updateFieldList", "updateField", "createTableHeader", 
			"bucketSpec", "skewSpec", "locationSpec", "query", "insertInto", "partitionSpecLocation", 
			"partitionSpec", "partitionVal", "describeFuncName", "describeColName", 
			"ctes", "namedQuery", "tableProvider", "tablePropertyList", "tableProperty", 
			"tablePropertyKey", "tablePropertyValue", "constantList", "nestedConstantList", 
			"createFileFormat", "fileFormat", "storageHandler", "resource", "queryNoWith", 
			"queryOrganization", "multiInsertQueryBody", "queryTerm", "queryPrimary", 
			"sortItem", "querySpecification", "hint", "hintStatement", "fromClause", 
			"aggregation", "groupingSet", "pivotClause", "pivotColumn", "pivotValue", 
			"lateralView", "setQuantifier", "relation", "joinRelation", "joinType", 
			"joinCriteria", "sample", "sampleMethod", "identifierList", "identifierSeq", 
			"orderedIdentifierList", "orderedIdentifier", "identifierCommentList", 
			"identifierComment", "relationPrimary", "inlineTable", "functionTable", 
			"tableAlias", "rowFormat", "tableIdentifier", "functionIdentifier", "namedExpression", 
			"namedExpressionSeq", "expression", "booleanExpression", "predicate", 
			"valueExpression", "primaryExpression", "constant", "comparisonOperator", 
			"arithmeticOperator", "predicateOperator", "booleanValue", "interval", 
			"intervalField", "intervalValue", "colPosition", "dataType", "colTypeList", 
			"colType", "complexColTypeList", "complexColType", "whenClause", "windows", 
			"namedWindow", "windowSpec", "windowFrame", "frameBound", "qualifiedName", 
			"identifier", "strictIdentifier", "quotedIdentifier", "number", "nonReserved"
		};
	}
	public static final String[] ruleNames = makeRuleNames();

	private static String[] makeLiteralNames() {
		return new String[] {
			null, "'('", "')'", "','", "'.'", "'/*+'", "'*/'", "'->'", "'['", "']'", 
			"':'", "'SELECT'", "'FROM'", "'ADD'", "'AS'", "'ALL'", "'ANY'", "'DISTINCT'", 
			"'WHERE'", "'GROUP'", "'BY'", "'GROUPING'", "'SETS'", "'CUBE'", "'ROLLUP'", 
			"'ORDER'", "'HAVING'", "'LIMIT'", "'AT'", "'OR'", "'AND'", "'IN'", null, 
			"'NO'", "'EXISTS'", "'BETWEEN'", "'LIKE'", null, "'IS'", "'NULL'", "'TRUE'", 
			"'FALSE'", "'NULLS'", "'ASC'", "'DESC'", "'FOR'", "'INTERVAL'", "'CASE'", 
			"'WHEN'", "'THEN'", "'ELSE'", "'END'", "'JOIN'", "'CROSS'", "'OUTER'", 
			"'INNER'", "'LEFT'", "'SEMI'", "'RIGHT'", "'FULL'", "'NATURAL'", "'ON'", 
			"'PIVOT'", "'LATERAL'", "'WINDOW'", "'OVER'", "'PARTITION'", "'RANGE'", 
			"'ROWS'", "'UNBOUNDED'", "'PRECEDING'", "'FOLLOWING'", "'CURRENT'", "'FIRST'", 
			"'AFTER'", "'LAST'", "'ROW'", "'WITH'", "'VALUES'", "'CREATE'", "'TABLE'", 
			"'DIRECTORY'", "'VIEW'", "'REPLACE'", "'INSERT'", "'UPDATE'", "'DELETE'", 
			"'INTO'", "'DESCRIBE'", "'EXPLAIN'", "'FORMAT'", "'LOGICAL'", "'CODEGEN'", 
			"'COST'", "'CAST'", "'SHOW'", "'TABLES'", "'COLUMNS'", "'COLUMN'", "'USE'", 
			"'PARTITIONS'", "'FUNCTIONS'", "'DROP'", "'UNION'", "'EXCEPT'", "'MINUS'", 
			"'INTERSECT'", "'TO'", "'TABLESAMPLE'", "'STRATIFY'", "'ALTER'", "'RENAME'", 
			"'ARRAY'", "'MAP'", "'STRUCT'", "'COMMENT'", "'SET'", "'RESET'", "'DATA'", 
			"'START'", "'TRANSACTION'", "'COMMIT'", "'ROLLBACK'", "'MACRO'", "'IGNORE'", 
			"'BOTH'", "'LEADING'", "'TRAILING'", "'IF'", "'POSITION'", "'EXTRACT'", 
			null, "'<=>'", "'<>'", "'!='", "'<'", null, "'>'", null, "'+'", "'-'", 
			"'*'", "'/'", "'%'", "'DIV'", "'~'", "'&'", "'|'", "'||'", "'^'", "'PERCENT'", 
			"'BUCKET'", "'OUT'", "'OF'", "'SORT'", "'CLUSTER'", "'DISTRIBUTE'", "'OVERWRITE'", 
			"'TRANSFORM'", "'REDUCE'", "'USING'", "'SERDE'", "'SERDEPROPERTIES'", 
			"'RECORDREADER'", "'RECORDWRITER'", "'DELIMITED'", "'FIELDS'", "'TERMINATED'", 
			"'COLLECTION'", "'ITEMS'", "'KEYS'", "'ESCAPED'", "'LINES'", "'SEPARATED'", 
			"'FUNCTION'", "'EXTENDED'", "'REFRESH'", "'CLEAR'", "'CACHE'", "'UNCACHE'", 
			"'LAZY'", "'FORMATTED'", "'GLOBAL'", null, "'OPTIONS'", "'UNSET'", "'TBLPROPERTIES'", 
			"'DBPROPERTIES'", "'BUCKETS'", "'SKEWED'", "'STORED'", "'DIRECTORIES'", 
			"'LOCATION'", "'EXCHANGE'", "'ARCHIVE'", "'UNARCHIVE'", "'FILEFORMAT'", 
			"'TOUCH'", "'COMPACT'", "'CONCATENATE'", "'CHANGE'", "'CASCADE'", "'RESTRICT'", 
			"'CLUSTERED'", "'SORTED'", "'PURGE'", "'INPUTFORMAT'", "'OUTPUTFORMAT'", 
			null, null, "'DFS'", "'TRUNCATE'", "'ANALYZE'", "'COMPUTE'", "'LIST'", 
			"'STATISTICS'", "'PARTITIONED'", "'EXTERNAL'", "'DEFINED'", "'REVOKE'", 
			"'GRANT'", "'LOCK'", "'UNLOCK'", "'MSCK'", "'REPAIR'", "'RECOVER'", "'EXPORT'", 
			"'IMPORT'", "'LOAD'", "'ROLE'", "'ROLES'", "'COMPACTIONS'", "'PRINCIPALS'", 
			"'TRANSACTIONS'", "'INDEX'", "'INDEXES'", "'LOCKS'", "'OPTION'", "'ANTI'", 
			"'LOCAL'", "'INPATH'", null, null, null, null, null, null, null, null, 
			null, null, null, "'/**/'"
		};
	}
	private static final String[] _LITERAL_NAMES = makeLiteralNames();
	private static String[] makeSymbolicNames() {
		return new String[] {
			null, null, null, null, null, null, null, null, null, null, null, "SELECT", 
			"FROM", "ADD", "AS", "ALL", "ANY", "DISTINCT", "WHERE", "GROUP", "BY", 
			"GROUPING", "SETS", "CUBE", "ROLLUP", "ORDER", "HAVING", "LIMIT", "AT", 
			"OR", "AND", "IN", "NOT", "NO", "EXISTS", "BETWEEN", "LIKE", "RLIKE", 
			"IS", "NULL", "TRUE", "FALSE", "NULLS", "ASC", "DESC", "FOR", "INTERVAL", 
			"CASE", "WHEN", "THEN", "ELSE", "END", "JOIN", "CROSS", "OUTER", "INNER", 
			"LEFT", "SEMI", "RIGHT", "FULL", "NATURAL", "ON", "PIVOT", "LATERAL", 
			"WINDOW", "OVER", "PARTITION", "RANGE", "ROWS", "UNBOUNDED", "PRECEDING", 
			"FOLLOWING", "CURRENT", "FIRST", "AFTER", "LAST", "ROW", "WITH", "VALUES", 
			"CREATE", "TABLE", "DIRECTORY", "VIEW", "REPLACE", "INSERT", "UPDATE", 
			"DELETE", "INTO", "DESCRIBE", "EXPLAIN", "FORMAT", "LOGICAL", "CODEGEN", 
			"COST", "CAST", "SHOW", "TABLES", "COLUMNS", "COLUMN", "USE", "PARTITIONS", 
			"FUNCTIONS", "DROP", "UNION", "EXCEPT", "SETMINUS", "INTERSECT", "TO", 
			"TABLESAMPLE", "STRATIFY", "ALTER", "RENAME", "ARRAY", "MAP", "STRUCT", 
			"COMMENT", "SET", "RESET", "DATA", "START", "TRANSACTION", "COMMIT", 
			"ROLLBACK", "MACRO", "IGNORE", "BOTH", "LEADING", "TRAILING", "IF", "POSITION", 
			"EXTRACT", "EQ", "NSEQ", "NEQ", "NEQJ", "LT", "LTE", "GT", "GTE", "PLUS", 
			"MINUS", "ASTERISK", "SLASH", "PERCENT", "DIV", "TILDE", "AMPERSAND", 
			"PIPE", "CONCAT_PIPE", "HAT", "PERCENTLIT", "BUCKET", "OUT", "OF", "SORT", 
			"CLUSTER", "DISTRIBUTE", "OVERWRITE", "TRANSFORM", "REDUCE", "USING", 
			"SERDE", "SERDEPROPERTIES", "RECORDREADER", "RECORDWRITER", "DELIMITED", 
			"FIELDS", "TERMINATED", "COLLECTION", "ITEMS", "KEYS", "ESCAPED", "LINES", 
			"SEPARATED", "FUNCTION", "EXTENDED", "REFRESH", "CLEAR", "CACHE", "UNCACHE", 
			"LAZY", "FORMATTED", "GLOBAL", "TEMPORARY", "OPTIONS", "UNSET", "TBLPROPERTIES", 
			"DBPROPERTIES", "BUCKETS", "SKEWED", "STORED", "DIRECTORIES", "LOCATION", 
			"EXCHANGE", "ARCHIVE", "UNARCHIVE", "FILEFORMAT", "TOUCH", "COMPACT", 
			"CONCATENATE", "CHANGE", "CASCADE", "RESTRICT", "CLUSTERED", "SORTED", 
			"PURGE", "INPUTFORMAT", "OUTPUTFORMAT", "DATABASE", "DATABASES", "DFS", 
			"TRUNCATE", "ANALYZE", "COMPUTE", "LIST", "STATISTICS", "PARTITIONED", 
			"EXTERNAL", "DEFINED", "REVOKE", "GRANT", "LOCK", "UNLOCK", "MSCK", "REPAIR", 
			"RECOVER", "EXPORT", "IMPORT", "LOAD", "ROLE", "ROLES", "COMPACTIONS", 
			"PRINCIPALS", "TRANSACTIONS", "INDEX", "INDEXES", "LOCKS", "OPTION", 
			"ANTI", "LOCAL", "INPATH", "STRING", "BIGINT_LITERAL", "SMALLINT_LITERAL", 
			"TINYINT_LITERAL", "INTEGER_VALUE", "DECIMAL_VALUE", "DOUBLE_LITERAL", 
			"BIGDECIMAL_LITERAL", "IDENTIFIER", "BACKQUOTED_IDENTIFIER", "SIMPLE_COMMENT", 
			"BRACKETED_EMPTY_COMMENT", "BRACKETED_COMMENT", "WS", "UNRECOGNIZED"
		};
	}
	private static final String[] _SYMBOLIC_NAMES = makeSymbolicNames();
	public static final Vocabulary VOCABULARY = new VocabularyImpl(_LITERAL_NAMES, _SYMBOLIC_NAMES);

	/**
	 * @deprecated Use {@link #VOCABULARY} instead.
	 */
	@Deprecated
	public static final String[] tokenNames;
	static {
		tokenNames = new String[_SYMBOLIC_NAMES.length];
		for (int i = 0; i < tokenNames.length; i++) {
			tokenNames[i] = VOCABULARY.getLiteralName(i);
			if (tokenNames[i] == null) {
				tokenNames[i] = VOCABULARY.getSymbolicName(i);
			}

			if (tokenNames[i] == null) {
				tokenNames[i] = "<INVALID>";
			}
		}
	}

	@Override
	@Deprecated
	public String[] getTokenNames() {
		return tokenNames;
	}

	@Override

	public Vocabulary getVocabulary() {
		return VOCABULARY;
	}

	@Override
	public String getGrammarFileName() { return "SqlHive.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public ATN getATN() { return _ATN; }


	  /**
	   * When false, INTERSECT is given the greater precedence over the other set
	   * operations (UNION, EXCEPT and MINUS) as per the SQL standard.
	   */
	  public boolean legacy_setops_precedence_enbled = false;

	  /**
	   * Verify whether current token is a valid decimal token (which contains dot).
	   * Returns true if the character that follows the token is not a digit or letter or underscore.
	   *
	   * For example:
	   * For char stream "2.3", "2." is not a valid decimal token, because it is followed by digit '3'.
	   * For char stream "2.3_", "2.3" is not a valid decimal token, because it is followed by '_'.
	   * For char stream "2.3W", "2.3" is not a valid decimal token, because it is followed by 'W'.
	   * For char stream "12.0D 34.E2+0.12 "  12.0D is a valid decimal token because it is followed
	   * by a space. 34.E2 is a valid decimal token because it is followed by symbol '+'
	   * which is not a digit or letter or underscore.
	   */
	  public boolean isValidDecimal() {
	    int nextChar = _input.LA(1);
	    if (nextChar >= 'A' && nextChar <= 'Z' || nextChar >= '0' && nextChar <= '9' ||
	      nextChar == '_') {
	      return false;
	    } else {
	      return true;
	    }
	  }

	public SqlHiveParser(TokenStream input) {
		super(input);
		_interp = new ParserATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}

	public static class SingleStatementContext extends ParserRuleContext {
		public StatementContext statement() {
			return getRuleContext(StatementContext.class,0);
		}
		public TerminalNode EOF() { return getToken(SqlHiveParser.EOF, 0); }
		public SingleStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_singleStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterSingleStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitSingleStatement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitSingleStatement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SingleStatementContext singleStatement() throws RecognitionException {
		SingleStatementContext _localctx = new SingleStatementContext(_ctx, getState());
		enterRule(_localctx, 0, RULE_singleStatement);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(210);
			statement();
			setState(211);
			match(EOF);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SingleExpressionContext extends ParserRuleContext {
		public NamedExpressionContext namedExpression() {
			return getRuleContext(NamedExpressionContext.class,0);
		}
		public TerminalNode EOF() { return getToken(SqlHiveParser.EOF, 0); }
		public SingleExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_singleExpression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterSingleExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitSingleExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitSingleExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SingleExpressionContext singleExpression() throws RecognitionException {
		SingleExpressionContext _localctx = new SingleExpressionContext(_ctx, getState());
		enterRule(_localctx, 2, RULE_singleExpression);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(213);
			namedExpression();
			setState(214);
			match(EOF);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SingleTableIdentifierContext extends ParserRuleContext {
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode EOF() { return getToken(SqlHiveParser.EOF, 0); }
		public SingleTableIdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_singleTableIdentifier; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterSingleTableIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitSingleTableIdentifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitSingleTableIdentifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SingleTableIdentifierContext singleTableIdentifier() throws RecognitionException {
		SingleTableIdentifierContext _localctx = new SingleTableIdentifierContext(_ctx, getState());
		enterRule(_localctx, 4, RULE_singleTableIdentifier);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(216);
			tableIdentifier();
			setState(217);
			match(EOF);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SingleFunctionIdentifierContext extends ParserRuleContext {
		public FunctionIdentifierContext functionIdentifier() {
			return getRuleContext(FunctionIdentifierContext.class,0);
		}
		public TerminalNode EOF() { return getToken(SqlHiveParser.EOF, 0); }
		public SingleFunctionIdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_singleFunctionIdentifier; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterSingleFunctionIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitSingleFunctionIdentifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitSingleFunctionIdentifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SingleFunctionIdentifierContext singleFunctionIdentifier() throws RecognitionException {
		SingleFunctionIdentifierContext _localctx = new SingleFunctionIdentifierContext(_ctx, getState());
		enterRule(_localctx, 6, RULE_singleFunctionIdentifier);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(219);
			functionIdentifier();
			setState(220);
			match(EOF);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SingleDataTypeContext extends ParserRuleContext {
		public DataTypeContext dataType() {
			return getRuleContext(DataTypeContext.class,0);
		}
		public TerminalNode EOF() { return getToken(SqlHiveParser.EOF, 0); }
		public SingleDataTypeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_singleDataType; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterSingleDataType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitSingleDataType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitSingleDataType(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SingleDataTypeContext singleDataType() throws RecognitionException {
		SingleDataTypeContext _localctx = new SingleDataTypeContext(_ctx, getState());
		enterRule(_localctx, 8, RULE_singleDataType);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(222);
			dataType();
			setState(223);
			match(EOF);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SingleTableSchemaContext extends ParserRuleContext {
		public ColTypeListContext colTypeList() {
			return getRuleContext(ColTypeListContext.class,0);
		}
		public TerminalNode EOF() { return getToken(SqlHiveParser.EOF, 0); }
		public SingleTableSchemaContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_singleTableSchema; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterSingleTableSchema(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitSingleTableSchema(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitSingleTableSchema(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SingleTableSchemaContext singleTableSchema() throws RecognitionException {
		SingleTableSchemaContext _localctx = new SingleTableSchemaContext(_ctx, getState());
		enterRule(_localctx, 10, RULE_singleTableSchema);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(225);
			colTypeList();
			setState(226);
			match(EOF);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class StatementContext extends ParserRuleContext {
		public StatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_statement; }
	 
		public StatementContext() { }
		public void copyFrom(StatementContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class ExplainContext extends StatementContext {
		public TerminalNode EXPLAIN() { return getToken(SqlHiveParser.EXPLAIN, 0); }
		public StatementContext statement() {
			return getRuleContext(StatementContext.class,0);
		}
		public TerminalNode LOGICAL() { return getToken(SqlHiveParser.LOGICAL, 0); }
		public TerminalNode FORMATTED() { return getToken(SqlHiveParser.FORMATTED, 0); }
		public TerminalNode EXTENDED() { return getToken(SqlHiveParser.EXTENDED, 0); }
		public TerminalNode CODEGEN() { return getToken(SqlHiveParser.CODEGEN, 0); }
		public TerminalNode COST() { return getToken(SqlHiveParser.COST, 0); }
		public ExplainContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterExplain(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitExplain(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitExplain(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class DropDatabaseContext extends StatementContext {
		public TerminalNode DROP() { return getToken(SqlHiveParser.DROP, 0); }
		public TerminalNode DATABASE() { return getToken(SqlHiveParser.DATABASE, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode IF() { return getToken(SqlHiveParser.IF, 0); }
		public TerminalNode EXISTS() { return getToken(SqlHiveParser.EXISTS, 0); }
		public TerminalNode RESTRICT() { return getToken(SqlHiveParser.RESTRICT, 0); }
		public TerminalNode CASCADE() { return getToken(SqlHiveParser.CASCADE, 0); }
		public DropDatabaseContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterDropDatabase(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitDropDatabase(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitDropDatabase(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ResetConfigurationContext extends StatementContext {
		public TerminalNode RESET() { return getToken(SqlHiveParser.RESET, 0); }
		public ResetConfigurationContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterResetConfiguration(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitResetConfiguration(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitResetConfiguration(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class DescribeDatabaseContext extends StatementContext {
		public TerminalNode DATABASE() { return getToken(SqlHiveParser.DATABASE, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode DESC() { return getToken(SqlHiveParser.DESC, 0); }
		public TerminalNode DESCRIBE() { return getToken(SqlHiveParser.DESCRIBE, 0); }
		public TerminalNode EXTENDED() { return getToken(SqlHiveParser.EXTENDED, 0); }
		public DescribeDatabaseContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterDescribeDatabase(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitDescribeDatabase(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitDescribeDatabase(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class AlterViewQueryContext extends StatementContext {
		public TerminalNode ALTER() { return getToken(SqlHiveParser.ALTER, 0); }
		public TerminalNode VIEW() { return getToken(SqlHiveParser.VIEW, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlHiveParser.AS, 0); }
		public AlterViewQueryContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterAlterViewQuery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitAlterViewQuery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitAlterViewQuery(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class UseContext extends StatementContext {
		public IdentifierContext db;
		public TerminalNode USE() { return getToken(SqlHiveParser.USE, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public UseContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterUse(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitUse(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitUse(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class CreateTempViewUsingContext extends StatementContext {
		public TerminalNode CREATE() { return getToken(SqlHiveParser.CREATE, 0); }
		public TerminalNode TEMPORARY() { return getToken(SqlHiveParser.TEMPORARY, 0); }
		public TerminalNode VIEW() { return getToken(SqlHiveParser.VIEW, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TableProviderContext tableProvider() {
			return getRuleContext(TableProviderContext.class,0);
		}
		public TerminalNode OR() { return getToken(SqlHiveParser.OR, 0); }
		public TerminalNode REPLACE() { return getToken(SqlHiveParser.REPLACE, 0); }
		public TerminalNode GLOBAL() { return getToken(SqlHiveParser.GLOBAL, 0); }
		public ColTypeListContext colTypeList() {
			return getRuleContext(ColTypeListContext.class,0);
		}
		public TerminalNode OPTIONS() { return getToken(SqlHiveParser.OPTIONS, 0); }
		public TablePropertyListContext tablePropertyList() {
			return getRuleContext(TablePropertyListContext.class,0);
		}
		public CreateTempViewUsingContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterCreateTempViewUsing(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitCreateTempViewUsing(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitCreateTempViewUsing(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class RenameTableContext extends StatementContext {
		public TableIdentifierContext from;
		public TableIdentifierContext to;
		public TerminalNode ALTER() { return getToken(SqlHiveParser.ALTER, 0); }
		public TerminalNode RENAME() { return getToken(SqlHiveParser.RENAME, 0); }
		public TerminalNode TO() { return getToken(SqlHiveParser.TO, 0); }
		public TerminalNode TABLE() { return getToken(SqlHiveParser.TABLE, 0); }
		public TerminalNode VIEW() { return getToken(SqlHiveParser.VIEW, 0); }
		public List<TableIdentifierContext> tableIdentifier() {
			return getRuleContexts(TableIdentifierContext.class);
		}
		public TableIdentifierContext tableIdentifier(int i) {
			return getRuleContext(TableIdentifierContext.class,i);
		}
		public RenameTableContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterRenameTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitRenameTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitRenameTable(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class FailNativeCommandContext extends StatementContext {
		public TerminalNode SET() { return getToken(SqlHiveParser.SET, 0); }
		public TerminalNode ROLE() { return getToken(SqlHiveParser.ROLE, 0); }
		public UnsupportedHiveNativeCommandsContext unsupportedHiveNativeCommands() {
			return getRuleContext(UnsupportedHiveNativeCommandsContext.class,0);
		}
		public FailNativeCommandContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterFailNativeCommand(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitFailNativeCommand(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitFailNativeCommand(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ClearCacheContext extends StatementContext {
		public TerminalNode CLEAR() { return getToken(SqlHiveParser.CLEAR, 0); }
		public TerminalNode CACHE() { return getToken(SqlHiveParser.CACHE, 0); }
		public ClearCacheContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterClearCache(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitClearCache(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitClearCache(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ShowTablesContext extends StatementContext {
		public IdentifierContext db;
		public Token pattern;
		public TerminalNode SHOW() { return getToken(SqlHiveParser.SHOW, 0); }
		public TerminalNode TABLES() { return getToken(SqlHiveParser.TABLES, 0); }
		public TerminalNode FROM() { return getToken(SqlHiveParser.FROM, 0); }
		public TerminalNode IN() { return getToken(SqlHiveParser.IN, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode STRING() { return getToken(SqlHiveParser.STRING, 0); }
		public TerminalNode LIKE() { return getToken(SqlHiveParser.LIKE, 0); }
		public ShowTablesContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterShowTables(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitShowTables(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitShowTables(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class RecoverPartitionsContext extends StatementContext {
		public TerminalNode ALTER() { return getToken(SqlHiveParser.ALTER, 0); }
		public TerminalNode TABLE() { return getToken(SqlHiveParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode RECOVER() { return getToken(SqlHiveParser.RECOVER, 0); }
		public TerminalNode PARTITIONS() { return getToken(SqlHiveParser.PARTITIONS, 0); }
		public RecoverPartitionsContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterRecoverPartitions(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitRecoverPartitions(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitRecoverPartitions(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class RenameTablePartitionContext extends StatementContext {
		public PartitionSpecContext from;
		public PartitionSpecContext to;
		public TerminalNode ALTER() { return getToken(SqlHiveParser.ALTER, 0); }
		public TerminalNode TABLE() { return getToken(SqlHiveParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode RENAME() { return getToken(SqlHiveParser.RENAME, 0); }
		public TerminalNode TO() { return getToken(SqlHiveParser.TO, 0); }
		public List<PartitionSpecContext> partitionSpec() {
			return getRuleContexts(PartitionSpecContext.class);
		}
		public PartitionSpecContext partitionSpec(int i) {
			return getRuleContext(PartitionSpecContext.class,i);
		}
		public RenameTablePartitionContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterRenameTablePartition(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitRenameTablePartition(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitRenameTablePartition(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class RepairTableContext extends StatementContext {
		public TerminalNode MSCK() { return getToken(SqlHiveParser.MSCK, 0); }
		public TerminalNode REPAIR() { return getToken(SqlHiveParser.REPAIR, 0); }
		public TerminalNode TABLE() { return getToken(SqlHiveParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public RepairTableContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterRepairTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitRepairTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitRepairTable(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class RefreshResourceContext extends StatementContext {
		public TerminalNode REFRESH() { return getToken(SqlHiveParser.REFRESH, 0); }
		public TerminalNode STRING() { return getToken(SqlHiveParser.STRING, 0); }
		public RefreshResourceContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterRefreshResource(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitRefreshResource(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitRefreshResource(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ShowCreateTableContext extends StatementContext {
		public TerminalNode SHOW() { return getToken(SqlHiveParser.SHOW, 0); }
		public TerminalNode CREATE() { return getToken(SqlHiveParser.CREATE, 0); }
		public TerminalNode TABLE() { return getToken(SqlHiveParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public ShowCreateTableContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterShowCreateTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitShowCreateTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitShowCreateTable(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ShowColumnsContext extends StatementContext {
		public IdentifierContext db;
		public TerminalNode SHOW() { return getToken(SqlHiveParser.SHOW, 0); }
		public TerminalNode COLUMNS() { return getToken(SqlHiveParser.COLUMNS, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public List<TerminalNode> FROM() { return getTokens(SqlHiveParser.FROM); }
		public TerminalNode FROM(int i) {
			return getToken(SqlHiveParser.FROM, i);
		}
		public List<TerminalNode> IN() { return getTokens(SqlHiveParser.IN); }
		public TerminalNode IN(int i) {
			return getToken(SqlHiveParser.IN, i);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public ShowColumnsContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterShowColumns(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitShowColumns(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitShowColumns(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class AddTablePartitionContext extends StatementContext {
		public TerminalNode ALTER() { return getToken(SqlHiveParser.ALTER, 0); }
		public TerminalNode TABLE() { return getToken(SqlHiveParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode ADD() { return getToken(SqlHiveParser.ADD, 0); }
		public TerminalNode IF() { return getToken(SqlHiveParser.IF, 0); }
		public TerminalNode NOT() { return getToken(SqlHiveParser.NOT, 0); }
		public TerminalNode EXISTS() { return getToken(SqlHiveParser.EXISTS, 0); }
		public List<PartitionSpecLocationContext> partitionSpecLocation() {
			return getRuleContexts(PartitionSpecLocationContext.class);
		}
		public PartitionSpecLocationContext partitionSpecLocation(int i) {
			return getRuleContext(PartitionSpecLocationContext.class,i);
		}
		public TerminalNode VIEW() { return getToken(SqlHiveParser.VIEW, 0); }
		public List<PartitionSpecContext> partitionSpec() {
			return getRuleContexts(PartitionSpecContext.class);
		}
		public PartitionSpecContext partitionSpec(int i) {
			return getRuleContext(PartitionSpecContext.class,i);
		}
		public AddTablePartitionContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterAddTablePartition(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitAddTablePartition(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitAddTablePartition(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class RefreshTableContext extends StatementContext {
		public TerminalNode REFRESH() { return getToken(SqlHiveParser.REFRESH, 0); }
		public TerminalNode TABLE() { return getToken(SqlHiveParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public RefreshTableContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterRefreshTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitRefreshTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitRefreshTable(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ManageResourceContext extends StatementContext {
		public Token op;
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode ADD() { return getToken(SqlHiveParser.ADD, 0); }
		public TerminalNode LIST() { return getToken(SqlHiveParser.LIST, 0); }
		public ManageResourceContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterManageResource(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitManageResource(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitManageResource(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class CreateDatabaseContext extends StatementContext {
		public Token comment;
		public TerminalNode CREATE() { return getToken(SqlHiveParser.CREATE, 0); }
		public TerminalNode DATABASE() { return getToken(SqlHiveParser.DATABASE, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode IF() { return getToken(SqlHiveParser.IF, 0); }
		public TerminalNode NOT() { return getToken(SqlHiveParser.NOT, 0); }
		public TerminalNode EXISTS() { return getToken(SqlHiveParser.EXISTS, 0); }
		public TerminalNode COMMENT() { return getToken(SqlHiveParser.COMMENT, 0); }
		public LocationSpecContext locationSpec() {
			return getRuleContext(LocationSpecContext.class,0);
		}
		public TerminalNode WITH() { return getToken(SqlHiveParser.WITH, 0); }
		public TerminalNode DBPROPERTIES() { return getToken(SqlHiveParser.DBPROPERTIES, 0); }
		public TablePropertyListContext tablePropertyList() {
			return getRuleContext(TablePropertyListContext.class,0);
		}
		public TerminalNode STRING() { return getToken(SqlHiveParser.STRING, 0); }
		public CreateDatabaseContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterCreateDatabase(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitCreateDatabase(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitCreateDatabase(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class AnalyzeContext extends StatementContext {
		public TerminalNode ANALYZE() { return getToken(SqlHiveParser.ANALYZE, 0); }
		public TerminalNode TABLE() { return getToken(SqlHiveParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode COMPUTE() { return getToken(SqlHiveParser.COMPUTE, 0); }
		public TerminalNode STATISTICS() { return getToken(SqlHiveParser.STATISTICS, 0); }
		public PartitionSpecContext partitionSpec() {
			return getRuleContext(PartitionSpecContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode FOR() { return getToken(SqlHiveParser.FOR, 0); }
		public TerminalNode COLUMNS() { return getToken(SqlHiveParser.COLUMNS, 0); }
		public IdentifierSeqContext identifierSeq() {
			return getRuleContext(IdentifierSeqContext.class,0);
		}
		public AnalyzeContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterAnalyze(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitAnalyze(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitAnalyze(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class CreateHiveTableContext extends StatementContext {
		public ColTypeListContext columns;
		public Token comment;
		public ColTypeListContext partitionColumns;
		public TablePropertyListContext tableProps;
		public CreateTableHeaderContext createTableHeader() {
			return getRuleContext(CreateTableHeaderContext.class,0);
		}
		public List<BucketSpecContext> bucketSpec() {
			return getRuleContexts(BucketSpecContext.class);
		}
		public BucketSpecContext bucketSpec(int i) {
			return getRuleContext(BucketSpecContext.class,i);
		}
		public List<SkewSpecContext> skewSpec() {
			return getRuleContexts(SkewSpecContext.class);
		}
		public SkewSpecContext skewSpec(int i) {
			return getRuleContext(SkewSpecContext.class,i);
		}
		public List<RowFormatContext> rowFormat() {
			return getRuleContexts(RowFormatContext.class);
		}
		public RowFormatContext rowFormat(int i) {
			return getRuleContext(RowFormatContext.class,i);
		}
		public List<CreateFileFormatContext> createFileFormat() {
			return getRuleContexts(CreateFileFormatContext.class);
		}
		public CreateFileFormatContext createFileFormat(int i) {
			return getRuleContext(CreateFileFormatContext.class,i);
		}
		public List<LocationSpecContext> locationSpec() {
			return getRuleContexts(LocationSpecContext.class);
		}
		public LocationSpecContext locationSpec(int i) {
			return getRuleContext(LocationSpecContext.class,i);
		}
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public List<ColTypeListContext> colTypeList() {
			return getRuleContexts(ColTypeListContext.class);
		}
		public ColTypeListContext colTypeList(int i) {
			return getRuleContext(ColTypeListContext.class,i);
		}
		public List<TerminalNode> COMMENT() { return getTokens(SqlHiveParser.COMMENT); }
		public TerminalNode COMMENT(int i) {
			return getToken(SqlHiveParser.COMMENT, i);
		}
		public List<TerminalNode> PARTITIONED() { return getTokens(SqlHiveParser.PARTITIONED); }
		public TerminalNode PARTITIONED(int i) {
			return getToken(SqlHiveParser.PARTITIONED, i);
		}
		public List<TerminalNode> BY() { return getTokens(SqlHiveParser.BY); }
		public TerminalNode BY(int i) {
			return getToken(SqlHiveParser.BY, i);
		}
		public List<TerminalNode> TBLPROPERTIES() { return getTokens(SqlHiveParser.TBLPROPERTIES); }
		public TerminalNode TBLPROPERTIES(int i) {
			return getToken(SqlHiveParser.TBLPROPERTIES, i);
		}
		public List<TerminalNode> STRING() { return getTokens(SqlHiveParser.STRING); }
		public TerminalNode STRING(int i) {
			return getToken(SqlHiveParser.STRING, i);
		}
		public List<TablePropertyListContext> tablePropertyList() {
			return getRuleContexts(TablePropertyListContext.class);
		}
		public TablePropertyListContext tablePropertyList(int i) {
			return getRuleContext(TablePropertyListContext.class,i);
		}
		public TerminalNode AS() { return getToken(SqlHiveParser.AS, 0); }
		public CreateHiveTableContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterCreateHiveTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitCreateHiveTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitCreateHiveTable(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class CreateFunctionContext extends StatementContext {
		public Token className;
		public TerminalNode CREATE() { return getToken(SqlHiveParser.CREATE, 0); }
		public TerminalNode FUNCTION() { return getToken(SqlHiveParser.FUNCTION, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlHiveParser.AS, 0); }
		public TerminalNode STRING() { return getToken(SqlHiveParser.STRING, 0); }
		public TerminalNode OR() { return getToken(SqlHiveParser.OR, 0); }
		public TerminalNode REPLACE() { return getToken(SqlHiveParser.REPLACE, 0); }
		public TerminalNode TEMPORARY() { return getToken(SqlHiveParser.TEMPORARY, 0); }
		public TerminalNode IF() { return getToken(SqlHiveParser.IF, 0); }
		public TerminalNode NOT() { return getToken(SqlHiveParser.NOT, 0); }
		public TerminalNode EXISTS() { return getToken(SqlHiveParser.EXISTS, 0); }
		public TerminalNode USING() { return getToken(SqlHiveParser.USING, 0); }
		public List<ResourceContext> resource() {
			return getRuleContexts(ResourceContext.class);
		}
		public ResourceContext resource(int i) {
			return getRuleContext(ResourceContext.class,i);
		}
		public CreateFunctionContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterCreateFunction(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitCreateFunction(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitCreateFunction(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ShowTableContext extends StatementContext {
		public IdentifierContext db;
		public Token pattern;
		public TerminalNode SHOW() { return getToken(SqlHiveParser.SHOW, 0); }
		public TerminalNode TABLE() { return getToken(SqlHiveParser.TABLE, 0); }
		public TerminalNode EXTENDED() { return getToken(SqlHiveParser.EXTENDED, 0); }
		public TerminalNode LIKE() { return getToken(SqlHiveParser.LIKE, 0); }
		public TerminalNode STRING() { return getToken(SqlHiveParser.STRING, 0); }
		public PartitionSpecContext partitionSpec() {
			return getRuleContext(PartitionSpecContext.class,0);
		}
		public TerminalNode FROM() { return getToken(SqlHiveParser.FROM, 0); }
		public TerminalNode IN() { return getToken(SqlHiveParser.IN, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public ShowTableContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterShowTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitShowTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitShowTable(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class UpdateCommandContext extends StatementContext {
		public UpdateContext update() {
			return getRuleContext(UpdateContext.class,0);
		}
		public UpdateCommandContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterUpdateCommand(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitUpdateCommand(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitUpdateCommand(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class SetDatabasePropertiesContext extends StatementContext {
		public TerminalNode ALTER() { return getToken(SqlHiveParser.ALTER, 0); }
		public TerminalNode DATABASE() { return getToken(SqlHiveParser.DATABASE, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode SET() { return getToken(SqlHiveParser.SET, 0); }
		public TerminalNode DBPROPERTIES() { return getToken(SqlHiveParser.DBPROPERTIES, 0); }
		public TablePropertyListContext tablePropertyList() {
			return getRuleContext(TablePropertyListContext.class,0);
		}
		public SetDatabasePropertiesContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterSetDatabaseProperties(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitSetDatabaseProperties(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitSetDatabaseProperties(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class CreateTableContext extends StatementContext {
		public TablePropertyListContext options;
		public IdentifierListContext partitionColumnNames;
		public Token comment;
		public TablePropertyListContext tableProps;
		public CreateTableHeaderContext createTableHeader() {
			return getRuleContext(CreateTableHeaderContext.class,0);
		}
		public TableProviderContext tableProvider() {
			return getRuleContext(TableProviderContext.class,0);
		}
		public ColTypeListContext colTypeList() {
			return getRuleContext(ColTypeListContext.class,0);
		}
		public List<BucketSpecContext> bucketSpec() {
			return getRuleContexts(BucketSpecContext.class);
		}
		public BucketSpecContext bucketSpec(int i) {
			return getRuleContext(BucketSpecContext.class,i);
		}
		public List<LocationSpecContext> locationSpec() {
			return getRuleContexts(LocationSpecContext.class);
		}
		public LocationSpecContext locationSpec(int i) {
			return getRuleContext(LocationSpecContext.class,i);
		}
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public List<TerminalNode> OPTIONS() { return getTokens(SqlHiveParser.OPTIONS); }
		public TerminalNode OPTIONS(int i) {
			return getToken(SqlHiveParser.OPTIONS, i);
		}
		public List<TerminalNode> PARTITIONED() { return getTokens(SqlHiveParser.PARTITIONED); }
		public TerminalNode PARTITIONED(int i) {
			return getToken(SqlHiveParser.PARTITIONED, i);
		}
		public List<TerminalNode> BY() { return getTokens(SqlHiveParser.BY); }
		public TerminalNode BY(int i) {
			return getToken(SqlHiveParser.BY, i);
		}
		public List<TerminalNode> COMMENT() { return getTokens(SqlHiveParser.COMMENT); }
		public TerminalNode COMMENT(int i) {
			return getToken(SqlHiveParser.COMMENT, i);
		}
		public List<TerminalNode> TBLPROPERTIES() { return getTokens(SqlHiveParser.TBLPROPERTIES); }
		public TerminalNode TBLPROPERTIES(int i) {
			return getToken(SqlHiveParser.TBLPROPERTIES, i);
		}
		public List<TablePropertyListContext> tablePropertyList() {
			return getRuleContexts(TablePropertyListContext.class);
		}
		public TablePropertyListContext tablePropertyList(int i) {
			return getRuleContext(TablePropertyListContext.class,i);
		}
		public List<IdentifierListContext> identifierList() {
			return getRuleContexts(IdentifierListContext.class);
		}
		public IdentifierListContext identifierList(int i) {
			return getRuleContext(IdentifierListContext.class,i);
		}
		public List<TerminalNode> STRING() { return getTokens(SqlHiveParser.STRING); }
		public TerminalNode STRING(int i) {
			return getToken(SqlHiveParser.STRING, i);
		}
		public TerminalNode AS() { return getToken(SqlHiveParser.AS, 0); }
		public CreateTableContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterCreateTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitCreateTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitCreateTable(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class DescribeTableContext extends StatementContext {
		public Token option;
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode DESC() { return getToken(SqlHiveParser.DESC, 0); }
		public TerminalNode DESCRIBE() { return getToken(SqlHiveParser.DESCRIBE, 0); }
		public TerminalNode TABLE() { return getToken(SqlHiveParser.TABLE, 0); }
		public PartitionSpecContext partitionSpec() {
			return getRuleContext(PartitionSpecContext.class,0);
		}
		public DescribeColNameContext describeColName() {
			return getRuleContext(DescribeColNameContext.class,0);
		}
		public TerminalNode EXTENDED() { return getToken(SqlHiveParser.EXTENDED, 0); }
		public TerminalNode FORMATTED() { return getToken(SqlHiveParser.FORMATTED, 0); }
		public DescribeTableContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterDescribeTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitDescribeTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitDescribeTable(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class CreateTableLikeContext extends StatementContext {
		public TableIdentifierContext target;
		public TableIdentifierContext source;
		public TerminalNode CREATE() { return getToken(SqlHiveParser.CREATE, 0); }
		public TerminalNode TABLE() { return getToken(SqlHiveParser.TABLE, 0); }
		public TerminalNode LIKE() { return getToken(SqlHiveParser.LIKE, 0); }
		public List<TableIdentifierContext> tableIdentifier() {
			return getRuleContexts(TableIdentifierContext.class);
		}
		public TableIdentifierContext tableIdentifier(int i) {
			return getRuleContext(TableIdentifierContext.class,i);
		}
		public TerminalNode IF() { return getToken(SqlHiveParser.IF, 0); }
		public TerminalNode NOT() { return getToken(SqlHiveParser.NOT, 0); }
		public TerminalNode EXISTS() { return getToken(SqlHiveParser.EXISTS, 0); }
		public LocationSpecContext locationSpec() {
			return getRuleContext(LocationSpecContext.class,0);
		}
		public CreateTableLikeContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterCreateTableLike(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitCreateTableLike(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitCreateTableLike(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class UncacheTableContext extends StatementContext {
		public TerminalNode UNCACHE() { return getToken(SqlHiveParser.UNCACHE, 0); }
		public TerminalNode TABLE() { return getToken(SqlHiveParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode IF() { return getToken(SqlHiveParser.IF, 0); }
		public TerminalNode EXISTS() { return getToken(SqlHiveParser.EXISTS, 0); }
		public UncacheTableContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterUncacheTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitUncacheTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitUncacheTable(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class DropFunctionContext extends StatementContext {
		public TerminalNode DROP() { return getToken(SqlHiveParser.DROP, 0); }
		public TerminalNode FUNCTION() { return getToken(SqlHiveParser.FUNCTION, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode TEMPORARY() { return getToken(SqlHiveParser.TEMPORARY, 0); }
		public TerminalNode IF() { return getToken(SqlHiveParser.IF, 0); }
		public TerminalNode EXISTS() { return getToken(SqlHiveParser.EXISTS, 0); }
		public DropFunctionContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterDropFunction(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitDropFunction(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitDropFunction(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class LoadDataContext extends StatementContext {
		public Token path;
		public TerminalNode LOAD() { return getToken(SqlHiveParser.LOAD, 0); }
		public TerminalNode DATA() { return getToken(SqlHiveParser.DATA, 0); }
		public TerminalNode INPATH() { return getToken(SqlHiveParser.INPATH, 0); }
		public TerminalNode INTO() { return getToken(SqlHiveParser.INTO, 0); }
		public TerminalNode TABLE() { return getToken(SqlHiveParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode STRING() { return getToken(SqlHiveParser.STRING, 0); }
		public TerminalNode LOCAL() { return getToken(SqlHiveParser.LOCAL, 0); }
		public TerminalNode OVERWRITE() { return getToken(SqlHiveParser.OVERWRITE, 0); }
		public PartitionSpecContext partitionSpec() {
			return getRuleContext(PartitionSpecContext.class,0);
		}
		public LoadDataContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterLoadData(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitLoadData(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitLoadData(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ShowPartitionsContext extends StatementContext {
		public TerminalNode SHOW() { return getToken(SqlHiveParser.SHOW, 0); }
		public TerminalNode PARTITIONS() { return getToken(SqlHiveParser.PARTITIONS, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public PartitionSpecContext partitionSpec() {
			return getRuleContext(PartitionSpecContext.class,0);
		}
		public ShowPartitionsContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterShowPartitions(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitShowPartitions(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitShowPartitions(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class DescribeFunctionContext extends StatementContext {
		public TerminalNode FUNCTION() { return getToken(SqlHiveParser.FUNCTION, 0); }
		public DescribeFuncNameContext describeFuncName() {
			return getRuleContext(DescribeFuncNameContext.class,0);
		}
		public TerminalNode DESC() { return getToken(SqlHiveParser.DESC, 0); }
		public TerminalNode DESCRIBE() { return getToken(SqlHiveParser.DESCRIBE, 0); }
		public TerminalNode EXTENDED() { return getToken(SqlHiveParser.EXTENDED, 0); }
		public DescribeFunctionContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterDescribeFunction(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitDescribeFunction(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitDescribeFunction(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ChangeColumnContext extends StatementContext {
		public TerminalNode ALTER() { return getToken(SqlHiveParser.ALTER, 0); }
		public TerminalNode TABLE() { return getToken(SqlHiveParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode CHANGE() { return getToken(SqlHiveParser.CHANGE, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public ColTypeContext colType() {
			return getRuleContext(ColTypeContext.class,0);
		}
		public PartitionSpecContext partitionSpec() {
			return getRuleContext(PartitionSpecContext.class,0);
		}
		public TerminalNode COLUMN() { return getToken(SqlHiveParser.COLUMN, 0); }
		public ColPositionContext colPosition() {
			return getRuleContext(ColPositionContext.class,0);
		}
		public ChangeColumnContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterChangeColumn(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitChangeColumn(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitChangeColumn(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class DeleteCommandContext extends StatementContext {
		public DeleteContext delete() {
			return getRuleContext(DeleteContext.class,0);
		}
		public DeleteCommandContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterDeleteCommand(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitDeleteCommand(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitDeleteCommand(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class StatementDefaultContext extends StatementContext {
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public StatementDefaultContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterStatementDefault(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitStatementDefault(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitStatementDefault(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class TruncateTableContext extends StatementContext {
		public TerminalNode TRUNCATE() { return getToken(SqlHiveParser.TRUNCATE, 0); }
		public TerminalNode TABLE() { return getToken(SqlHiveParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public PartitionSpecContext partitionSpec() {
			return getRuleContext(PartitionSpecContext.class,0);
		}
		public TruncateTableContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterTruncateTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitTruncateTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitTruncateTable(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class SetTableSerDeContext extends StatementContext {
		public TerminalNode ALTER() { return getToken(SqlHiveParser.ALTER, 0); }
		public TerminalNode TABLE() { return getToken(SqlHiveParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode SET() { return getToken(SqlHiveParser.SET, 0); }
		public TerminalNode SERDE() { return getToken(SqlHiveParser.SERDE, 0); }
		public TerminalNode STRING() { return getToken(SqlHiveParser.STRING, 0); }
		public PartitionSpecContext partitionSpec() {
			return getRuleContext(PartitionSpecContext.class,0);
		}
		public TerminalNode WITH() { return getToken(SqlHiveParser.WITH, 0); }
		public TerminalNode SERDEPROPERTIES() { return getToken(SqlHiveParser.SERDEPROPERTIES, 0); }
		public TablePropertyListContext tablePropertyList() {
			return getRuleContext(TablePropertyListContext.class,0);
		}
		public SetTableSerDeContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterSetTableSerDe(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitSetTableSerDe(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitSetTableSerDe(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class CreateViewContext extends StatementContext {
		public TerminalNode CREATE() { return getToken(SqlHiveParser.CREATE, 0); }
		public TerminalNode VIEW() { return getToken(SqlHiveParser.VIEW, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlHiveParser.AS, 0); }
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public TerminalNode OR() { return getToken(SqlHiveParser.OR, 0); }
		public TerminalNode REPLACE() { return getToken(SqlHiveParser.REPLACE, 0); }
		public TerminalNode TEMPORARY() { return getToken(SqlHiveParser.TEMPORARY, 0); }
		public TerminalNode IF() { return getToken(SqlHiveParser.IF, 0); }
		public TerminalNode NOT() { return getToken(SqlHiveParser.NOT, 0); }
		public TerminalNode EXISTS() { return getToken(SqlHiveParser.EXISTS, 0); }
		public IdentifierCommentListContext identifierCommentList() {
			return getRuleContext(IdentifierCommentListContext.class,0);
		}
		public TerminalNode COMMENT() { return getToken(SqlHiveParser.COMMENT, 0); }
		public TerminalNode STRING() { return getToken(SqlHiveParser.STRING, 0); }
		public TerminalNode PARTITIONED() { return getToken(SqlHiveParser.PARTITIONED, 0); }
		public TerminalNode ON() { return getToken(SqlHiveParser.ON, 0); }
		public IdentifierListContext identifierList() {
			return getRuleContext(IdentifierListContext.class,0);
		}
		public TerminalNode TBLPROPERTIES() { return getToken(SqlHiveParser.TBLPROPERTIES, 0); }
		public TablePropertyListContext tablePropertyList() {
			return getRuleContext(TablePropertyListContext.class,0);
		}
		public TerminalNode GLOBAL() { return getToken(SqlHiveParser.GLOBAL, 0); }
		public CreateViewContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterCreateView(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitCreateView(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitCreateView(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class DropTablePartitionsContext extends StatementContext {
		public TerminalNode ALTER() { return getToken(SqlHiveParser.ALTER, 0); }
		public TerminalNode TABLE() { return getToken(SqlHiveParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode DROP() { return getToken(SqlHiveParser.DROP, 0); }
		public List<PartitionSpecContext> partitionSpec() {
			return getRuleContexts(PartitionSpecContext.class);
		}
		public PartitionSpecContext partitionSpec(int i) {
			return getRuleContext(PartitionSpecContext.class,i);
		}
		public TerminalNode IF() { return getToken(SqlHiveParser.IF, 0); }
		public TerminalNode EXISTS() { return getToken(SqlHiveParser.EXISTS, 0); }
		public TerminalNode PURGE() { return getToken(SqlHiveParser.PURGE, 0); }
		public TerminalNode VIEW() { return getToken(SqlHiveParser.VIEW, 0); }
		public DropTablePartitionsContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterDropTablePartitions(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitDropTablePartitions(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitDropTablePartitions(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class SetConfigurationContext extends StatementContext {
		public TerminalNode SET() { return getToken(SqlHiveParser.SET, 0); }
		public SetConfigurationContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterSetConfiguration(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitSetConfiguration(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitSetConfiguration(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class DropTableContext extends StatementContext {
		public TerminalNode DROP() { return getToken(SqlHiveParser.DROP, 0); }
		public TerminalNode TABLE() { return getToken(SqlHiveParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode IF() { return getToken(SqlHiveParser.IF, 0); }
		public TerminalNode EXISTS() { return getToken(SqlHiveParser.EXISTS, 0); }
		public TerminalNode PURGE() { return getToken(SqlHiveParser.PURGE, 0); }
		public TerminalNode VIEW() { return getToken(SqlHiveParser.VIEW, 0); }
		public DropTableContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterDropTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitDropTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitDropTable(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ShowDatabasesContext extends StatementContext {
		public Token pattern;
		public TerminalNode SHOW() { return getToken(SqlHiveParser.SHOW, 0); }
		public TerminalNode DATABASES() { return getToken(SqlHiveParser.DATABASES, 0); }
		public TerminalNode STRING() { return getToken(SqlHiveParser.STRING, 0); }
		public TerminalNode LIKE() { return getToken(SqlHiveParser.LIKE, 0); }
		public ShowDatabasesContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterShowDatabases(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitShowDatabases(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitShowDatabases(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ShowTblPropertiesContext extends StatementContext {
		public TableIdentifierContext table;
		public TablePropertyKeyContext key;
		public TerminalNode SHOW() { return getToken(SqlHiveParser.SHOW, 0); }
		public TerminalNode TBLPROPERTIES() { return getToken(SqlHiveParser.TBLPROPERTIES, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TablePropertyKeyContext tablePropertyKey() {
			return getRuleContext(TablePropertyKeyContext.class,0);
		}
		public ShowTblPropertiesContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterShowTblProperties(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitShowTblProperties(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitShowTblProperties(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class UnsetTablePropertiesContext extends StatementContext {
		public TerminalNode ALTER() { return getToken(SqlHiveParser.ALTER, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode UNSET() { return getToken(SqlHiveParser.UNSET, 0); }
		public TerminalNode TBLPROPERTIES() { return getToken(SqlHiveParser.TBLPROPERTIES, 0); }
		public TablePropertyListContext tablePropertyList() {
			return getRuleContext(TablePropertyListContext.class,0);
		}
		public TerminalNode TABLE() { return getToken(SqlHiveParser.TABLE, 0); }
		public TerminalNode VIEW() { return getToken(SqlHiveParser.VIEW, 0); }
		public TerminalNode IF() { return getToken(SqlHiveParser.IF, 0); }
		public TerminalNode EXISTS() { return getToken(SqlHiveParser.EXISTS, 0); }
		public UnsetTablePropertiesContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterUnsetTableProperties(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitUnsetTableProperties(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitUnsetTableProperties(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class SetTableLocationContext extends StatementContext {
		public TerminalNode ALTER() { return getToken(SqlHiveParser.ALTER, 0); }
		public TerminalNode TABLE() { return getToken(SqlHiveParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode SET() { return getToken(SqlHiveParser.SET, 0); }
		public LocationSpecContext locationSpec() {
			return getRuleContext(LocationSpecContext.class,0);
		}
		public PartitionSpecContext partitionSpec() {
			return getRuleContext(PartitionSpecContext.class,0);
		}
		public SetTableLocationContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterSetTableLocation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitSetTableLocation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitSetTableLocation(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ShowFunctionsContext extends StatementContext {
		public Token pattern;
		public TerminalNode SHOW() { return getToken(SqlHiveParser.SHOW, 0); }
		public TerminalNode FUNCTIONS() { return getToken(SqlHiveParser.FUNCTIONS, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode LIKE() { return getToken(SqlHiveParser.LIKE, 0); }
		public TerminalNode STRING() { return getToken(SqlHiveParser.STRING, 0); }
		public ShowFunctionsContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterShowFunctions(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitShowFunctions(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitShowFunctions(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class CacheTableContext extends StatementContext {
		public TerminalNode CACHE() { return getToken(SqlHiveParser.CACHE, 0); }
		public TerminalNode TABLE() { return getToken(SqlHiveParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode LAZY() { return getToken(SqlHiveParser.LAZY, 0); }
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlHiveParser.AS, 0); }
		public CacheTableContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterCacheTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitCacheTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitCacheTable(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class AddTableColumnsContext extends StatementContext {
		public ColTypeListContext columns;
		public TerminalNode ALTER() { return getToken(SqlHiveParser.ALTER, 0); }
		public TerminalNode TABLE() { return getToken(SqlHiveParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode ADD() { return getToken(SqlHiveParser.ADD, 0); }
		public TerminalNode COLUMNS() { return getToken(SqlHiveParser.COLUMNS, 0); }
		public ColTypeListContext colTypeList() {
			return getRuleContext(ColTypeListContext.class,0);
		}
		public AddTableColumnsContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterAddTableColumns(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitAddTableColumns(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitAddTableColumns(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class SetTablePropertiesContext extends StatementContext {
		public TerminalNode ALTER() { return getToken(SqlHiveParser.ALTER, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode SET() { return getToken(SqlHiveParser.SET, 0); }
		public TerminalNode TBLPROPERTIES() { return getToken(SqlHiveParser.TBLPROPERTIES, 0); }
		public TablePropertyListContext tablePropertyList() {
			return getRuleContext(TablePropertyListContext.class,0);
		}
		public TerminalNode TABLE() { return getToken(SqlHiveParser.TABLE, 0); }
		public TerminalNode VIEW() { return getToken(SqlHiveParser.VIEW, 0); }
		public SetTablePropertiesContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterSetTableProperties(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitSetTableProperties(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitSetTableProperties(this);
			else return visitor.visitChildren(this);
		}
	}

	public final StatementContext statement() throws RecognitionException {
		StatementContext _localctx = new StatementContext(_ctx, getState());
		enterRule(_localctx, 12, RULE_statement);
		int _la;
		try {
			int _alt;
			setState(836);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,95,_ctx) ) {
			case 1:
				_localctx = new StatementDefaultContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(228);
				query();
				}
				break;
			case 2:
				_localctx = new DeleteCommandContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(229);
				delete();
				}
				break;
			case 3:
				_localctx = new UpdateCommandContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(230);
				update();
				}
				break;
			case 4:
				_localctx = new UseContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(231);
				match(USE);
				setState(232);
				((UseContext)_localctx).db = identifier();
				}
				break;
			case 5:
				_localctx = new CreateDatabaseContext(_localctx);
				enterOuterAlt(_localctx, 5);
				{
				setState(233);
				match(CREATE);
				setState(234);
				match(DATABASE);
				setState(238);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,0,_ctx) ) {
				case 1:
					{
					setState(235);
					match(IF);
					setState(236);
					match(NOT);
					setState(237);
					match(EXISTS);
					}
					break;
				}
				setState(240);
				identifier();
				setState(243);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==COMMENT) {
					{
					setState(241);
					match(COMMENT);
					setState(242);
					((CreateDatabaseContext)_localctx).comment = match(STRING);
					}
				}

				setState(246);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LOCATION) {
					{
					setState(245);
					locationSpec();
					}
				}

				setState(251);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==WITH) {
					{
					setState(248);
					match(WITH);
					setState(249);
					match(DBPROPERTIES);
					setState(250);
					tablePropertyList();
					}
				}

				}
				break;
			case 6:
				_localctx = new SetDatabasePropertiesContext(_localctx);
				enterOuterAlt(_localctx, 6);
				{
				setState(253);
				match(ALTER);
				setState(254);
				match(DATABASE);
				setState(255);
				identifier();
				setState(256);
				match(SET);
				setState(257);
				match(DBPROPERTIES);
				setState(258);
				tablePropertyList();
				}
				break;
			case 7:
				_localctx = new DropDatabaseContext(_localctx);
				enterOuterAlt(_localctx, 7);
				{
				setState(260);
				match(DROP);
				setState(261);
				match(DATABASE);
				setState(264);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,4,_ctx) ) {
				case 1:
					{
					setState(262);
					match(IF);
					setState(263);
					match(EXISTS);
					}
					break;
				}
				setState(266);
				identifier();
				setState(268);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==CASCADE || _la==RESTRICT) {
					{
					setState(267);
					_la = _input.LA(1);
					if ( !(_la==CASCADE || _la==RESTRICT) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
				}

				}
				break;
			case 8:
				_localctx = new CreateTableContext(_localctx);
				enterOuterAlt(_localctx, 8);
				{
				setState(270);
				createTableHeader();
				setState(275);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__0) {
					{
					setState(271);
					match(T__0);
					setState(272);
					colTypeList();
					setState(273);
					match(T__1);
					}
				}

				setState(277);
				tableProvider();
				setState(291);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMENT || ((((_la - 184)) & ~0x3f) == 0 && ((1L << (_la - 184)) & ((1L << (OPTIONS - 184)) | (1L << (TBLPROPERTIES - 184)) | (1L << (LOCATION - 184)) | (1L << (CLUSTERED - 184)) | (1L << (PARTITIONED - 184)))) != 0)) {
					{
					setState(289);
					_errHandler.sync(this);
					switch (_input.LA(1)) {
					case OPTIONS:
						{
						{
						setState(278);
						match(OPTIONS);
						setState(279);
						((CreateTableContext)_localctx).options = tablePropertyList();
						}
						}
						break;
					case PARTITIONED:
						{
						{
						setState(280);
						match(PARTITIONED);
						setState(281);
						match(BY);
						setState(282);
						((CreateTableContext)_localctx).partitionColumnNames = identifierList();
						}
						}
						break;
					case CLUSTERED:
						{
						setState(283);
						bucketSpec();
						}
						break;
					case LOCATION:
						{
						setState(284);
						locationSpec();
						}
						break;
					case COMMENT:
						{
						{
						setState(285);
						match(COMMENT);
						setState(286);
						((CreateTableContext)_localctx).comment = match(STRING);
						}
						}
						break;
					case TBLPROPERTIES:
						{
						{
						setState(287);
						match(TBLPROPERTIES);
						setState(288);
						((CreateTableContext)_localctx).tableProps = tablePropertyList();
						}
						}
						break;
					default:
						throw new NoViableAltException(this);
					}
					}
					setState(293);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(298);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__0) | (1L << SELECT) | (1L << FROM) | (1L << AS))) != 0) || ((((_la - 77)) & ~0x3f) == 0 && ((1L << (_la - 77)) & ((1L << (WITH - 77)) | (1L << (VALUES - 77)) | (1L << (TABLE - 77)) | (1L << (INSERT - 77)) | (1L << (MAP - 77)))) != 0) || _la==REDUCE) {
					{
					setState(295);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==AS) {
						{
						setState(294);
						match(AS);
						}
					}

					setState(297);
					query();
					}
				}

				}
				break;
			case 9:
				_localctx = new CreateHiveTableContext(_localctx);
				enterOuterAlt(_localctx, 9);
				{
				setState(300);
				createTableHeader();
				setState(305);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,11,_ctx) ) {
				case 1:
					{
					setState(301);
					match(T__0);
					setState(302);
					((CreateHiveTableContext)_localctx).columns = colTypeList();
					setState(303);
					match(T__1);
					}
					break;
				}
				setState(324);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==ROW || _la==COMMENT || ((((_la - 186)) & ~0x3f) == 0 && ((1L << (_la - 186)) & ((1L << (TBLPROPERTIES - 186)) | (1L << (SKEWED - 186)) | (1L << (STORED - 186)) | (1L << (LOCATION - 186)) | (1L << (CLUSTERED - 186)) | (1L << (PARTITIONED - 186)))) != 0)) {
					{
					setState(322);
					_errHandler.sync(this);
					switch (_input.LA(1)) {
					case COMMENT:
						{
						{
						setState(307);
						match(COMMENT);
						setState(308);
						((CreateHiveTableContext)_localctx).comment = match(STRING);
						}
						}
						break;
					case PARTITIONED:
						{
						{
						setState(309);
						match(PARTITIONED);
						setState(310);
						match(BY);
						setState(311);
						match(T__0);
						setState(312);
						((CreateHiveTableContext)_localctx).partitionColumns = colTypeList();
						setState(313);
						match(T__1);
						}
						}
						break;
					case CLUSTERED:
						{
						setState(315);
						bucketSpec();
						}
						break;
					case SKEWED:
						{
						setState(316);
						skewSpec();
						}
						break;
					case ROW:
						{
						setState(317);
						rowFormat();
						}
						break;
					case STORED:
						{
						setState(318);
						createFileFormat();
						}
						break;
					case LOCATION:
						{
						setState(319);
						locationSpec();
						}
						break;
					case TBLPROPERTIES:
						{
						{
						setState(320);
						match(TBLPROPERTIES);
						setState(321);
						((CreateHiveTableContext)_localctx).tableProps = tablePropertyList();
						}
						}
						break;
					default:
						throw new NoViableAltException(this);
					}
					}
					setState(326);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(331);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__0) | (1L << SELECT) | (1L << FROM) | (1L << AS))) != 0) || ((((_la - 77)) & ~0x3f) == 0 && ((1L << (_la - 77)) & ((1L << (WITH - 77)) | (1L << (VALUES - 77)) | (1L << (TABLE - 77)) | (1L << (INSERT - 77)) | (1L << (MAP - 77)))) != 0) || _la==REDUCE) {
					{
					setState(328);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==AS) {
						{
						setState(327);
						match(AS);
						}
					}

					setState(330);
					query();
					}
				}

				}
				break;
			case 10:
				_localctx = new CreateTableLikeContext(_localctx);
				enterOuterAlt(_localctx, 10);
				{
				setState(333);
				match(CREATE);
				setState(334);
				match(TABLE);
				setState(338);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,16,_ctx) ) {
				case 1:
					{
					setState(335);
					match(IF);
					setState(336);
					match(NOT);
					setState(337);
					match(EXISTS);
					}
					break;
				}
				setState(340);
				((CreateTableLikeContext)_localctx).target = tableIdentifier();
				setState(341);
				match(LIKE);
				setState(342);
				((CreateTableLikeContext)_localctx).source = tableIdentifier();
				setState(344);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LOCATION) {
					{
					setState(343);
					locationSpec();
					}
				}

				}
				break;
			case 11:
				_localctx = new AnalyzeContext(_localctx);
				enterOuterAlt(_localctx, 11);
				{
				setState(346);
				match(ANALYZE);
				setState(347);
				match(TABLE);
				setState(348);
				tableIdentifier();
				setState(350);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(349);
					partitionSpec();
					}
				}

				setState(352);
				match(COMPUTE);
				setState(353);
				match(STATISTICS);
				setState(358);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,19,_ctx) ) {
				case 1:
					{
					setState(354);
					identifier();
					}
					break;
				case 2:
					{
					setState(355);
					match(FOR);
					setState(356);
					match(COLUMNS);
					setState(357);
					identifierSeq();
					}
					break;
				}
				}
				break;
			case 12:
				_localctx = new AddTableColumnsContext(_localctx);
				enterOuterAlt(_localctx, 12);
				{
				setState(360);
				match(ALTER);
				setState(361);
				match(TABLE);
				setState(362);
				tableIdentifier();
				setState(363);
				match(ADD);
				setState(364);
				match(COLUMNS);
				setState(365);
				match(T__0);
				setState(366);
				((AddTableColumnsContext)_localctx).columns = colTypeList();
				setState(367);
				match(T__1);
				}
				break;
			case 13:
				_localctx = new RenameTableContext(_localctx);
				enterOuterAlt(_localctx, 13);
				{
				setState(369);
				match(ALTER);
				setState(370);
				_la = _input.LA(1);
				if ( !(_la==TABLE || _la==VIEW) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(371);
				((RenameTableContext)_localctx).from = tableIdentifier();
				setState(372);
				match(RENAME);
				setState(373);
				match(TO);
				setState(374);
				((RenameTableContext)_localctx).to = tableIdentifier();
				}
				break;
			case 14:
				_localctx = new SetTablePropertiesContext(_localctx);
				enterOuterAlt(_localctx, 14);
				{
				setState(376);
				match(ALTER);
				setState(377);
				_la = _input.LA(1);
				if ( !(_la==TABLE || _la==VIEW) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(378);
				tableIdentifier();
				setState(379);
				match(SET);
				setState(380);
				match(TBLPROPERTIES);
				setState(381);
				tablePropertyList();
				}
				break;
			case 15:
				_localctx = new UnsetTablePropertiesContext(_localctx);
				enterOuterAlt(_localctx, 15);
				{
				setState(383);
				match(ALTER);
				setState(384);
				_la = _input.LA(1);
				if ( !(_la==TABLE || _la==VIEW) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(385);
				tableIdentifier();
				setState(386);
				match(UNSET);
				setState(387);
				match(TBLPROPERTIES);
				setState(390);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==IF) {
					{
					setState(388);
					match(IF);
					setState(389);
					match(EXISTS);
					}
				}

				setState(392);
				tablePropertyList();
				}
				break;
			case 16:
				_localctx = new ChangeColumnContext(_localctx);
				enterOuterAlt(_localctx, 16);
				{
				setState(394);
				match(ALTER);
				setState(395);
				match(TABLE);
				setState(396);
				tableIdentifier();
				setState(398);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(397);
					partitionSpec();
					}
				}

				setState(400);
				match(CHANGE);
				setState(402);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,22,_ctx) ) {
				case 1:
					{
					setState(401);
					match(COLUMN);
					}
					break;
				}
				setState(404);
				identifier();
				setState(405);
				colType();
				setState(407);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==FIRST || _la==AFTER) {
					{
					setState(406);
					colPosition();
					}
				}

				}
				break;
			case 17:
				_localctx = new SetTableSerDeContext(_localctx);
				enterOuterAlt(_localctx, 17);
				{
				setState(409);
				match(ALTER);
				setState(410);
				match(TABLE);
				setState(411);
				tableIdentifier();
				setState(413);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(412);
					partitionSpec();
					}
				}

				setState(415);
				match(SET);
				setState(416);
				match(SERDE);
				setState(417);
				match(STRING);
				setState(421);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==WITH) {
					{
					setState(418);
					match(WITH);
					setState(419);
					match(SERDEPROPERTIES);
					setState(420);
					tablePropertyList();
					}
				}

				}
				break;
			case 18:
				_localctx = new SetTableSerDeContext(_localctx);
				enterOuterAlt(_localctx, 18);
				{
				setState(423);
				match(ALTER);
				setState(424);
				match(TABLE);
				setState(425);
				tableIdentifier();
				setState(427);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(426);
					partitionSpec();
					}
				}

				setState(429);
				match(SET);
				setState(430);
				match(SERDEPROPERTIES);
				setState(431);
				tablePropertyList();
				}
				break;
			case 19:
				_localctx = new AddTablePartitionContext(_localctx);
				enterOuterAlt(_localctx, 19);
				{
				setState(433);
				match(ALTER);
				setState(434);
				match(TABLE);
				setState(435);
				tableIdentifier();
				setState(436);
				match(ADD);
				setState(440);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==IF) {
					{
					setState(437);
					match(IF);
					setState(438);
					match(NOT);
					setState(439);
					match(EXISTS);
					}
				}

				setState(443); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(442);
					partitionSpecLocation();
					}
					}
					setState(445); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==PARTITION );
				}
				break;
			case 20:
				_localctx = new AddTablePartitionContext(_localctx);
				enterOuterAlt(_localctx, 20);
				{
				setState(447);
				match(ALTER);
				setState(448);
				match(VIEW);
				setState(449);
				tableIdentifier();
				setState(450);
				match(ADD);
				setState(454);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==IF) {
					{
					setState(451);
					match(IF);
					setState(452);
					match(NOT);
					setState(453);
					match(EXISTS);
					}
				}

				setState(457); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(456);
					partitionSpec();
					}
					}
					setState(459); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==PARTITION );
				}
				break;
			case 21:
				_localctx = new RenameTablePartitionContext(_localctx);
				enterOuterAlt(_localctx, 21);
				{
				setState(461);
				match(ALTER);
				setState(462);
				match(TABLE);
				setState(463);
				tableIdentifier();
				setState(464);
				((RenameTablePartitionContext)_localctx).from = partitionSpec();
				setState(465);
				match(RENAME);
				setState(466);
				match(TO);
				setState(467);
				((RenameTablePartitionContext)_localctx).to = partitionSpec();
				}
				break;
			case 22:
				_localctx = new DropTablePartitionsContext(_localctx);
				enterOuterAlt(_localctx, 22);
				{
				setState(469);
				match(ALTER);
				setState(470);
				match(TABLE);
				setState(471);
				tableIdentifier();
				setState(472);
				match(DROP);
				setState(475);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==IF) {
					{
					setState(473);
					match(IF);
					setState(474);
					match(EXISTS);
					}
				}

				setState(477);
				partitionSpec();
				setState(482);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__2) {
					{
					{
					setState(478);
					match(T__2);
					setState(479);
					partitionSpec();
					}
					}
					setState(484);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(486);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PURGE) {
					{
					setState(485);
					match(PURGE);
					}
				}

				}
				break;
			case 23:
				_localctx = new DropTablePartitionsContext(_localctx);
				enterOuterAlt(_localctx, 23);
				{
				setState(488);
				match(ALTER);
				setState(489);
				match(VIEW);
				setState(490);
				tableIdentifier();
				setState(491);
				match(DROP);
				setState(494);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==IF) {
					{
					setState(492);
					match(IF);
					setState(493);
					match(EXISTS);
					}
				}

				setState(496);
				partitionSpec();
				setState(501);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__2) {
					{
					{
					setState(497);
					match(T__2);
					setState(498);
					partitionSpec();
					}
					}
					setState(503);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
				break;
			case 24:
				_localctx = new SetTableLocationContext(_localctx);
				enterOuterAlt(_localctx, 24);
				{
				setState(504);
				match(ALTER);
				setState(505);
				match(TABLE);
				setState(506);
				tableIdentifier();
				setState(508);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(507);
					partitionSpec();
					}
				}

				setState(510);
				match(SET);
				setState(511);
				locationSpec();
				}
				break;
			case 25:
				_localctx = new RecoverPartitionsContext(_localctx);
				enterOuterAlt(_localctx, 25);
				{
				setState(513);
				match(ALTER);
				setState(514);
				match(TABLE);
				setState(515);
				tableIdentifier();
				setState(516);
				match(RECOVER);
				setState(517);
				match(PARTITIONS);
				}
				break;
			case 26:
				_localctx = new DropTableContext(_localctx);
				enterOuterAlt(_localctx, 26);
				{
				setState(519);
				match(DROP);
				setState(520);
				match(TABLE);
				setState(523);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,37,_ctx) ) {
				case 1:
					{
					setState(521);
					match(IF);
					setState(522);
					match(EXISTS);
					}
					break;
				}
				setState(525);
				tableIdentifier();
				setState(527);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PURGE) {
					{
					setState(526);
					match(PURGE);
					}
				}

				}
				break;
			case 27:
				_localctx = new DropTableContext(_localctx);
				enterOuterAlt(_localctx, 27);
				{
				setState(529);
				match(DROP);
				setState(530);
				match(VIEW);
				setState(533);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,39,_ctx) ) {
				case 1:
					{
					setState(531);
					match(IF);
					setState(532);
					match(EXISTS);
					}
					break;
				}
				setState(535);
				tableIdentifier();
				}
				break;
			case 28:
				_localctx = new CreateViewContext(_localctx);
				enterOuterAlt(_localctx, 28);
				{
				setState(536);
				match(CREATE);
				setState(539);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OR) {
					{
					setState(537);
					match(OR);
					setState(538);
					match(REPLACE);
					}
				}

				setState(545);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==GLOBAL || _la==TEMPORARY) {
					{
					setState(542);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==GLOBAL) {
						{
						setState(541);
						match(GLOBAL);
						}
					}

					setState(544);
					match(TEMPORARY);
					}
				}

				setState(547);
				match(VIEW);
				setState(551);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,43,_ctx) ) {
				case 1:
					{
					setState(548);
					match(IF);
					setState(549);
					match(NOT);
					setState(550);
					match(EXISTS);
					}
					break;
				}
				setState(553);
				tableIdentifier();
				setState(555);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__0) {
					{
					setState(554);
					identifierCommentList();
					}
				}

				setState(559);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==COMMENT) {
					{
					setState(557);
					match(COMMENT);
					setState(558);
					match(STRING);
					}
				}

				setState(564);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITIONED) {
					{
					setState(561);
					match(PARTITIONED);
					setState(562);
					match(ON);
					setState(563);
					identifierList();
					}
				}

				setState(568);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==TBLPROPERTIES) {
					{
					setState(566);
					match(TBLPROPERTIES);
					setState(567);
					tablePropertyList();
					}
				}

				setState(570);
				match(AS);
				setState(571);
				query();
				}
				break;
			case 29:
				_localctx = new CreateTempViewUsingContext(_localctx);
				enterOuterAlt(_localctx, 29);
				{
				setState(573);
				match(CREATE);
				setState(576);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OR) {
					{
					setState(574);
					match(OR);
					setState(575);
					match(REPLACE);
					}
				}

				setState(579);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==GLOBAL) {
					{
					setState(578);
					match(GLOBAL);
					}
				}

				setState(581);
				match(TEMPORARY);
				setState(582);
				match(VIEW);
				setState(583);
				tableIdentifier();
				setState(588);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__0) {
					{
					setState(584);
					match(T__0);
					setState(585);
					colTypeList();
					setState(586);
					match(T__1);
					}
				}

				setState(590);
				tableProvider();
				setState(593);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OPTIONS) {
					{
					setState(591);
					match(OPTIONS);
					setState(592);
					tablePropertyList();
					}
				}

				}
				break;
			case 30:
				_localctx = new AlterViewQueryContext(_localctx);
				enterOuterAlt(_localctx, 30);
				{
				setState(595);
				match(ALTER);
				setState(596);
				match(VIEW);
				setState(597);
				tableIdentifier();
				setState(599);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==AS) {
					{
					setState(598);
					match(AS);
					}
				}

				setState(601);
				query();
				}
				break;
			case 31:
				_localctx = new CreateFunctionContext(_localctx);
				enterOuterAlt(_localctx, 31);
				{
				setState(603);
				match(CREATE);
				setState(606);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OR) {
					{
					setState(604);
					match(OR);
					setState(605);
					match(REPLACE);
					}
				}

				setState(609);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==TEMPORARY) {
					{
					setState(608);
					match(TEMPORARY);
					}
				}

				setState(611);
				match(FUNCTION);
				setState(615);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,55,_ctx) ) {
				case 1:
					{
					setState(612);
					match(IF);
					setState(613);
					match(NOT);
					setState(614);
					match(EXISTS);
					}
					break;
				}
				setState(617);
				qualifiedName();
				setState(618);
				match(AS);
				setState(619);
				((CreateFunctionContext)_localctx).className = match(STRING);
				setState(629);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==USING) {
					{
					setState(620);
					match(USING);
					setState(621);
					resource();
					setState(626);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__2) {
						{
						{
						setState(622);
						match(T__2);
						setState(623);
						resource();
						}
						}
						setState(628);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				}
				break;
			case 32:
				_localctx = new DropFunctionContext(_localctx);
				enterOuterAlt(_localctx, 32);
				{
				setState(631);
				match(DROP);
				setState(633);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==TEMPORARY) {
					{
					setState(632);
					match(TEMPORARY);
					}
				}

				setState(635);
				match(FUNCTION);
				setState(638);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,59,_ctx) ) {
				case 1:
					{
					setState(636);
					match(IF);
					setState(637);
					match(EXISTS);
					}
					break;
				}
				setState(640);
				qualifiedName();
				}
				break;
			case 33:
				_localctx = new ExplainContext(_localctx);
				enterOuterAlt(_localctx, 33);
				{
				setState(641);
				match(EXPLAIN);
				setState(643);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (((((_la - 91)) & ~0x3f) == 0 && ((1L << (_la - 91)) & ((1L << (LOGICAL - 91)) | (1L << (CODEGEN - 91)) | (1L << (COST - 91)))) != 0) || _la==EXTENDED || _la==FORMATTED) {
					{
					setState(642);
					_la = _input.LA(1);
					if ( !(((((_la - 91)) & ~0x3f) == 0 && ((1L << (_la - 91)) & ((1L << (LOGICAL - 91)) | (1L << (CODEGEN - 91)) | (1L << (COST - 91)))) != 0) || _la==EXTENDED || _la==FORMATTED) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
				}

				setState(645);
				statement();
				}
				break;
			case 34:
				_localctx = new ShowTablesContext(_localctx);
				enterOuterAlt(_localctx, 34);
				{
				setState(646);
				match(SHOW);
				setState(647);
				match(TABLES);
				setState(650);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==FROM || _la==IN) {
					{
					setState(648);
					_la = _input.LA(1);
					if ( !(_la==FROM || _la==IN) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					setState(649);
					((ShowTablesContext)_localctx).db = identifier();
					}
				}

				setState(656);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LIKE || _la==STRING) {
					{
					setState(653);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==LIKE) {
						{
						setState(652);
						match(LIKE);
						}
					}

					setState(655);
					((ShowTablesContext)_localctx).pattern = match(STRING);
					}
				}

				}
				break;
			case 35:
				_localctx = new ShowTableContext(_localctx);
				enterOuterAlt(_localctx, 35);
				{
				setState(658);
				match(SHOW);
				setState(659);
				match(TABLE);
				setState(660);
				match(EXTENDED);
				setState(663);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==FROM || _la==IN) {
					{
					setState(661);
					_la = _input.LA(1);
					if ( !(_la==FROM || _la==IN) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					setState(662);
					((ShowTableContext)_localctx).db = identifier();
					}
				}

				setState(665);
				match(LIKE);
				setState(666);
				((ShowTableContext)_localctx).pattern = match(STRING);
				setState(668);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(667);
					partitionSpec();
					}
				}

				}
				break;
			case 36:
				_localctx = new ShowDatabasesContext(_localctx);
				enterOuterAlt(_localctx, 36);
				{
				setState(670);
				match(SHOW);
				setState(671);
				match(DATABASES);
				setState(676);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LIKE || _la==STRING) {
					{
					setState(673);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==LIKE) {
						{
						setState(672);
						match(LIKE);
						}
					}

					setState(675);
					((ShowDatabasesContext)_localctx).pattern = match(STRING);
					}
				}

				}
				break;
			case 37:
				_localctx = new ShowTblPropertiesContext(_localctx);
				enterOuterAlt(_localctx, 37);
				{
				setState(678);
				match(SHOW);
				setState(679);
				match(TBLPROPERTIES);
				setState(680);
				((ShowTblPropertiesContext)_localctx).table = tableIdentifier();
				setState(685);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__0) {
					{
					setState(681);
					match(T__0);
					setState(682);
					((ShowTblPropertiesContext)_localctx).key = tablePropertyKey();
					setState(683);
					match(T__1);
					}
				}

				}
				break;
			case 38:
				_localctx = new ShowColumnsContext(_localctx);
				enterOuterAlt(_localctx, 38);
				{
				setState(687);
				match(SHOW);
				setState(688);
				match(COLUMNS);
				setState(689);
				_la = _input.LA(1);
				if ( !(_la==FROM || _la==IN) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(690);
				tableIdentifier();
				setState(693);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==FROM || _la==IN) {
					{
					setState(691);
					_la = _input.LA(1);
					if ( !(_la==FROM || _la==IN) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					setState(692);
					((ShowColumnsContext)_localctx).db = identifier();
					}
				}

				}
				break;
			case 39:
				_localctx = new ShowPartitionsContext(_localctx);
				enterOuterAlt(_localctx, 39);
				{
				setState(695);
				match(SHOW);
				setState(696);
				match(PARTITIONS);
				setState(697);
				tableIdentifier();
				setState(699);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(698);
					partitionSpec();
					}
				}

				}
				break;
			case 40:
				_localctx = new ShowFunctionsContext(_localctx);
				enterOuterAlt(_localctx, 40);
				{
				setState(701);
				match(SHOW);
				setState(703);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,71,_ctx) ) {
				case 1:
					{
					setState(702);
					identifier();
					}
					break;
				}
				setState(705);
				match(FUNCTIONS);
				setState(713);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << SELECT) | (1L << FROM) | (1L << ADD) | (1L << AS) | (1L << ALL) | (1L << ANY) | (1L << DISTINCT) | (1L << WHERE) | (1L << GROUP) | (1L << BY) | (1L << GROUPING) | (1L << SETS) | (1L << CUBE) | (1L << ROLLUP) | (1L << ORDER) | (1L << HAVING) | (1L << LIMIT) | (1L << AT) | (1L << OR) | (1L << AND) | (1L << IN) | (1L << NOT) | (1L << NO) | (1L << EXISTS) | (1L << BETWEEN) | (1L << LIKE) | (1L << RLIKE) | (1L << IS) | (1L << NULL) | (1L << TRUE) | (1L << FALSE) | (1L << NULLS) | (1L << ASC) | (1L << DESC) | (1L << FOR) | (1L << INTERVAL) | (1L << CASE) | (1L << WHEN) | (1L << THEN) | (1L << ELSE) | (1L << END) | (1L << JOIN) | (1L << CROSS) | (1L << OUTER) | (1L << INNER) | (1L << LEFT) | (1L << SEMI) | (1L << RIGHT) | (1L << FULL) | (1L << NATURAL) | (1L << ON) | (1L << PIVOT) | (1L << LATERAL))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (WINDOW - 64)) | (1L << (OVER - 64)) | (1L << (PARTITION - 64)) | (1L << (RANGE - 64)) | (1L << (ROWS - 64)) | (1L << (UNBOUNDED - 64)) | (1L << (PRECEDING - 64)) | (1L << (FOLLOWING - 64)) | (1L << (CURRENT - 64)) | (1L << (FIRST - 64)) | (1L << (AFTER - 64)) | (1L << (LAST - 64)) | (1L << (ROW - 64)) | (1L << (WITH - 64)) | (1L << (VALUES - 64)) | (1L << (CREATE - 64)) | (1L << (TABLE - 64)) | (1L << (DIRECTORY - 64)) | (1L << (VIEW - 64)) | (1L << (REPLACE - 64)) | (1L << (INSERT - 64)) | (1L << (UPDATE - 64)) | (1L << (DELETE - 64)) | (1L << (INTO - 64)) | (1L << (DESCRIBE - 64)) | (1L << (EXPLAIN - 64)) | (1L << (FORMAT - 64)) | (1L << (LOGICAL - 64)) | (1L << (CODEGEN - 64)) | (1L << (COST - 64)) | (1L << (CAST - 64)) | (1L << (SHOW - 64)) | (1L << (TABLES - 64)) | (1L << (COLUMNS - 64)) | (1L << (COLUMN - 64)) | (1L << (USE - 64)) | (1L << (PARTITIONS - 64)) | (1L << (FUNCTIONS - 64)) | (1L << (DROP - 64)) | (1L << (UNION - 64)) | (1L << (EXCEPT - 64)) | (1L << (SETMINUS - 64)) | (1L << (INTERSECT - 64)) | (1L << (TO - 64)) | (1L << (TABLESAMPLE - 64)) | (1L << (STRATIFY - 64)) | (1L << (ALTER - 64)) | (1L << (RENAME - 64)) | (1L << (ARRAY - 64)) | (1L << (MAP - 64)) | (1L << (STRUCT - 64)) | (1L << (COMMENT - 64)) | (1L << (SET - 64)) | (1L << (RESET - 64)) | (1L << (DATA - 64)) | (1L << (START - 64)) | (1L << (TRANSACTION - 64)) | (1L << (COMMIT - 64)) | (1L << (ROLLBACK - 64)) | (1L << (MACRO - 64)) | (1L << (IGNORE - 64)) | (1L << (BOTH - 64)) | (1L << (LEADING - 64)) | (1L << (TRAILING - 64)))) != 0) || ((((_la - 128)) & ~0x3f) == 0 && ((1L << (_la - 128)) & ((1L << (IF - 128)) | (1L << (POSITION - 128)) | (1L << (EXTRACT - 128)) | (1L << (DIV - 128)) | (1L << (PERCENTLIT - 128)) | (1L << (BUCKET - 128)) | (1L << (OUT - 128)) | (1L << (OF - 128)) | (1L << (SORT - 128)) | (1L << (CLUSTER - 128)) | (1L << (DISTRIBUTE - 128)) | (1L << (OVERWRITE - 128)) | (1L << (TRANSFORM - 128)) | (1L << (REDUCE - 128)) | (1L << (SERDE - 128)) | (1L << (SERDEPROPERTIES - 128)) | (1L << (RECORDREADER - 128)) | (1L << (RECORDWRITER - 128)) | (1L << (DELIMITED - 128)) | (1L << (FIELDS - 128)) | (1L << (TERMINATED - 128)) | (1L << (COLLECTION - 128)) | (1L << (ITEMS - 128)) | (1L << (KEYS - 128)) | (1L << (ESCAPED - 128)) | (1L << (LINES - 128)) | (1L << (SEPARATED - 128)) | (1L << (FUNCTION - 128)) | (1L << (EXTENDED - 128)) | (1L << (REFRESH - 128)) | (1L << (CLEAR - 128)) | (1L << (CACHE - 128)) | (1L << (UNCACHE - 128)) | (1L << (LAZY - 128)) | (1L << (FORMATTED - 128)) | (1L << (GLOBAL - 128)) | (1L << (TEMPORARY - 128)) | (1L << (OPTIONS - 128)) | (1L << (UNSET - 128)) | (1L << (TBLPROPERTIES - 128)) | (1L << (DBPROPERTIES - 128)) | (1L << (BUCKETS - 128)) | (1L << (SKEWED - 128)) | (1L << (STORED - 128)) | (1L << (DIRECTORIES - 128)))) != 0) || ((((_la - 192)) & ~0x3f) == 0 && ((1L << (_la - 192)) & ((1L << (LOCATION - 192)) | (1L << (EXCHANGE - 192)) | (1L << (ARCHIVE - 192)) | (1L << (UNARCHIVE - 192)) | (1L << (FILEFORMAT - 192)) | (1L << (TOUCH - 192)) | (1L << (COMPACT - 192)) | (1L << (CONCATENATE - 192)) | (1L << (CHANGE - 192)) | (1L << (CASCADE - 192)) | (1L << (RESTRICT - 192)) | (1L << (CLUSTERED - 192)) | (1L << (SORTED - 192)) | (1L << (PURGE - 192)) | (1L << (INPUTFORMAT - 192)) | (1L << (OUTPUTFORMAT - 192)) | (1L << (DATABASE - 192)) | (1L << (DATABASES - 192)) | (1L << (DFS - 192)) | (1L << (TRUNCATE - 192)) | (1L << (ANALYZE - 192)) | (1L << (COMPUTE - 192)) | (1L << (LIST - 192)) | (1L << (STATISTICS - 192)) | (1L << (PARTITIONED - 192)) | (1L << (EXTERNAL - 192)) | (1L << (DEFINED - 192)) | (1L << (REVOKE - 192)) | (1L << (GRANT - 192)) | (1L << (LOCK - 192)) | (1L << (UNLOCK - 192)) | (1L << (MSCK - 192)) | (1L << (REPAIR - 192)) | (1L << (RECOVER - 192)) | (1L << (EXPORT - 192)) | (1L << (IMPORT - 192)) | (1L << (LOAD - 192)) | (1L << (ROLE - 192)) | (1L << (ROLES - 192)) | (1L << (COMPACTIONS - 192)) | (1L << (PRINCIPALS - 192)) | (1L << (TRANSACTIONS - 192)) | (1L << (INDEX - 192)) | (1L << (INDEXES - 192)) | (1L << (LOCKS - 192)) | (1L << (OPTION - 192)) | (1L << (ANTI - 192)) | (1L << (LOCAL - 192)) | (1L << (INPATH - 192)) | (1L << (STRING - 192)) | (1L << (IDENTIFIER - 192)) | (1L << (BACKQUOTED_IDENTIFIER - 192)))) != 0)) {
					{
					setState(707);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,72,_ctx) ) {
					case 1:
						{
						setState(706);
						match(LIKE);
						}
						break;
					}
					setState(711);
					_errHandler.sync(this);
					switch (_input.LA(1)) {
					case SELECT:
					case FROM:
					case ADD:
					case AS:
					case ALL:
					case ANY:
					case DISTINCT:
					case WHERE:
					case GROUP:
					case BY:
					case GROUPING:
					case SETS:
					case CUBE:
					case ROLLUP:
					case ORDER:
					case HAVING:
					case LIMIT:
					case AT:
					case OR:
					case AND:
					case IN:
					case NOT:
					case NO:
					case EXISTS:
					case BETWEEN:
					case LIKE:
					case RLIKE:
					case IS:
					case NULL:
					case TRUE:
					case FALSE:
					case NULLS:
					case ASC:
					case DESC:
					case FOR:
					case INTERVAL:
					case CASE:
					case WHEN:
					case THEN:
					case ELSE:
					case END:
					case JOIN:
					case CROSS:
					case OUTER:
					case INNER:
					case LEFT:
					case SEMI:
					case RIGHT:
					case FULL:
					case NATURAL:
					case ON:
					case PIVOT:
					case LATERAL:
					case WINDOW:
					case OVER:
					case PARTITION:
					case RANGE:
					case ROWS:
					case UNBOUNDED:
					case PRECEDING:
					case FOLLOWING:
					case CURRENT:
					case FIRST:
					case AFTER:
					case LAST:
					case ROW:
					case WITH:
					case VALUES:
					case CREATE:
					case TABLE:
					case DIRECTORY:
					case VIEW:
					case REPLACE:
					case INSERT:
					case UPDATE:
					case DELETE:
					case INTO:
					case DESCRIBE:
					case EXPLAIN:
					case FORMAT:
					case LOGICAL:
					case CODEGEN:
					case COST:
					case CAST:
					case SHOW:
					case TABLES:
					case COLUMNS:
					case COLUMN:
					case USE:
					case PARTITIONS:
					case FUNCTIONS:
					case DROP:
					case UNION:
					case EXCEPT:
					case SETMINUS:
					case INTERSECT:
					case TO:
					case TABLESAMPLE:
					case STRATIFY:
					case ALTER:
					case RENAME:
					case ARRAY:
					case MAP:
					case STRUCT:
					case COMMENT:
					case SET:
					case RESET:
					case DATA:
					case START:
					case TRANSACTION:
					case COMMIT:
					case ROLLBACK:
					case MACRO:
					case IGNORE:
					case BOTH:
					case LEADING:
					case TRAILING:
					case IF:
					case POSITION:
					case EXTRACT:
					case DIV:
					case PERCENTLIT:
					case BUCKET:
					case OUT:
					case OF:
					case SORT:
					case CLUSTER:
					case DISTRIBUTE:
					case OVERWRITE:
					case TRANSFORM:
					case REDUCE:
					case SERDE:
					case SERDEPROPERTIES:
					case RECORDREADER:
					case RECORDWRITER:
					case DELIMITED:
					case FIELDS:
					case TERMINATED:
					case COLLECTION:
					case ITEMS:
					case KEYS:
					case ESCAPED:
					case LINES:
					case SEPARATED:
					case FUNCTION:
					case EXTENDED:
					case REFRESH:
					case CLEAR:
					case CACHE:
					case UNCACHE:
					case LAZY:
					case FORMATTED:
					case GLOBAL:
					case TEMPORARY:
					case OPTIONS:
					case UNSET:
					case TBLPROPERTIES:
					case DBPROPERTIES:
					case BUCKETS:
					case SKEWED:
					case STORED:
					case DIRECTORIES:
					case LOCATION:
					case EXCHANGE:
					case ARCHIVE:
					case UNARCHIVE:
					case FILEFORMAT:
					case TOUCH:
					case COMPACT:
					case CONCATENATE:
					case CHANGE:
					case CASCADE:
					case RESTRICT:
					case CLUSTERED:
					case SORTED:
					case PURGE:
					case INPUTFORMAT:
					case OUTPUTFORMAT:
					case DATABASE:
					case DATABASES:
					case DFS:
					case TRUNCATE:
					case ANALYZE:
					case COMPUTE:
					case LIST:
					case STATISTICS:
					case PARTITIONED:
					case EXTERNAL:
					case DEFINED:
					case REVOKE:
					case GRANT:
					case LOCK:
					case UNLOCK:
					case MSCK:
					case REPAIR:
					case RECOVER:
					case EXPORT:
					case IMPORT:
					case LOAD:
					case ROLE:
					case ROLES:
					case COMPACTIONS:
					case PRINCIPALS:
					case TRANSACTIONS:
					case INDEX:
					case INDEXES:
					case LOCKS:
					case OPTION:
					case ANTI:
					case LOCAL:
					case INPATH:
					case IDENTIFIER:
					case BACKQUOTED_IDENTIFIER:
						{
						setState(709);
						qualifiedName();
						}
						break;
					case STRING:
						{
						setState(710);
						((ShowFunctionsContext)_localctx).pattern = match(STRING);
						}
						break;
					default:
						throw new NoViableAltException(this);
					}
					}
				}

				}
				break;
			case 41:
				_localctx = new ShowCreateTableContext(_localctx);
				enterOuterAlt(_localctx, 41);
				{
				setState(715);
				match(SHOW);
				setState(716);
				match(CREATE);
				setState(717);
				match(TABLE);
				setState(718);
				tableIdentifier();
				}
				break;
			case 42:
				_localctx = new DescribeFunctionContext(_localctx);
				enterOuterAlt(_localctx, 42);
				{
				setState(719);
				_la = _input.LA(1);
				if ( !(_la==DESC || _la==DESCRIBE) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(720);
				match(FUNCTION);
				setState(722);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,75,_ctx) ) {
				case 1:
					{
					setState(721);
					match(EXTENDED);
					}
					break;
				}
				setState(724);
				describeFuncName();
				}
				break;
			case 43:
				_localctx = new DescribeDatabaseContext(_localctx);
				enterOuterAlt(_localctx, 43);
				{
				setState(725);
				_la = _input.LA(1);
				if ( !(_la==DESC || _la==DESCRIBE) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(726);
				match(DATABASE);
				setState(728);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,76,_ctx) ) {
				case 1:
					{
					setState(727);
					match(EXTENDED);
					}
					break;
				}
				setState(730);
				identifier();
				}
				break;
			case 44:
				_localctx = new DescribeTableContext(_localctx);
				enterOuterAlt(_localctx, 44);
				{
				setState(731);
				_la = _input.LA(1);
				if ( !(_la==DESC || _la==DESCRIBE) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(733);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,77,_ctx) ) {
				case 1:
					{
					setState(732);
					match(TABLE);
					}
					break;
				}
				setState(736);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,78,_ctx) ) {
				case 1:
					{
					setState(735);
					((DescribeTableContext)_localctx).option = _input.LT(1);
					_la = _input.LA(1);
					if ( !(_la==EXTENDED || _la==FORMATTED) ) {
						((DescribeTableContext)_localctx).option = (Token)_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
					break;
				}
				setState(738);
				tableIdentifier();
				setState(740);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,79,_ctx) ) {
				case 1:
					{
					setState(739);
					partitionSpec();
					}
					break;
				}
				setState(743);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << SELECT) | (1L << FROM) | (1L << ADD) | (1L << AS) | (1L << ALL) | (1L << ANY) | (1L << DISTINCT) | (1L << WHERE) | (1L << GROUP) | (1L << BY) | (1L << GROUPING) | (1L << SETS) | (1L << CUBE) | (1L << ROLLUP) | (1L << ORDER) | (1L << HAVING) | (1L << LIMIT) | (1L << AT) | (1L << OR) | (1L << AND) | (1L << IN) | (1L << NOT) | (1L << NO) | (1L << EXISTS) | (1L << BETWEEN) | (1L << LIKE) | (1L << RLIKE) | (1L << IS) | (1L << NULL) | (1L << TRUE) | (1L << FALSE) | (1L << NULLS) | (1L << ASC) | (1L << DESC) | (1L << FOR) | (1L << INTERVAL) | (1L << CASE) | (1L << WHEN) | (1L << THEN) | (1L << ELSE) | (1L << END) | (1L << JOIN) | (1L << CROSS) | (1L << OUTER) | (1L << INNER) | (1L << LEFT) | (1L << SEMI) | (1L << RIGHT) | (1L << FULL) | (1L << NATURAL) | (1L << ON) | (1L << PIVOT) | (1L << LATERAL))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (WINDOW - 64)) | (1L << (OVER - 64)) | (1L << (PARTITION - 64)) | (1L << (RANGE - 64)) | (1L << (ROWS - 64)) | (1L << (UNBOUNDED - 64)) | (1L << (PRECEDING - 64)) | (1L << (FOLLOWING - 64)) | (1L << (CURRENT - 64)) | (1L << (FIRST - 64)) | (1L << (AFTER - 64)) | (1L << (LAST - 64)) | (1L << (ROW - 64)) | (1L << (WITH - 64)) | (1L << (VALUES - 64)) | (1L << (CREATE - 64)) | (1L << (TABLE - 64)) | (1L << (DIRECTORY - 64)) | (1L << (VIEW - 64)) | (1L << (REPLACE - 64)) | (1L << (INSERT - 64)) | (1L << (UPDATE - 64)) | (1L << (DELETE - 64)) | (1L << (INTO - 64)) | (1L << (DESCRIBE - 64)) | (1L << (EXPLAIN - 64)) | (1L << (FORMAT - 64)) | (1L << (LOGICAL - 64)) | (1L << (CODEGEN - 64)) | (1L << (COST - 64)) | (1L << (CAST - 64)) | (1L << (SHOW - 64)) | (1L << (TABLES - 64)) | (1L << (COLUMNS - 64)) | (1L << (COLUMN - 64)) | (1L << (USE - 64)) | (1L << (PARTITIONS - 64)) | (1L << (FUNCTIONS - 64)) | (1L << (DROP - 64)) | (1L << (UNION - 64)) | (1L << (EXCEPT - 64)) | (1L << (SETMINUS - 64)) | (1L << (INTERSECT - 64)) | (1L << (TO - 64)) | (1L << (TABLESAMPLE - 64)) | (1L << (STRATIFY - 64)) | (1L << (ALTER - 64)) | (1L << (RENAME - 64)) | (1L << (ARRAY - 64)) | (1L << (MAP - 64)) | (1L << (STRUCT - 64)) | (1L << (COMMENT - 64)) | (1L << (SET - 64)) | (1L << (RESET - 64)) | (1L << (DATA - 64)) | (1L << (START - 64)) | (1L << (TRANSACTION - 64)) | (1L << (COMMIT - 64)) | (1L << (ROLLBACK - 64)) | (1L << (MACRO - 64)) | (1L << (IGNORE - 64)) | (1L << (BOTH - 64)) | (1L << (LEADING - 64)) | (1L << (TRAILING - 64)))) != 0) || ((((_la - 128)) & ~0x3f) == 0 && ((1L << (_la - 128)) & ((1L << (IF - 128)) | (1L << (POSITION - 128)) | (1L << (EXTRACT - 128)) | (1L << (DIV - 128)) | (1L << (PERCENTLIT - 128)) | (1L << (BUCKET - 128)) | (1L << (OUT - 128)) | (1L << (OF - 128)) | (1L << (SORT - 128)) | (1L << (CLUSTER - 128)) | (1L << (DISTRIBUTE - 128)) | (1L << (OVERWRITE - 128)) | (1L << (TRANSFORM - 128)) | (1L << (REDUCE - 128)) | (1L << (SERDE - 128)) | (1L << (SERDEPROPERTIES - 128)) | (1L << (RECORDREADER - 128)) | (1L << (RECORDWRITER - 128)) | (1L << (DELIMITED - 128)) | (1L << (FIELDS - 128)) | (1L << (TERMINATED - 128)) | (1L << (COLLECTION - 128)) | (1L << (ITEMS - 128)) | (1L << (KEYS - 128)) | (1L << (ESCAPED - 128)) | (1L << (LINES - 128)) | (1L << (SEPARATED - 128)) | (1L << (FUNCTION - 128)) | (1L << (EXTENDED - 128)) | (1L << (REFRESH - 128)) | (1L << (CLEAR - 128)) | (1L << (CACHE - 128)) | (1L << (UNCACHE - 128)) | (1L << (LAZY - 128)) | (1L << (FORMATTED - 128)) | (1L << (GLOBAL - 128)) | (1L << (TEMPORARY - 128)) | (1L << (OPTIONS - 128)) | (1L << (UNSET - 128)) | (1L << (TBLPROPERTIES - 128)) | (1L << (DBPROPERTIES - 128)) | (1L << (BUCKETS - 128)) | (1L << (SKEWED - 128)) | (1L << (STORED - 128)) | (1L << (DIRECTORIES - 128)))) != 0) || ((((_la - 192)) & ~0x3f) == 0 && ((1L << (_la - 192)) & ((1L << (LOCATION - 192)) | (1L << (EXCHANGE - 192)) | (1L << (ARCHIVE - 192)) | (1L << (UNARCHIVE - 192)) | (1L << (FILEFORMAT - 192)) | (1L << (TOUCH - 192)) | (1L << (COMPACT - 192)) | (1L << (CONCATENATE - 192)) | (1L << (CHANGE - 192)) | (1L << (CASCADE - 192)) | (1L << (RESTRICT - 192)) | (1L << (CLUSTERED - 192)) | (1L << (SORTED - 192)) | (1L << (PURGE - 192)) | (1L << (INPUTFORMAT - 192)) | (1L << (OUTPUTFORMAT - 192)) | (1L << (DATABASE - 192)) | (1L << (DATABASES - 192)) | (1L << (DFS - 192)) | (1L << (TRUNCATE - 192)) | (1L << (ANALYZE - 192)) | (1L << (COMPUTE - 192)) | (1L << (LIST - 192)) | (1L << (STATISTICS - 192)) | (1L << (PARTITIONED - 192)) | (1L << (EXTERNAL - 192)) | (1L << (DEFINED - 192)) | (1L << (REVOKE - 192)) | (1L << (GRANT - 192)) | (1L << (LOCK - 192)) | (1L << (UNLOCK - 192)) | (1L << (MSCK - 192)) | (1L << (REPAIR - 192)) | (1L << (RECOVER - 192)) | (1L << (EXPORT - 192)) | (1L << (IMPORT - 192)) | (1L << (LOAD - 192)) | (1L << (ROLE - 192)) | (1L << (ROLES - 192)) | (1L << (COMPACTIONS - 192)) | (1L << (PRINCIPALS - 192)) | (1L << (TRANSACTIONS - 192)) | (1L << (INDEX - 192)) | (1L << (INDEXES - 192)) | (1L << (LOCKS - 192)) | (1L << (OPTION - 192)) | (1L << (ANTI - 192)) | (1L << (LOCAL - 192)) | (1L << (INPATH - 192)) | (1L << (IDENTIFIER - 192)) | (1L << (BACKQUOTED_IDENTIFIER - 192)))) != 0)) {
					{
					setState(742);
					describeColName();
					}
				}

				}
				break;
			case 45:
				_localctx = new RefreshTableContext(_localctx);
				enterOuterAlt(_localctx, 45);
				{
				setState(745);
				match(REFRESH);
				setState(746);
				match(TABLE);
				setState(747);
				tableIdentifier();
				}
				break;
			case 46:
				_localctx = new RefreshResourceContext(_localctx);
				enterOuterAlt(_localctx, 46);
				{
				setState(748);
				match(REFRESH);
				setState(756);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,82,_ctx) ) {
				case 1:
					{
					setState(749);
					match(STRING);
					}
					break;
				case 2:
					{
					setState(753);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,81,_ctx);
					while ( _alt!=1 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
						if ( _alt==1+1 ) {
							{
							{
							setState(750);
							matchWildcard();
							}
							} 
						}
						setState(755);
						_errHandler.sync(this);
						_alt = getInterpreter().adaptivePredict(_input,81,_ctx);
					}
					}
					break;
				}
				}
				break;
			case 47:
				_localctx = new CacheTableContext(_localctx);
				enterOuterAlt(_localctx, 47);
				{
				setState(758);
				match(CACHE);
				setState(760);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LAZY) {
					{
					setState(759);
					match(LAZY);
					}
				}

				setState(762);
				match(TABLE);
				setState(763);
				tableIdentifier();
				setState(768);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__0) | (1L << SELECT) | (1L << FROM) | (1L << AS))) != 0) || ((((_la - 77)) & ~0x3f) == 0 && ((1L << (_la - 77)) & ((1L << (WITH - 77)) | (1L << (VALUES - 77)) | (1L << (TABLE - 77)) | (1L << (INSERT - 77)) | (1L << (MAP - 77)))) != 0) || _la==REDUCE) {
					{
					setState(765);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==AS) {
						{
						setState(764);
						match(AS);
						}
					}

					setState(767);
					query();
					}
				}

				}
				break;
			case 48:
				_localctx = new UncacheTableContext(_localctx);
				enterOuterAlt(_localctx, 48);
				{
				setState(770);
				match(UNCACHE);
				setState(771);
				match(TABLE);
				setState(774);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,86,_ctx) ) {
				case 1:
					{
					setState(772);
					match(IF);
					setState(773);
					match(EXISTS);
					}
					break;
				}
				setState(776);
				tableIdentifier();
				}
				break;
			case 49:
				_localctx = new ClearCacheContext(_localctx);
				enterOuterAlt(_localctx, 49);
				{
				setState(777);
				match(CLEAR);
				setState(778);
				match(CACHE);
				}
				break;
			case 50:
				_localctx = new LoadDataContext(_localctx);
				enterOuterAlt(_localctx, 50);
				{
				setState(779);
				match(LOAD);
				setState(780);
				match(DATA);
				setState(782);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LOCAL) {
					{
					setState(781);
					match(LOCAL);
					}
				}

				setState(784);
				match(INPATH);
				setState(785);
				((LoadDataContext)_localctx).path = match(STRING);
				setState(787);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OVERWRITE) {
					{
					setState(786);
					match(OVERWRITE);
					}
				}

				setState(789);
				match(INTO);
				setState(790);
				match(TABLE);
				setState(791);
				tableIdentifier();
				setState(793);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(792);
					partitionSpec();
					}
				}

				}
				break;
			case 51:
				_localctx = new TruncateTableContext(_localctx);
				enterOuterAlt(_localctx, 51);
				{
				setState(795);
				match(TRUNCATE);
				setState(796);
				match(TABLE);
				setState(797);
				tableIdentifier();
				setState(799);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(798);
					partitionSpec();
					}
				}

				}
				break;
			case 52:
				_localctx = new RepairTableContext(_localctx);
				enterOuterAlt(_localctx, 52);
				{
				setState(801);
				match(MSCK);
				setState(802);
				match(REPAIR);
				setState(803);
				match(TABLE);
				setState(804);
				tableIdentifier();
				}
				break;
			case 53:
				_localctx = new ManageResourceContext(_localctx);
				enterOuterAlt(_localctx, 53);
				{
				setState(805);
				((ManageResourceContext)_localctx).op = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==ADD || _la==LIST) ) {
					((ManageResourceContext)_localctx).op = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(806);
				identifier();
				setState(810);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,91,_ctx);
				while ( _alt!=1 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1+1 ) {
						{
						{
						setState(807);
						matchWildcard();
						}
						} 
					}
					setState(812);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,91,_ctx);
				}
				}
				break;
			case 54:
				_localctx = new FailNativeCommandContext(_localctx);
				enterOuterAlt(_localctx, 54);
				{
				setState(813);
				match(SET);
				setState(814);
				match(ROLE);
				setState(818);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,92,_ctx);
				while ( _alt!=1 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1+1 ) {
						{
						{
						setState(815);
						matchWildcard();
						}
						} 
					}
					setState(820);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,92,_ctx);
				}
				}
				break;
			case 55:
				_localctx = new SetConfigurationContext(_localctx);
				enterOuterAlt(_localctx, 55);
				{
				setState(821);
				match(SET);
				setState(825);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,93,_ctx);
				while ( _alt!=1 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1+1 ) {
						{
						{
						setState(822);
						matchWildcard();
						}
						} 
					}
					setState(827);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,93,_ctx);
				}
				}
				break;
			case 56:
				_localctx = new ResetConfigurationContext(_localctx);
				enterOuterAlt(_localctx, 56);
				{
				setState(828);
				match(RESET);
				}
				break;
			case 57:
				_localctx = new FailNativeCommandContext(_localctx);
				enterOuterAlt(_localctx, 57);
				{
				setState(829);
				unsupportedHiveNativeCommands();
				setState(833);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,94,_ctx);
				while ( _alt!=1 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1+1 ) {
						{
						{
						setState(830);
						matchWildcard();
						}
						} 
					}
					setState(835);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,94,_ctx);
				}
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class UnsupportedHiveNativeCommandsContext extends ParserRuleContext {
		public Token kw1;
		public Token kw2;
		public Token kw3;
		public Token kw4;
		public Token kw5;
		public Token kw6;
		public TerminalNode CREATE() { return getToken(SqlHiveParser.CREATE, 0); }
		public TerminalNode ROLE() { return getToken(SqlHiveParser.ROLE, 0); }
		public TerminalNode DROP() { return getToken(SqlHiveParser.DROP, 0); }
		public TerminalNode GRANT() { return getToken(SqlHiveParser.GRANT, 0); }
		public TerminalNode REVOKE() { return getToken(SqlHiveParser.REVOKE, 0); }
		public TerminalNode SHOW() { return getToken(SqlHiveParser.SHOW, 0); }
		public TerminalNode PRINCIPALS() { return getToken(SqlHiveParser.PRINCIPALS, 0); }
		public TerminalNode ROLES() { return getToken(SqlHiveParser.ROLES, 0); }
		public TerminalNode CURRENT() { return getToken(SqlHiveParser.CURRENT, 0); }
		public TerminalNode EXPORT() { return getToken(SqlHiveParser.EXPORT, 0); }
		public TerminalNode TABLE() { return getToken(SqlHiveParser.TABLE, 0); }
		public TerminalNode IMPORT() { return getToken(SqlHiveParser.IMPORT, 0); }
		public TerminalNode COMPACTIONS() { return getToken(SqlHiveParser.COMPACTIONS, 0); }
		public TerminalNode TRANSACTIONS() { return getToken(SqlHiveParser.TRANSACTIONS, 0); }
		public TerminalNode INDEXES() { return getToken(SqlHiveParser.INDEXES, 0); }
		public TerminalNode LOCKS() { return getToken(SqlHiveParser.LOCKS, 0); }
		public TerminalNode INDEX() { return getToken(SqlHiveParser.INDEX, 0); }
		public TerminalNode ALTER() { return getToken(SqlHiveParser.ALTER, 0); }
		public TerminalNode LOCK() { return getToken(SqlHiveParser.LOCK, 0); }
		public TerminalNode DATABASE() { return getToken(SqlHiveParser.DATABASE, 0); }
		public TerminalNode UNLOCK() { return getToken(SqlHiveParser.UNLOCK, 0); }
		public TerminalNode TEMPORARY() { return getToken(SqlHiveParser.TEMPORARY, 0); }
		public TerminalNode MACRO() { return getToken(SqlHiveParser.MACRO, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode NOT() { return getToken(SqlHiveParser.NOT, 0); }
		public TerminalNode CLUSTERED() { return getToken(SqlHiveParser.CLUSTERED, 0); }
		public TerminalNode BY() { return getToken(SqlHiveParser.BY, 0); }
		public TerminalNode SORTED() { return getToken(SqlHiveParser.SORTED, 0); }
		public TerminalNode SKEWED() { return getToken(SqlHiveParser.SKEWED, 0); }
		public TerminalNode STORED() { return getToken(SqlHiveParser.STORED, 0); }
		public TerminalNode AS() { return getToken(SqlHiveParser.AS, 0); }
		public TerminalNode DIRECTORIES() { return getToken(SqlHiveParser.DIRECTORIES, 0); }
		public TerminalNode SET() { return getToken(SqlHiveParser.SET, 0); }
		public TerminalNode LOCATION() { return getToken(SqlHiveParser.LOCATION, 0); }
		public TerminalNode EXCHANGE() { return getToken(SqlHiveParser.EXCHANGE, 0); }
		public TerminalNode PARTITION() { return getToken(SqlHiveParser.PARTITION, 0); }
		public TerminalNode ARCHIVE() { return getToken(SqlHiveParser.ARCHIVE, 0); }
		public TerminalNode UNARCHIVE() { return getToken(SqlHiveParser.UNARCHIVE, 0); }
		public TerminalNode TOUCH() { return getToken(SqlHiveParser.TOUCH, 0); }
		public TerminalNode COMPACT() { return getToken(SqlHiveParser.COMPACT, 0); }
		public PartitionSpecContext partitionSpec() {
			return getRuleContext(PartitionSpecContext.class,0);
		}
		public TerminalNode CONCATENATE() { return getToken(SqlHiveParser.CONCATENATE, 0); }
		public TerminalNode FILEFORMAT() { return getToken(SqlHiveParser.FILEFORMAT, 0); }
		public TerminalNode REPLACE() { return getToken(SqlHiveParser.REPLACE, 0); }
		public TerminalNode COLUMNS() { return getToken(SqlHiveParser.COLUMNS, 0); }
		public TerminalNode START() { return getToken(SqlHiveParser.START, 0); }
		public TerminalNode TRANSACTION() { return getToken(SqlHiveParser.TRANSACTION, 0); }
		public TerminalNode COMMIT() { return getToken(SqlHiveParser.COMMIT, 0); }
		public TerminalNode ROLLBACK() { return getToken(SqlHiveParser.ROLLBACK, 0); }
		public TerminalNode DFS() { return getToken(SqlHiveParser.DFS, 0); }
		public UnsupportedHiveNativeCommandsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_unsupportedHiveNativeCommands; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterUnsupportedHiveNativeCommands(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitUnsupportedHiveNativeCommands(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitUnsupportedHiveNativeCommands(this);
			else return visitor.visitChildren(this);
		}
	}

	public final UnsupportedHiveNativeCommandsContext unsupportedHiveNativeCommands() throws RecognitionException {
		UnsupportedHiveNativeCommandsContext _localctx = new UnsupportedHiveNativeCommandsContext(_ctx, getState());
		enterRule(_localctx, 14, RULE_unsupportedHiveNativeCommands);
		int _la;
		try {
			setState(1006);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,103,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(838);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(CREATE);
				setState(839);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(ROLE);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(840);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(DROP);
				setState(841);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(ROLE);
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(842);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(GRANT);
				setState(844);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,96,_ctx) ) {
				case 1:
					{
					setState(843);
					((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(ROLE);
					}
					break;
				}
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(846);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(REVOKE);
				setState(848);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,97,_ctx) ) {
				case 1:
					{
					setState(847);
					((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(ROLE);
					}
					break;
				}
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(850);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(SHOW);
				setState(851);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(GRANT);
				}
				break;
			case 6:
				enterOuterAlt(_localctx, 6);
				{
				setState(852);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(SHOW);
				setState(853);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(ROLE);
				setState(855);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,98,_ctx) ) {
				case 1:
					{
					setState(854);
					((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(GRANT);
					}
					break;
				}
				}
				break;
			case 7:
				enterOuterAlt(_localctx, 7);
				{
				setState(857);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(SHOW);
				setState(858);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(PRINCIPALS);
				}
				break;
			case 8:
				enterOuterAlt(_localctx, 8);
				{
				setState(859);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(SHOW);
				setState(860);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(ROLES);
				}
				break;
			case 9:
				enterOuterAlt(_localctx, 9);
				{
				setState(861);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(SHOW);
				setState(862);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(CURRENT);
				setState(863);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(ROLES);
				}
				break;
			case 10:
				enterOuterAlt(_localctx, 10);
				{
				setState(864);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(EXPORT);
				setState(865);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				}
				break;
			case 11:
				enterOuterAlt(_localctx, 11);
				{
				setState(866);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(IMPORT);
				setState(867);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				}
				break;
			case 12:
				enterOuterAlt(_localctx, 12);
				{
				setState(868);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(SHOW);
				setState(869);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(COMPACTIONS);
				}
				break;
			case 13:
				enterOuterAlt(_localctx, 13);
				{
				setState(870);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(SHOW);
				setState(871);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(CREATE);
				setState(872);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(TABLE);
				}
				break;
			case 14:
				enterOuterAlt(_localctx, 14);
				{
				setState(873);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(SHOW);
				setState(874);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TRANSACTIONS);
				}
				break;
			case 15:
				enterOuterAlt(_localctx, 15);
				{
				setState(875);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(SHOW);
				setState(876);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(INDEXES);
				}
				break;
			case 16:
				enterOuterAlt(_localctx, 16);
				{
				setState(877);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(SHOW);
				setState(878);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(LOCKS);
				}
				break;
			case 17:
				enterOuterAlt(_localctx, 17);
				{
				setState(879);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(CREATE);
				setState(880);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(INDEX);
				}
				break;
			case 18:
				enterOuterAlt(_localctx, 18);
				{
				setState(881);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(DROP);
				setState(882);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(INDEX);
				}
				break;
			case 19:
				enterOuterAlt(_localctx, 19);
				{
				setState(883);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(884);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(INDEX);
				}
				break;
			case 20:
				enterOuterAlt(_localctx, 20);
				{
				setState(885);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(LOCK);
				setState(886);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				}
				break;
			case 21:
				enterOuterAlt(_localctx, 21);
				{
				setState(887);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(LOCK);
				setState(888);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(DATABASE);
				}
				break;
			case 22:
				enterOuterAlt(_localctx, 22);
				{
				setState(889);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(UNLOCK);
				setState(890);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				}
				break;
			case 23:
				enterOuterAlt(_localctx, 23);
				{
				setState(891);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(UNLOCK);
				setState(892);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(DATABASE);
				}
				break;
			case 24:
				enterOuterAlt(_localctx, 24);
				{
				setState(893);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(CREATE);
				setState(894);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TEMPORARY);
				setState(895);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(MACRO);
				}
				break;
			case 25:
				enterOuterAlt(_localctx, 25);
				{
				setState(896);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(DROP);
				setState(897);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TEMPORARY);
				setState(898);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(MACRO);
				}
				break;
			case 26:
				enterOuterAlt(_localctx, 26);
				{
				setState(899);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(900);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(901);
				tableIdentifier();
				setState(902);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(NOT);
				setState(903);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw4 = match(CLUSTERED);
				}
				break;
			case 27:
				enterOuterAlt(_localctx, 27);
				{
				setState(905);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(906);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(907);
				tableIdentifier();
				setState(908);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(CLUSTERED);
				setState(909);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw4 = match(BY);
				}
				break;
			case 28:
				enterOuterAlt(_localctx, 28);
				{
				setState(911);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(912);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(913);
				tableIdentifier();
				setState(914);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(NOT);
				setState(915);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw4 = match(SORTED);
				}
				break;
			case 29:
				enterOuterAlt(_localctx, 29);
				{
				setState(917);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(918);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(919);
				tableIdentifier();
				setState(920);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(SKEWED);
				setState(921);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw4 = match(BY);
				}
				break;
			case 30:
				enterOuterAlt(_localctx, 30);
				{
				setState(923);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(924);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(925);
				tableIdentifier();
				setState(926);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(NOT);
				setState(927);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw4 = match(SKEWED);
				}
				break;
			case 31:
				enterOuterAlt(_localctx, 31);
				{
				setState(929);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(930);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(931);
				tableIdentifier();
				setState(932);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(NOT);
				setState(933);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw4 = match(STORED);
				setState(934);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw5 = match(AS);
				setState(935);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw6 = match(DIRECTORIES);
				}
				break;
			case 32:
				enterOuterAlt(_localctx, 32);
				{
				setState(937);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(938);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(939);
				tableIdentifier();
				setState(940);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(SET);
				setState(941);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw4 = match(SKEWED);
				setState(942);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw5 = match(LOCATION);
				}
				break;
			case 33:
				enterOuterAlt(_localctx, 33);
				{
				setState(944);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(945);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(946);
				tableIdentifier();
				setState(947);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(EXCHANGE);
				setState(948);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw4 = match(PARTITION);
				}
				break;
			case 34:
				enterOuterAlt(_localctx, 34);
				{
				setState(950);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(951);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(952);
				tableIdentifier();
				setState(953);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(ARCHIVE);
				setState(954);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw4 = match(PARTITION);
				}
				break;
			case 35:
				enterOuterAlt(_localctx, 35);
				{
				setState(956);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(957);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(958);
				tableIdentifier();
				setState(959);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(UNARCHIVE);
				setState(960);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw4 = match(PARTITION);
				}
				break;
			case 36:
				enterOuterAlt(_localctx, 36);
				{
				setState(962);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(963);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(964);
				tableIdentifier();
				setState(965);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(TOUCH);
				}
				break;
			case 37:
				enterOuterAlt(_localctx, 37);
				{
				setState(967);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(968);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(969);
				tableIdentifier();
				setState(971);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(970);
					partitionSpec();
					}
				}

				setState(973);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(COMPACT);
				}
				break;
			case 38:
				enterOuterAlt(_localctx, 38);
				{
				setState(975);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(976);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(977);
				tableIdentifier();
				setState(979);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(978);
					partitionSpec();
					}
				}

				setState(981);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(CONCATENATE);
				}
				break;
			case 39:
				enterOuterAlt(_localctx, 39);
				{
				setState(983);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(984);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(985);
				tableIdentifier();
				setState(987);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(986);
					partitionSpec();
					}
				}

				setState(989);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(SET);
				setState(990);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw4 = match(FILEFORMAT);
				}
				break;
			case 40:
				enterOuterAlt(_localctx, 40);
				{
				setState(992);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(993);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(994);
				tableIdentifier();
				setState(996);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(995);
					partitionSpec();
					}
				}

				setState(998);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(REPLACE);
				setState(999);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw4 = match(COLUMNS);
				}
				break;
			case 41:
				enterOuterAlt(_localctx, 41);
				{
				setState(1001);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(START);
				setState(1002);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TRANSACTION);
				}
				break;
			case 42:
				enterOuterAlt(_localctx, 42);
				{
				setState(1003);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(COMMIT);
				}
				break;
			case 43:
				enterOuterAlt(_localctx, 43);
				{
				setState(1004);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ROLLBACK);
				}
				break;
			case 44:
				enterOuterAlt(_localctx, 44);
				{
				setState(1005);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(DFS);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class DeleteContext extends ParserRuleContext {
		public BooleanExpressionContext where;
		public TerminalNode DELETE() { return getToken(SqlHiveParser.DELETE, 0); }
		public TerminalNode FROM() { return getToken(SqlHiveParser.FROM, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode WHERE() { return getToken(SqlHiveParser.WHERE, 0); }
		public BooleanExpressionContext booleanExpression() {
			return getRuleContext(BooleanExpressionContext.class,0);
		}
		public DeleteContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_delete; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterDelete(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitDelete(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitDelete(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DeleteContext delete() throws RecognitionException {
		DeleteContext _localctx = new DeleteContext(_ctx, getState());
		enterRule(_localctx, 16, RULE_delete);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1008);
			match(DELETE);
			setState(1009);
			match(FROM);
			setState(1010);
			tableIdentifier();
			setState(1013);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WHERE) {
				{
				setState(1011);
				match(WHERE);
				setState(1012);
				((DeleteContext)_localctx).where = booleanExpression(0);
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class UpdateContext extends ParserRuleContext {
		public BooleanExpressionContext where;
		public TerminalNode UPDATE() { return getToken(SqlHiveParser.UPDATE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode SET() { return getToken(SqlHiveParser.SET, 0); }
		public UpdateFieldListContext updateFieldList() {
			return getRuleContext(UpdateFieldListContext.class,0);
		}
		public TerminalNode WHERE() { return getToken(SqlHiveParser.WHERE, 0); }
		public BooleanExpressionContext booleanExpression() {
			return getRuleContext(BooleanExpressionContext.class,0);
		}
		public UpdateContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_update; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterUpdate(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitUpdate(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitUpdate(this);
			else return visitor.visitChildren(this);
		}
	}

	public final UpdateContext update() throws RecognitionException {
		UpdateContext _localctx = new UpdateContext(_ctx, getState());
		enterRule(_localctx, 18, RULE_update);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1015);
			match(UPDATE);
			setState(1016);
			tableIdentifier();
			setState(1017);
			match(SET);
			setState(1018);
			updateFieldList();
			setState(1021);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WHERE) {
				{
				setState(1019);
				match(WHERE);
				setState(1020);
				((UpdateContext)_localctx).where = booleanExpression(0);
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class UpdateFieldListContext extends ParserRuleContext {
		public List<UpdateFieldContext> updateField() {
			return getRuleContexts(UpdateFieldContext.class);
		}
		public UpdateFieldContext updateField(int i) {
			return getRuleContext(UpdateFieldContext.class,i);
		}
		public UpdateFieldListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_updateFieldList; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterUpdateFieldList(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitUpdateFieldList(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitUpdateFieldList(this);
			else return visitor.visitChildren(this);
		}
	}

	public final UpdateFieldListContext updateFieldList() throws RecognitionException {
		UpdateFieldListContext _localctx = new UpdateFieldListContext(_ctx, getState());
		enterRule(_localctx, 20, RULE_updateFieldList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1023);
			updateField();
			setState(1028);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__2) {
				{
				{
				setState(1024);
				match(T__2);
				setState(1025);
				updateField();
				}
				}
				setState(1030);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class UpdateFieldContext extends ParserRuleContext {
		public IdentifierContext key;
		public ValueExpressionContext value;
		public TerminalNode EQ() { return getToken(SqlHiveParser.EQ, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public ValueExpressionContext valueExpression() {
			return getRuleContext(ValueExpressionContext.class,0);
		}
		public UpdateFieldContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_updateField; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterUpdateField(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitUpdateField(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitUpdateField(this);
			else return visitor.visitChildren(this);
		}
	}

	public final UpdateFieldContext updateField() throws RecognitionException {
		UpdateFieldContext _localctx = new UpdateFieldContext(_ctx, getState());
		enterRule(_localctx, 22, RULE_updateField);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1031);
			((UpdateFieldContext)_localctx).key = identifier();
			setState(1032);
			match(EQ);
			setState(1033);
			((UpdateFieldContext)_localctx).value = valueExpression(0);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class CreateTableHeaderContext extends ParserRuleContext {
		public TerminalNode CREATE() { return getToken(SqlHiveParser.CREATE, 0); }
		public TerminalNode TABLE() { return getToken(SqlHiveParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode TEMPORARY() { return getToken(SqlHiveParser.TEMPORARY, 0); }
		public TerminalNode EXTERNAL() { return getToken(SqlHiveParser.EXTERNAL, 0); }
		public TerminalNode IF() { return getToken(SqlHiveParser.IF, 0); }
		public TerminalNode NOT() { return getToken(SqlHiveParser.NOT, 0); }
		public TerminalNode EXISTS() { return getToken(SqlHiveParser.EXISTS, 0); }
		public CreateTableHeaderContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_createTableHeader; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterCreateTableHeader(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitCreateTableHeader(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitCreateTableHeader(this);
			else return visitor.visitChildren(this);
		}
	}

	public final CreateTableHeaderContext createTableHeader() throws RecognitionException {
		CreateTableHeaderContext _localctx = new CreateTableHeaderContext(_ctx, getState());
		enterRule(_localctx, 24, RULE_createTableHeader);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1035);
			match(CREATE);
			setState(1037);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==TEMPORARY) {
				{
				setState(1036);
				match(TEMPORARY);
				}
			}

			setState(1040);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==EXTERNAL) {
				{
				setState(1039);
				match(EXTERNAL);
				}
			}

			setState(1042);
			match(TABLE);
			setState(1046);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,109,_ctx) ) {
			case 1:
				{
				setState(1043);
				match(IF);
				setState(1044);
				match(NOT);
				setState(1045);
				match(EXISTS);
				}
				break;
			}
			setState(1048);
			tableIdentifier();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class BucketSpecContext extends ParserRuleContext {
		public TerminalNode CLUSTERED() { return getToken(SqlHiveParser.CLUSTERED, 0); }
		public List<TerminalNode> BY() { return getTokens(SqlHiveParser.BY); }
		public TerminalNode BY(int i) {
			return getToken(SqlHiveParser.BY, i);
		}
		public IdentifierListContext identifierList() {
			return getRuleContext(IdentifierListContext.class,0);
		}
		public TerminalNode INTO() { return getToken(SqlHiveParser.INTO, 0); }
		public TerminalNode INTEGER_VALUE() { return getToken(SqlHiveParser.INTEGER_VALUE, 0); }
		public TerminalNode BUCKETS() { return getToken(SqlHiveParser.BUCKETS, 0); }
		public TerminalNode SORTED() { return getToken(SqlHiveParser.SORTED, 0); }
		public OrderedIdentifierListContext orderedIdentifierList() {
			return getRuleContext(OrderedIdentifierListContext.class,0);
		}
		public BucketSpecContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_bucketSpec; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterBucketSpec(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitBucketSpec(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitBucketSpec(this);
			else return visitor.visitChildren(this);
		}
	}

	public final BucketSpecContext bucketSpec() throws RecognitionException {
		BucketSpecContext _localctx = new BucketSpecContext(_ctx, getState());
		enterRule(_localctx, 26, RULE_bucketSpec);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1050);
			match(CLUSTERED);
			setState(1051);
			match(BY);
			setState(1052);
			identifierList();
			setState(1056);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==SORTED) {
				{
				setState(1053);
				match(SORTED);
				setState(1054);
				match(BY);
				setState(1055);
				orderedIdentifierList();
				}
			}

			setState(1058);
			match(INTO);
			setState(1059);
			match(INTEGER_VALUE);
			setState(1060);
			match(BUCKETS);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SkewSpecContext extends ParserRuleContext {
		public TerminalNode SKEWED() { return getToken(SqlHiveParser.SKEWED, 0); }
		public TerminalNode BY() { return getToken(SqlHiveParser.BY, 0); }
		public IdentifierListContext identifierList() {
			return getRuleContext(IdentifierListContext.class,0);
		}
		public TerminalNode ON() { return getToken(SqlHiveParser.ON, 0); }
		public ConstantListContext constantList() {
			return getRuleContext(ConstantListContext.class,0);
		}
		public NestedConstantListContext nestedConstantList() {
			return getRuleContext(NestedConstantListContext.class,0);
		}
		public TerminalNode STORED() { return getToken(SqlHiveParser.STORED, 0); }
		public TerminalNode AS() { return getToken(SqlHiveParser.AS, 0); }
		public TerminalNode DIRECTORIES() { return getToken(SqlHiveParser.DIRECTORIES, 0); }
		public SkewSpecContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_skewSpec; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterSkewSpec(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitSkewSpec(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitSkewSpec(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SkewSpecContext skewSpec() throws RecognitionException {
		SkewSpecContext _localctx = new SkewSpecContext(_ctx, getState());
		enterRule(_localctx, 28, RULE_skewSpec);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1062);
			match(SKEWED);
			setState(1063);
			match(BY);
			setState(1064);
			identifierList();
			setState(1065);
			match(ON);
			setState(1068);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,111,_ctx) ) {
			case 1:
				{
				setState(1066);
				constantList();
				}
				break;
			case 2:
				{
				setState(1067);
				nestedConstantList();
				}
				break;
			}
			setState(1073);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,112,_ctx) ) {
			case 1:
				{
				setState(1070);
				match(STORED);
				setState(1071);
				match(AS);
				setState(1072);
				match(DIRECTORIES);
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class LocationSpecContext extends ParserRuleContext {
		public TerminalNode LOCATION() { return getToken(SqlHiveParser.LOCATION, 0); }
		public TerminalNode STRING() { return getToken(SqlHiveParser.STRING, 0); }
		public LocationSpecContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_locationSpec; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterLocationSpec(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitLocationSpec(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitLocationSpec(this);
			else return visitor.visitChildren(this);
		}
	}

	public final LocationSpecContext locationSpec() throws RecognitionException {
		LocationSpecContext _localctx = new LocationSpecContext(_ctx, getState());
		enterRule(_localctx, 30, RULE_locationSpec);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1075);
			match(LOCATION);
			setState(1076);
			match(STRING);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class QueryContext extends ParserRuleContext {
		public QueryNoWithContext queryNoWith() {
			return getRuleContext(QueryNoWithContext.class,0);
		}
		public CtesContext ctes() {
			return getRuleContext(CtesContext.class,0);
		}
		public QueryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_query; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterQuery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitQuery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitQuery(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QueryContext query() throws RecognitionException {
		QueryContext _localctx = new QueryContext(_ctx, getState());
		enterRule(_localctx, 32, RULE_query);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1079);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WITH) {
				{
				setState(1078);
				ctes();
				}
			}

			setState(1081);
			queryNoWith();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class InsertIntoContext extends ParserRuleContext {
		public InsertIntoContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_insertInto; }
	 
		public InsertIntoContext() { }
		public void copyFrom(InsertIntoContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class InsertOverwriteHiveDirContext extends InsertIntoContext {
		public Token path;
		public TerminalNode INSERT() { return getToken(SqlHiveParser.INSERT, 0); }
		public TerminalNode OVERWRITE() { return getToken(SqlHiveParser.OVERWRITE, 0); }
		public TerminalNode DIRECTORY() { return getToken(SqlHiveParser.DIRECTORY, 0); }
		public TerminalNode STRING() { return getToken(SqlHiveParser.STRING, 0); }
		public TerminalNode LOCAL() { return getToken(SqlHiveParser.LOCAL, 0); }
		public RowFormatContext rowFormat() {
			return getRuleContext(RowFormatContext.class,0);
		}
		public CreateFileFormatContext createFileFormat() {
			return getRuleContext(CreateFileFormatContext.class,0);
		}
		public InsertOverwriteHiveDirContext(InsertIntoContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterInsertOverwriteHiveDir(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitInsertOverwriteHiveDir(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitInsertOverwriteHiveDir(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class InsertOverwriteDirContext extends InsertIntoContext {
		public Token path;
		public TablePropertyListContext options;
		public TerminalNode INSERT() { return getToken(SqlHiveParser.INSERT, 0); }
		public TerminalNode OVERWRITE() { return getToken(SqlHiveParser.OVERWRITE, 0); }
		public TerminalNode DIRECTORY() { return getToken(SqlHiveParser.DIRECTORY, 0); }
		public TableProviderContext tableProvider() {
			return getRuleContext(TableProviderContext.class,0);
		}
		public TerminalNode LOCAL() { return getToken(SqlHiveParser.LOCAL, 0); }
		public TerminalNode OPTIONS() { return getToken(SqlHiveParser.OPTIONS, 0); }
		public TerminalNode STRING() { return getToken(SqlHiveParser.STRING, 0); }
		public TablePropertyListContext tablePropertyList() {
			return getRuleContext(TablePropertyListContext.class,0);
		}
		public InsertOverwriteDirContext(InsertIntoContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterInsertOverwriteDir(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitInsertOverwriteDir(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitInsertOverwriteDir(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class InsertOverwriteTableContext extends InsertIntoContext {
		public TerminalNode INSERT() { return getToken(SqlHiveParser.INSERT, 0); }
		public TerminalNode OVERWRITE() { return getToken(SqlHiveParser.OVERWRITE, 0); }
		public TerminalNode TABLE() { return getToken(SqlHiveParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public PartitionSpecContext partitionSpec() {
			return getRuleContext(PartitionSpecContext.class,0);
		}
		public TerminalNode IF() { return getToken(SqlHiveParser.IF, 0); }
		public TerminalNode NOT() { return getToken(SqlHiveParser.NOT, 0); }
		public TerminalNode EXISTS() { return getToken(SqlHiveParser.EXISTS, 0); }
		public InsertOverwriteTableContext(InsertIntoContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterInsertOverwriteTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitInsertOverwriteTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitInsertOverwriteTable(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class InsertIntoTableContext extends InsertIntoContext {
		public TerminalNode INSERT() { return getToken(SqlHiveParser.INSERT, 0); }
		public TerminalNode INTO() { return getToken(SqlHiveParser.INTO, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode TABLE() { return getToken(SqlHiveParser.TABLE, 0); }
		public PartitionSpecContext partitionSpec() {
			return getRuleContext(PartitionSpecContext.class,0);
		}
		public InsertIntoTableContext(InsertIntoContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterInsertIntoTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitInsertIntoTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitInsertIntoTable(this);
			else return visitor.visitChildren(this);
		}
	}

	public final InsertIntoContext insertInto() throws RecognitionException {
		InsertIntoContext _localctx = new InsertIntoContext(_ctx, getState());
		enterRule(_localctx, 34, RULE_insertInto);
		int _la;
		try {
			setState(1131);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,124,_ctx) ) {
			case 1:
				_localctx = new InsertOverwriteTableContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1083);
				match(INSERT);
				setState(1084);
				match(OVERWRITE);
				setState(1085);
				match(TABLE);
				setState(1086);
				tableIdentifier();
				setState(1093);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(1087);
					partitionSpec();
					setState(1091);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==IF) {
						{
						setState(1088);
						match(IF);
						setState(1089);
						match(NOT);
						setState(1090);
						match(EXISTS);
						}
					}

					}
				}

				}
				break;
			case 2:
				_localctx = new InsertIntoTableContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1095);
				match(INSERT);
				setState(1096);
				match(INTO);
				setState(1098);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,116,_ctx) ) {
				case 1:
					{
					setState(1097);
					match(TABLE);
					}
					break;
				}
				setState(1100);
				tableIdentifier();
				setState(1102);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(1101);
					partitionSpec();
					}
				}

				}
				break;
			case 3:
				_localctx = new InsertOverwriteHiveDirContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(1104);
				match(INSERT);
				setState(1105);
				match(OVERWRITE);
				setState(1107);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LOCAL) {
					{
					setState(1106);
					match(LOCAL);
					}
				}

				setState(1109);
				match(DIRECTORY);
				setState(1110);
				((InsertOverwriteHiveDirContext)_localctx).path = match(STRING);
				setState(1112);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ROW) {
					{
					setState(1111);
					rowFormat();
					}
				}

				setState(1115);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==STORED) {
					{
					setState(1114);
					createFileFormat();
					}
				}

				}
				break;
			case 4:
				_localctx = new InsertOverwriteDirContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(1117);
				match(INSERT);
				setState(1118);
				match(OVERWRITE);
				setState(1120);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LOCAL) {
					{
					setState(1119);
					match(LOCAL);
					}
				}

				setState(1122);
				match(DIRECTORY);
				setState(1124);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==STRING) {
					{
					setState(1123);
					((InsertOverwriteDirContext)_localctx).path = match(STRING);
					}
				}

				setState(1126);
				tableProvider();
				setState(1129);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OPTIONS) {
					{
					setState(1127);
					match(OPTIONS);
					setState(1128);
					((InsertOverwriteDirContext)_localctx).options = tablePropertyList();
					}
				}

				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class PartitionSpecLocationContext extends ParserRuleContext {
		public PartitionSpecContext partitionSpec() {
			return getRuleContext(PartitionSpecContext.class,0);
		}
		public LocationSpecContext locationSpec() {
			return getRuleContext(LocationSpecContext.class,0);
		}
		public PartitionSpecLocationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_partitionSpecLocation; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterPartitionSpecLocation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitPartitionSpecLocation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitPartitionSpecLocation(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PartitionSpecLocationContext partitionSpecLocation() throws RecognitionException {
		PartitionSpecLocationContext _localctx = new PartitionSpecLocationContext(_ctx, getState());
		enterRule(_localctx, 36, RULE_partitionSpecLocation);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1133);
			partitionSpec();
			setState(1135);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==LOCATION) {
				{
				setState(1134);
				locationSpec();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class PartitionSpecContext extends ParserRuleContext {
		public TerminalNode PARTITION() { return getToken(SqlHiveParser.PARTITION, 0); }
		public List<PartitionValContext> partitionVal() {
			return getRuleContexts(PartitionValContext.class);
		}
		public PartitionValContext partitionVal(int i) {
			return getRuleContext(PartitionValContext.class,i);
		}
		public PartitionSpecContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_partitionSpec; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterPartitionSpec(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitPartitionSpec(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitPartitionSpec(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PartitionSpecContext partitionSpec() throws RecognitionException {
		PartitionSpecContext _localctx = new PartitionSpecContext(_ctx, getState());
		enterRule(_localctx, 38, RULE_partitionSpec);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1137);
			match(PARTITION);
			setState(1138);
			match(T__0);
			setState(1139);
			partitionVal();
			setState(1144);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__2) {
				{
				{
				setState(1140);
				match(T__2);
				setState(1141);
				partitionVal();
				}
				}
				setState(1146);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(1147);
			match(T__1);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class PartitionValContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode EQ() { return getToken(SqlHiveParser.EQ, 0); }
		public ConstantContext constant() {
			return getRuleContext(ConstantContext.class,0);
		}
		public PartitionValContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_partitionVal; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterPartitionVal(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitPartitionVal(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitPartitionVal(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PartitionValContext partitionVal() throws RecognitionException {
		PartitionValContext _localctx = new PartitionValContext(_ctx, getState());
		enterRule(_localctx, 40, RULE_partitionVal);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1149);
			identifier();
			setState(1152);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==EQ) {
				{
				setState(1150);
				match(EQ);
				setState(1151);
				constant();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class DescribeFuncNameContext extends ParserRuleContext {
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode STRING() { return getToken(SqlHiveParser.STRING, 0); }
		public ComparisonOperatorContext comparisonOperator() {
			return getRuleContext(ComparisonOperatorContext.class,0);
		}
		public ArithmeticOperatorContext arithmeticOperator() {
			return getRuleContext(ArithmeticOperatorContext.class,0);
		}
		public PredicateOperatorContext predicateOperator() {
			return getRuleContext(PredicateOperatorContext.class,0);
		}
		public DescribeFuncNameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_describeFuncName; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterDescribeFuncName(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitDescribeFuncName(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitDescribeFuncName(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DescribeFuncNameContext describeFuncName() throws RecognitionException {
		DescribeFuncNameContext _localctx = new DescribeFuncNameContext(_ctx, getState());
		enterRule(_localctx, 42, RULE_describeFuncName);
		try {
			setState(1159);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,128,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1154);
				qualifiedName();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1155);
				match(STRING);
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(1156);
				comparisonOperator();
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(1157);
				arithmeticOperator();
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(1158);
				predicateOperator();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class DescribeColNameContext extends ParserRuleContext {
		public IdentifierContext identifier;
		public List<IdentifierContext> nameParts = new ArrayList<IdentifierContext>();
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public DescribeColNameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_describeColName; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterDescribeColName(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitDescribeColName(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitDescribeColName(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DescribeColNameContext describeColName() throws RecognitionException {
		DescribeColNameContext _localctx = new DescribeColNameContext(_ctx, getState());
		enterRule(_localctx, 44, RULE_describeColName);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1161);
			((DescribeColNameContext)_localctx).identifier = identifier();
			((DescribeColNameContext)_localctx).nameParts.add(((DescribeColNameContext)_localctx).identifier);
			setState(1166);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__3) {
				{
				{
				setState(1162);
				match(T__3);
				setState(1163);
				((DescribeColNameContext)_localctx).identifier = identifier();
				((DescribeColNameContext)_localctx).nameParts.add(((DescribeColNameContext)_localctx).identifier);
				}
				}
				setState(1168);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class CtesContext extends ParserRuleContext {
		public TerminalNode WITH() { return getToken(SqlHiveParser.WITH, 0); }
		public List<NamedQueryContext> namedQuery() {
			return getRuleContexts(NamedQueryContext.class);
		}
		public NamedQueryContext namedQuery(int i) {
			return getRuleContext(NamedQueryContext.class,i);
		}
		public CtesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_ctes; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterCtes(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitCtes(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitCtes(this);
			else return visitor.visitChildren(this);
		}
	}

	public final CtesContext ctes() throws RecognitionException {
		CtesContext _localctx = new CtesContext(_ctx, getState());
		enterRule(_localctx, 46, RULE_ctes);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1169);
			match(WITH);
			setState(1170);
			namedQuery();
			setState(1175);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__2) {
				{
				{
				setState(1171);
				match(T__2);
				setState(1172);
				namedQuery();
				}
				}
				setState(1177);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class NamedQueryContext extends ParserRuleContext {
		public IdentifierContext name;
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlHiveParser.AS, 0); }
		public NamedQueryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_namedQuery; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterNamedQuery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitNamedQuery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitNamedQuery(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NamedQueryContext namedQuery() throws RecognitionException {
		NamedQueryContext _localctx = new NamedQueryContext(_ctx, getState());
		enterRule(_localctx, 48, RULE_namedQuery);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1178);
			((NamedQueryContext)_localctx).name = identifier();
			setState(1180);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==AS) {
				{
				setState(1179);
				match(AS);
				}
			}

			setState(1182);
			match(T__0);
			setState(1183);
			query();
			setState(1184);
			match(T__1);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class TableProviderContext extends ParserRuleContext {
		public TerminalNode USING() { return getToken(SqlHiveParser.USING, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TableProviderContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_tableProvider; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterTableProvider(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitTableProvider(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitTableProvider(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TableProviderContext tableProvider() throws RecognitionException {
		TableProviderContext _localctx = new TableProviderContext(_ctx, getState());
		enterRule(_localctx, 50, RULE_tableProvider);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1186);
			match(USING);
			setState(1187);
			qualifiedName();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class TablePropertyListContext extends ParserRuleContext {
		public List<TablePropertyContext> tableProperty() {
			return getRuleContexts(TablePropertyContext.class);
		}
		public TablePropertyContext tableProperty(int i) {
			return getRuleContext(TablePropertyContext.class,i);
		}
		public TablePropertyListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_tablePropertyList; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterTablePropertyList(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitTablePropertyList(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitTablePropertyList(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TablePropertyListContext tablePropertyList() throws RecognitionException {
		TablePropertyListContext _localctx = new TablePropertyListContext(_ctx, getState());
		enterRule(_localctx, 52, RULE_tablePropertyList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1189);
			match(T__0);
			setState(1190);
			tableProperty();
			setState(1195);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__2) {
				{
				{
				setState(1191);
				match(T__2);
				setState(1192);
				tableProperty();
				}
				}
				setState(1197);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(1198);
			match(T__1);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class TablePropertyContext extends ParserRuleContext {
		public TablePropertyKeyContext key;
		public TablePropertyValueContext value;
		public TablePropertyKeyContext tablePropertyKey() {
			return getRuleContext(TablePropertyKeyContext.class,0);
		}
		public TablePropertyValueContext tablePropertyValue() {
			return getRuleContext(TablePropertyValueContext.class,0);
		}
		public TerminalNode EQ() { return getToken(SqlHiveParser.EQ, 0); }
		public TablePropertyContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_tableProperty; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterTableProperty(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitTableProperty(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitTableProperty(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TablePropertyContext tableProperty() throws RecognitionException {
		TablePropertyContext _localctx = new TablePropertyContext(_ctx, getState());
		enterRule(_localctx, 54, RULE_tableProperty);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1200);
			((TablePropertyContext)_localctx).key = tablePropertyKey();
			setState(1205);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==TRUE || _la==FALSE || _la==EQ || ((((_la - 241)) & ~0x3f) == 0 && ((1L << (_la - 241)) & ((1L << (STRING - 241)) | (1L << (INTEGER_VALUE - 241)) | (1L << (DECIMAL_VALUE - 241)))) != 0)) {
				{
				setState(1202);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==EQ) {
					{
					setState(1201);
					match(EQ);
					}
				}

				setState(1204);
				((TablePropertyContext)_localctx).value = tablePropertyValue();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class TablePropertyKeyContext extends ParserRuleContext {
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public TerminalNode STRING() { return getToken(SqlHiveParser.STRING, 0); }
		public TablePropertyKeyContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_tablePropertyKey; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterTablePropertyKey(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitTablePropertyKey(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitTablePropertyKey(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TablePropertyKeyContext tablePropertyKey() throws RecognitionException {
		TablePropertyKeyContext _localctx = new TablePropertyKeyContext(_ctx, getState());
		enterRule(_localctx, 56, RULE_tablePropertyKey);
		int _la;
		try {
			setState(1216);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case SELECT:
			case FROM:
			case ADD:
			case AS:
			case ALL:
			case ANY:
			case DISTINCT:
			case WHERE:
			case GROUP:
			case BY:
			case GROUPING:
			case SETS:
			case CUBE:
			case ROLLUP:
			case ORDER:
			case HAVING:
			case LIMIT:
			case AT:
			case OR:
			case AND:
			case IN:
			case NOT:
			case NO:
			case EXISTS:
			case BETWEEN:
			case LIKE:
			case RLIKE:
			case IS:
			case NULL:
			case TRUE:
			case FALSE:
			case NULLS:
			case ASC:
			case DESC:
			case FOR:
			case INTERVAL:
			case CASE:
			case WHEN:
			case THEN:
			case ELSE:
			case END:
			case JOIN:
			case CROSS:
			case OUTER:
			case INNER:
			case LEFT:
			case SEMI:
			case RIGHT:
			case FULL:
			case NATURAL:
			case ON:
			case PIVOT:
			case LATERAL:
			case WINDOW:
			case OVER:
			case PARTITION:
			case RANGE:
			case ROWS:
			case UNBOUNDED:
			case PRECEDING:
			case FOLLOWING:
			case CURRENT:
			case FIRST:
			case AFTER:
			case LAST:
			case ROW:
			case WITH:
			case VALUES:
			case CREATE:
			case TABLE:
			case DIRECTORY:
			case VIEW:
			case REPLACE:
			case INSERT:
			case UPDATE:
			case DELETE:
			case INTO:
			case DESCRIBE:
			case EXPLAIN:
			case FORMAT:
			case LOGICAL:
			case CODEGEN:
			case COST:
			case CAST:
			case SHOW:
			case TABLES:
			case COLUMNS:
			case COLUMN:
			case USE:
			case PARTITIONS:
			case FUNCTIONS:
			case DROP:
			case UNION:
			case EXCEPT:
			case SETMINUS:
			case INTERSECT:
			case TO:
			case TABLESAMPLE:
			case STRATIFY:
			case ALTER:
			case RENAME:
			case ARRAY:
			case MAP:
			case STRUCT:
			case COMMENT:
			case SET:
			case RESET:
			case DATA:
			case START:
			case TRANSACTION:
			case COMMIT:
			case ROLLBACK:
			case MACRO:
			case IGNORE:
			case BOTH:
			case LEADING:
			case TRAILING:
			case IF:
			case POSITION:
			case EXTRACT:
			case DIV:
			case PERCENTLIT:
			case BUCKET:
			case OUT:
			case OF:
			case SORT:
			case CLUSTER:
			case DISTRIBUTE:
			case OVERWRITE:
			case TRANSFORM:
			case REDUCE:
			case SERDE:
			case SERDEPROPERTIES:
			case RECORDREADER:
			case RECORDWRITER:
			case DELIMITED:
			case FIELDS:
			case TERMINATED:
			case COLLECTION:
			case ITEMS:
			case KEYS:
			case ESCAPED:
			case LINES:
			case SEPARATED:
			case FUNCTION:
			case EXTENDED:
			case REFRESH:
			case CLEAR:
			case CACHE:
			case UNCACHE:
			case LAZY:
			case FORMATTED:
			case GLOBAL:
			case TEMPORARY:
			case OPTIONS:
			case UNSET:
			case TBLPROPERTIES:
			case DBPROPERTIES:
			case BUCKETS:
			case SKEWED:
			case STORED:
			case DIRECTORIES:
			case LOCATION:
			case EXCHANGE:
			case ARCHIVE:
			case UNARCHIVE:
			case FILEFORMAT:
			case TOUCH:
			case COMPACT:
			case CONCATENATE:
			case CHANGE:
			case CASCADE:
			case RESTRICT:
			case CLUSTERED:
			case SORTED:
			case PURGE:
			case INPUTFORMAT:
			case OUTPUTFORMAT:
			case DATABASE:
			case DATABASES:
			case DFS:
			case TRUNCATE:
			case ANALYZE:
			case COMPUTE:
			case LIST:
			case STATISTICS:
			case PARTITIONED:
			case EXTERNAL:
			case DEFINED:
			case REVOKE:
			case GRANT:
			case LOCK:
			case UNLOCK:
			case MSCK:
			case REPAIR:
			case RECOVER:
			case EXPORT:
			case IMPORT:
			case LOAD:
			case ROLE:
			case ROLES:
			case COMPACTIONS:
			case PRINCIPALS:
			case TRANSACTIONS:
			case INDEX:
			case INDEXES:
			case LOCKS:
			case OPTION:
			case ANTI:
			case LOCAL:
			case INPATH:
			case IDENTIFIER:
			case BACKQUOTED_IDENTIFIER:
				enterOuterAlt(_localctx, 1);
				{
				setState(1207);
				identifier();
				setState(1212);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(1208);
					match(T__3);
					setState(1209);
					identifier();
					}
					}
					setState(1214);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
				break;
			case STRING:
				enterOuterAlt(_localctx, 2);
				{
				setState(1215);
				match(STRING);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class TablePropertyValueContext extends ParserRuleContext {
		public TerminalNode INTEGER_VALUE() { return getToken(SqlHiveParser.INTEGER_VALUE, 0); }
		public TerminalNode DECIMAL_VALUE() { return getToken(SqlHiveParser.DECIMAL_VALUE, 0); }
		public BooleanValueContext booleanValue() {
			return getRuleContext(BooleanValueContext.class,0);
		}
		public TerminalNode STRING() { return getToken(SqlHiveParser.STRING, 0); }
		public TablePropertyValueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_tablePropertyValue; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterTablePropertyValue(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitTablePropertyValue(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitTablePropertyValue(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TablePropertyValueContext tablePropertyValue() throws RecognitionException {
		TablePropertyValueContext _localctx = new TablePropertyValueContext(_ctx, getState());
		enterRule(_localctx, 58, RULE_tablePropertyValue);
		try {
			setState(1222);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case INTEGER_VALUE:
				enterOuterAlt(_localctx, 1);
				{
				setState(1218);
				match(INTEGER_VALUE);
				}
				break;
			case DECIMAL_VALUE:
				enterOuterAlt(_localctx, 2);
				{
				setState(1219);
				match(DECIMAL_VALUE);
				}
				break;
			case TRUE:
			case FALSE:
				enterOuterAlt(_localctx, 3);
				{
				setState(1220);
				booleanValue();
				}
				break;
			case STRING:
				enterOuterAlt(_localctx, 4);
				{
				setState(1221);
				match(STRING);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ConstantListContext extends ParserRuleContext {
		public List<ConstantContext> constant() {
			return getRuleContexts(ConstantContext.class);
		}
		public ConstantContext constant(int i) {
			return getRuleContext(ConstantContext.class,i);
		}
		public ConstantListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_constantList; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterConstantList(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitConstantList(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitConstantList(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ConstantListContext constantList() throws RecognitionException {
		ConstantListContext _localctx = new ConstantListContext(_ctx, getState());
		enterRule(_localctx, 60, RULE_constantList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1224);
			match(T__0);
			setState(1225);
			constant();
			setState(1230);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__2) {
				{
				{
				setState(1226);
				match(T__2);
				setState(1227);
				constant();
				}
				}
				setState(1232);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(1233);
			match(T__1);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class NestedConstantListContext extends ParserRuleContext {
		public List<ConstantListContext> constantList() {
			return getRuleContexts(ConstantListContext.class);
		}
		public ConstantListContext constantList(int i) {
			return getRuleContext(ConstantListContext.class,i);
		}
		public NestedConstantListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_nestedConstantList; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterNestedConstantList(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitNestedConstantList(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitNestedConstantList(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NestedConstantListContext nestedConstantList() throws RecognitionException {
		NestedConstantListContext _localctx = new NestedConstantListContext(_ctx, getState());
		enterRule(_localctx, 62, RULE_nestedConstantList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1235);
			match(T__0);
			setState(1236);
			constantList();
			setState(1241);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__2) {
				{
				{
				setState(1237);
				match(T__2);
				setState(1238);
				constantList();
				}
				}
				setState(1243);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(1244);
			match(T__1);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class CreateFileFormatContext extends ParserRuleContext {
		public TerminalNode STORED() { return getToken(SqlHiveParser.STORED, 0); }
		public TerminalNode AS() { return getToken(SqlHiveParser.AS, 0); }
		public FileFormatContext fileFormat() {
			return getRuleContext(FileFormatContext.class,0);
		}
		public TerminalNode BY() { return getToken(SqlHiveParser.BY, 0); }
		public StorageHandlerContext storageHandler() {
			return getRuleContext(StorageHandlerContext.class,0);
		}
		public CreateFileFormatContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_createFileFormat; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterCreateFileFormat(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitCreateFileFormat(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitCreateFileFormat(this);
			else return visitor.visitChildren(this);
		}
	}

	public final CreateFileFormatContext createFileFormat() throws RecognitionException {
		CreateFileFormatContext _localctx = new CreateFileFormatContext(_ctx, getState());
		enterRule(_localctx, 64, RULE_createFileFormat);
		try {
			setState(1252);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,140,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1246);
				match(STORED);
				setState(1247);
				match(AS);
				setState(1248);
				fileFormat();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1249);
				match(STORED);
				setState(1250);
				match(BY);
				setState(1251);
				storageHandler();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class FileFormatContext extends ParserRuleContext {
		public FileFormatContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_fileFormat; }
	 
		public FileFormatContext() { }
		public void copyFrom(FileFormatContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class TableFileFormatContext extends FileFormatContext {
		public Token inFmt;
		public Token outFmt;
		public TerminalNode INPUTFORMAT() { return getToken(SqlHiveParser.INPUTFORMAT, 0); }
		public TerminalNode OUTPUTFORMAT() { return getToken(SqlHiveParser.OUTPUTFORMAT, 0); }
		public List<TerminalNode> STRING() { return getTokens(SqlHiveParser.STRING); }
		public TerminalNode STRING(int i) {
			return getToken(SqlHiveParser.STRING, i);
		}
		public TableFileFormatContext(FileFormatContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterTableFileFormat(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitTableFileFormat(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitTableFileFormat(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class GenericFileFormatContext extends FileFormatContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public GenericFileFormatContext(FileFormatContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterGenericFileFormat(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitGenericFileFormat(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitGenericFileFormat(this);
			else return visitor.visitChildren(this);
		}
	}

	public final FileFormatContext fileFormat() throws RecognitionException {
		FileFormatContext _localctx = new FileFormatContext(_ctx, getState());
		enterRule(_localctx, 66, RULE_fileFormat);
		try {
			setState(1259);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,141,_ctx) ) {
			case 1:
				_localctx = new TableFileFormatContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1254);
				match(INPUTFORMAT);
				setState(1255);
				((TableFileFormatContext)_localctx).inFmt = match(STRING);
				setState(1256);
				match(OUTPUTFORMAT);
				setState(1257);
				((TableFileFormatContext)_localctx).outFmt = match(STRING);
				}
				break;
			case 2:
				_localctx = new GenericFileFormatContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1258);
				identifier();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class StorageHandlerContext extends ParserRuleContext {
		public TerminalNode STRING() { return getToken(SqlHiveParser.STRING, 0); }
		public TerminalNode WITH() { return getToken(SqlHiveParser.WITH, 0); }
		public TerminalNode SERDEPROPERTIES() { return getToken(SqlHiveParser.SERDEPROPERTIES, 0); }
		public TablePropertyListContext tablePropertyList() {
			return getRuleContext(TablePropertyListContext.class,0);
		}
		public StorageHandlerContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_storageHandler; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterStorageHandler(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitStorageHandler(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitStorageHandler(this);
			else return visitor.visitChildren(this);
		}
	}

	public final StorageHandlerContext storageHandler() throws RecognitionException {
		StorageHandlerContext _localctx = new StorageHandlerContext(_ctx, getState());
		enterRule(_localctx, 68, RULE_storageHandler);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1261);
			match(STRING);
			setState(1265);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,142,_ctx) ) {
			case 1:
				{
				setState(1262);
				match(WITH);
				setState(1263);
				match(SERDEPROPERTIES);
				setState(1264);
				tablePropertyList();
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ResourceContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode STRING() { return getToken(SqlHiveParser.STRING, 0); }
		public ResourceContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_resource; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterResource(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitResource(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitResource(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ResourceContext resource() throws RecognitionException {
		ResourceContext _localctx = new ResourceContext(_ctx, getState());
		enterRule(_localctx, 70, RULE_resource);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1267);
			identifier();
			setState(1268);
			match(STRING);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class QueryNoWithContext extends ParserRuleContext {
		public QueryNoWithContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_queryNoWith; }
	 
		public QueryNoWithContext() { }
		public void copyFrom(QueryNoWithContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class SingleInsertQueryContext extends QueryNoWithContext {
		public QueryTermContext queryTerm() {
			return getRuleContext(QueryTermContext.class,0);
		}
		public QueryOrganizationContext queryOrganization() {
			return getRuleContext(QueryOrganizationContext.class,0);
		}
		public InsertIntoContext insertInto() {
			return getRuleContext(InsertIntoContext.class,0);
		}
		public SingleInsertQueryContext(QueryNoWithContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterSingleInsertQuery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitSingleInsertQuery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitSingleInsertQuery(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class MultiInsertQueryContext extends QueryNoWithContext {
		public FromClauseContext fromClause() {
			return getRuleContext(FromClauseContext.class,0);
		}
		public List<MultiInsertQueryBodyContext> multiInsertQueryBody() {
			return getRuleContexts(MultiInsertQueryBodyContext.class);
		}
		public MultiInsertQueryBodyContext multiInsertQueryBody(int i) {
			return getRuleContext(MultiInsertQueryBodyContext.class,i);
		}
		public MultiInsertQueryContext(QueryNoWithContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterMultiInsertQuery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitMultiInsertQuery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitMultiInsertQuery(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QueryNoWithContext queryNoWith() throws RecognitionException {
		QueryNoWithContext _localctx = new QueryNoWithContext(_ctx, getState());
		enterRule(_localctx, 72, RULE_queryNoWith);
		int _la;
		try {
			setState(1282);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,145,_ctx) ) {
			case 1:
				_localctx = new SingleInsertQueryContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1271);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==INSERT) {
					{
					setState(1270);
					insertInto();
					}
				}

				setState(1273);
				queryTerm(0);
				setState(1274);
				queryOrganization();
				}
				break;
			case 2:
				_localctx = new MultiInsertQueryContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1276);
				fromClause();
				setState(1278); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(1277);
					multiInsertQueryBody();
					}
					}
					setState(1280); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==SELECT || _la==FROM || _la==INSERT || _la==MAP || _la==REDUCE );
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class QueryOrganizationContext extends ParserRuleContext {
		public SortItemContext sortItem;
		public List<SortItemContext> order = new ArrayList<SortItemContext>();
		public ExpressionContext expression;
		public List<ExpressionContext> clusterBy = new ArrayList<ExpressionContext>();
		public List<ExpressionContext> distributeBy = new ArrayList<ExpressionContext>();
		public List<SortItemContext> sort = new ArrayList<SortItemContext>();
		public ExpressionContext limit;
		public TerminalNode ORDER() { return getToken(SqlHiveParser.ORDER, 0); }
		public List<TerminalNode> BY() { return getTokens(SqlHiveParser.BY); }
		public TerminalNode BY(int i) {
			return getToken(SqlHiveParser.BY, i);
		}
		public TerminalNode CLUSTER() { return getToken(SqlHiveParser.CLUSTER, 0); }
		public TerminalNode DISTRIBUTE() { return getToken(SqlHiveParser.DISTRIBUTE, 0); }
		public TerminalNode SORT() { return getToken(SqlHiveParser.SORT, 0); }
		public WindowsContext windows() {
			return getRuleContext(WindowsContext.class,0);
		}
		public TerminalNode LIMIT() { return getToken(SqlHiveParser.LIMIT, 0); }
		public List<SortItemContext> sortItem() {
			return getRuleContexts(SortItemContext.class);
		}
		public SortItemContext sortItem(int i) {
			return getRuleContext(SortItemContext.class,i);
		}
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TerminalNode ALL() { return getToken(SqlHiveParser.ALL, 0); }
		public QueryOrganizationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_queryOrganization; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterQueryOrganization(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitQueryOrganization(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitQueryOrganization(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QueryOrganizationContext queryOrganization() throws RecognitionException {
		QueryOrganizationContext _localctx = new QueryOrganizationContext(_ctx, getState());
		enterRule(_localctx, 74, RULE_queryOrganization);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1294);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ORDER) {
				{
				setState(1284);
				match(ORDER);
				setState(1285);
				match(BY);
				setState(1286);
				((QueryOrganizationContext)_localctx).sortItem = sortItem();
				((QueryOrganizationContext)_localctx).order.add(((QueryOrganizationContext)_localctx).sortItem);
				setState(1291);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__2) {
					{
					{
					setState(1287);
					match(T__2);
					setState(1288);
					((QueryOrganizationContext)_localctx).sortItem = sortItem();
					((QueryOrganizationContext)_localctx).order.add(((QueryOrganizationContext)_localctx).sortItem);
					}
					}
					setState(1293);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
			}

			setState(1306);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==CLUSTER) {
				{
				setState(1296);
				match(CLUSTER);
				setState(1297);
				match(BY);
				setState(1298);
				((QueryOrganizationContext)_localctx).expression = expression();
				((QueryOrganizationContext)_localctx).clusterBy.add(((QueryOrganizationContext)_localctx).expression);
				setState(1303);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__2) {
					{
					{
					setState(1299);
					match(T__2);
					setState(1300);
					((QueryOrganizationContext)_localctx).expression = expression();
					((QueryOrganizationContext)_localctx).clusterBy.add(((QueryOrganizationContext)_localctx).expression);
					}
					}
					setState(1305);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
			}

			setState(1318);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==DISTRIBUTE) {
				{
				setState(1308);
				match(DISTRIBUTE);
				setState(1309);
				match(BY);
				setState(1310);
				((QueryOrganizationContext)_localctx).expression = expression();
				((QueryOrganizationContext)_localctx).distributeBy.add(((QueryOrganizationContext)_localctx).expression);
				setState(1315);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__2) {
					{
					{
					setState(1311);
					match(T__2);
					setState(1312);
					((QueryOrganizationContext)_localctx).expression = expression();
					((QueryOrganizationContext)_localctx).distributeBy.add(((QueryOrganizationContext)_localctx).expression);
					}
					}
					setState(1317);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
			}

			setState(1330);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==SORT) {
				{
				setState(1320);
				match(SORT);
				setState(1321);
				match(BY);
				setState(1322);
				((QueryOrganizationContext)_localctx).sortItem = sortItem();
				((QueryOrganizationContext)_localctx).sort.add(((QueryOrganizationContext)_localctx).sortItem);
				setState(1327);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__2) {
					{
					{
					setState(1323);
					match(T__2);
					setState(1324);
					((QueryOrganizationContext)_localctx).sortItem = sortItem();
					((QueryOrganizationContext)_localctx).sort.add(((QueryOrganizationContext)_localctx).sortItem);
					}
					}
					setState(1329);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
			}

			setState(1333);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WINDOW) {
				{
				setState(1332);
				windows();
				}
			}

			setState(1340);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==LIMIT) {
				{
				setState(1335);
				match(LIMIT);
				setState(1338);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,155,_ctx) ) {
				case 1:
					{
					setState(1336);
					match(ALL);
					}
					break;
				case 2:
					{
					setState(1337);
					((QueryOrganizationContext)_localctx).limit = expression();
					}
					break;
				}
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class MultiInsertQueryBodyContext extends ParserRuleContext {
		public QuerySpecificationContext querySpecification() {
			return getRuleContext(QuerySpecificationContext.class,0);
		}
		public QueryOrganizationContext queryOrganization() {
			return getRuleContext(QueryOrganizationContext.class,0);
		}
		public InsertIntoContext insertInto() {
			return getRuleContext(InsertIntoContext.class,0);
		}
		public MultiInsertQueryBodyContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_multiInsertQueryBody; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterMultiInsertQueryBody(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitMultiInsertQueryBody(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitMultiInsertQueryBody(this);
			else return visitor.visitChildren(this);
		}
	}

	public final MultiInsertQueryBodyContext multiInsertQueryBody() throws RecognitionException {
		MultiInsertQueryBodyContext _localctx = new MultiInsertQueryBodyContext(_ctx, getState());
		enterRule(_localctx, 76, RULE_multiInsertQueryBody);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1343);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==INSERT) {
				{
				setState(1342);
				insertInto();
				}
			}

			setState(1345);
			querySpecification();
			setState(1346);
			queryOrganization();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class QueryTermContext extends ParserRuleContext {
		public QueryTermContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_queryTerm; }
	 
		public QueryTermContext() { }
		public void copyFrom(QueryTermContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class QueryTermDefaultContext extends QueryTermContext {
		public QueryPrimaryContext queryPrimary() {
			return getRuleContext(QueryPrimaryContext.class,0);
		}
		public QueryTermDefaultContext(QueryTermContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterQueryTermDefault(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitQueryTermDefault(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitQueryTermDefault(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class SetOperationContext extends QueryTermContext {
		public QueryTermContext left;
		public Token operator;
		public QueryTermContext right;
		public List<QueryTermContext> queryTerm() {
			return getRuleContexts(QueryTermContext.class);
		}
		public QueryTermContext queryTerm(int i) {
			return getRuleContext(QueryTermContext.class,i);
		}
		public TerminalNode INTERSECT() { return getToken(SqlHiveParser.INTERSECT, 0); }
		public TerminalNode UNION() { return getToken(SqlHiveParser.UNION, 0); }
		public TerminalNode EXCEPT() { return getToken(SqlHiveParser.EXCEPT, 0); }
		public TerminalNode SETMINUS() { return getToken(SqlHiveParser.SETMINUS, 0); }
		public SetQuantifierContext setQuantifier() {
			return getRuleContext(SetQuantifierContext.class,0);
		}
		public SetOperationContext(QueryTermContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterSetOperation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitSetOperation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitSetOperation(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QueryTermContext queryTerm() throws RecognitionException {
		return queryTerm(0);
	}

	private QueryTermContext queryTerm(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		QueryTermContext _localctx = new QueryTermContext(_ctx, _parentState);
		QueryTermContext _prevctx = _localctx;
		int _startState = 78;
		enterRecursionRule(_localctx, 78, RULE_queryTerm, _p);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			{
			_localctx = new QueryTermDefaultContext(_localctx);
			_ctx = _localctx;
			_prevctx = _localctx;

			setState(1349);
			queryPrimary();
			}
			_ctx.stop = _input.LT(-1);
			setState(1374);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,162,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(1372);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,161,_ctx) ) {
					case 1:
						{
						_localctx = new SetOperationContext(new QueryTermContext(_parentctx, _parentState));
						((SetOperationContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_queryTerm);
						setState(1351);
						if (!(precpred(_ctx, 3))) throw new FailedPredicateException(this, "precpred(_ctx, 3)");
						setState(1352);
						if (!(legacy_setops_precedence_enbled)) throw new FailedPredicateException(this, "legacy_setops_precedence_enbled");
						setState(1353);
						((SetOperationContext)_localctx).operator = _input.LT(1);
						_la = _input.LA(1);
						if ( !(((((_la - 103)) & ~0x3f) == 0 && ((1L << (_la - 103)) & ((1L << (UNION - 103)) | (1L << (EXCEPT - 103)) | (1L << (SETMINUS - 103)) | (1L << (INTERSECT - 103)))) != 0)) ) {
							((SetOperationContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						setState(1355);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==ALL || _la==DISTINCT) {
							{
							setState(1354);
							setQuantifier();
							}
						}

						setState(1357);
						((SetOperationContext)_localctx).right = queryTerm(4);
						}
						break;
					case 2:
						{
						_localctx = new SetOperationContext(new QueryTermContext(_parentctx, _parentState));
						((SetOperationContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_queryTerm);
						setState(1358);
						if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
						setState(1359);
						if (!(!legacy_setops_precedence_enbled)) throw new FailedPredicateException(this, "!legacy_setops_precedence_enbled");
						setState(1360);
						((SetOperationContext)_localctx).operator = match(INTERSECT);
						setState(1362);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==ALL || _la==DISTINCT) {
							{
							setState(1361);
							setQuantifier();
							}
						}

						setState(1364);
						((SetOperationContext)_localctx).right = queryTerm(3);
						}
						break;
					case 3:
						{
						_localctx = new SetOperationContext(new QueryTermContext(_parentctx, _parentState));
						((SetOperationContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_queryTerm);
						setState(1365);
						if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
						setState(1366);
						if (!(!legacy_setops_precedence_enbled)) throw new FailedPredicateException(this, "!legacy_setops_precedence_enbled");
						setState(1367);
						((SetOperationContext)_localctx).operator = _input.LT(1);
						_la = _input.LA(1);
						if ( !(((((_la - 103)) & ~0x3f) == 0 && ((1L << (_la - 103)) & ((1L << (UNION - 103)) | (1L << (EXCEPT - 103)) | (1L << (SETMINUS - 103)))) != 0)) ) {
							((SetOperationContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						setState(1369);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==ALL || _la==DISTINCT) {
							{
							setState(1368);
							setQuantifier();
							}
						}

						setState(1371);
						((SetOperationContext)_localctx).right = queryTerm(2);
						}
						break;
					}
					} 
				}
				setState(1376);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,162,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	public static class QueryPrimaryContext extends ParserRuleContext {
		public QueryPrimaryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_queryPrimary; }
	 
		public QueryPrimaryContext() { }
		public void copyFrom(QueryPrimaryContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class SubqueryContext extends QueryPrimaryContext {
		public QueryNoWithContext queryNoWith() {
			return getRuleContext(QueryNoWithContext.class,0);
		}
		public SubqueryContext(QueryPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterSubquery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitSubquery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitSubquery(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class QueryPrimaryDefaultContext extends QueryPrimaryContext {
		public QuerySpecificationContext querySpecification() {
			return getRuleContext(QuerySpecificationContext.class,0);
		}
		public QueryPrimaryDefaultContext(QueryPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterQueryPrimaryDefault(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitQueryPrimaryDefault(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitQueryPrimaryDefault(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class InlineTableDefault1Context extends QueryPrimaryContext {
		public InlineTableContext inlineTable() {
			return getRuleContext(InlineTableContext.class,0);
		}
		public InlineTableDefault1Context(QueryPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterInlineTableDefault1(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitInlineTableDefault1(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitInlineTableDefault1(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class TableContext extends QueryPrimaryContext {
		public TerminalNode TABLE() { return getToken(SqlHiveParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TableContext(QueryPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitTable(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QueryPrimaryContext queryPrimary() throws RecognitionException {
		QueryPrimaryContext _localctx = new QueryPrimaryContext(_ctx, getState());
		enterRule(_localctx, 80, RULE_queryPrimary);
		try {
			setState(1385);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case SELECT:
			case FROM:
			case MAP:
			case REDUCE:
				_localctx = new QueryPrimaryDefaultContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1377);
				querySpecification();
				}
				break;
			case TABLE:
				_localctx = new TableContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1378);
				match(TABLE);
				setState(1379);
				tableIdentifier();
				}
				break;
			case VALUES:
				_localctx = new InlineTableDefault1Context(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(1380);
				inlineTable();
				}
				break;
			case T__0:
				_localctx = new SubqueryContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(1381);
				match(T__0);
				setState(1382);
				queryNoWith();
				setState(1383);
				match(T__1);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SortItemContext extends ParserRuleContext {
		public Token ordering;
		public Token nullOrder;
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode NULLS() { return getToken(SqlHiveParser.NULLS, 0); }
		public TerminalNode ASC() { return getToken(SqlHiveParser.ASC, 0); }
		public TerminalNode DESC() { return getToken(SqlHiveParser.DESC, 0); }
		public TerminalNode LAST() { return getToken(SqlHiveParser.LAST, 0); }
		public TerminalNode FIRST() { return getToken(SqlHiveParser.FIRST, 0); }
		public SortItemContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sortItem; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterSortItem(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitSortItem(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitSortItem(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SortItemContext sortItem() throws RecognitionException {
		SortItemContext _localctx = new SortItemContext(_ctx, getState());
		enterRule(_localctx, 82, RULE_sortItem);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1387);
			expression();
			setState(1389);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ASC || _la==DESC) {
				{
				setState(1388);
				((SortItemContext)_localctx).ordering = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==ASC || _la==DESC) ) {
					((SortItemContext)_localctx).ordering = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
			}

			setState(1393);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==NULLS) {
				{
				setState(1391);
				match(NULLS);
				setState(1392);
				((SortItemContext)_localctx).nullOrder = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==FIRST || _la==LAST) ) {
					((SortItemContext)_localctx).nullOrder = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class QuerySpecificationContext extends ParserRuleContext {
		public Token kind;
		public RowFormatContext inRowFormat;
		public Token recordWriter;
		public Token script;
		public RowFormatContext outRowFormat;
		public Token recordReader;
		public BooleanExpressionContext where;
		public HintContext hint;
		public List<HintContext> hints = new ArrayList<HintContext>();
		public BooleanExpressionContext having;
		public TerminalNode USING() { return getToken(SqlHiveParser.USING, 0); }
		public List<TerminalNode> STRING() { return getTokens(SqlHiveParser.STRING); }
		public TerminalNode STRING(int i) {
			return getToken(SqlHiveParser.STRING, i);
		}
		public TerminalNode RECORDWRITER() { return getToken(SqlHiveParser.RECORDWRITER, 0); }
		public TerminalNode AS() { return getToken(SqlHiveParser.AS, 0); }
		public TerminalNode RECORDREADER() { return getToken(SqlHiveParser.RECORDREADER, 0); }
		public FromClauseContext fromClause() {
			return getRuleContext(FromClauseContext.class,0);
		}
		public TerminalNode WHERE() { return getToken(SqlHiveParser.WHERE, 0); }
		public TerminalNode SELECT() { return getToken(SqlHiveParser.SELECT, 0); }
		public NamedExpressionSeqContext namedExpressionSeq() {
			return getRuleContext(NamedExpressionSeqContext.class,0);
		}
		public List<RowFormatContext> rowFormat() {
			return getRuleContexts(RowFormatContext.class);
		}
		public RowFormatContext rowFormat(int i) {
			return getRuleContext(RowFormatContext.class,i);
		}
		public List<BooleanExpressionContext> booleanExpression() {
			return getRuleContexts(BooleanExpressionContext.class);
		}
		public BooleanExpressionContext booleanExpression(int i) {
			return getRuleContext(BooleanExpressionContext.class,i);
		}
		public TerminalNode TRANSFORM() { return getToken(SqlHiveParser.TRANSFORM, 0); }
		public TerminalNode MAP() { return getToken(SqlHiveParser.MAP, 0); }
		public TerminalNode REDUCE() { return getToken(SqlHiveParser.REDUCE, 0); }
		public IdentifierSeqContext identifierSeq() {
			return getRuleContext(IdentifierSeqContext.class,0);
		}
		public ColTypeListContext colTypeList() {
			return getRuleContext(ColTypeListContext.class,0);
		}
		public List<LateralViewContext> lateralView() {
			return getRuleContexts(LateralViewContext.class);
		}
		public LateralViewContext lateralView(int i) {
			return getRuleContext(LateralViewContext.class,i);
		}
		public AggregationContext aggregation() {
			return getRuleContext(AggregationContext.class,0);
		}
		public TerminalNode HAVING() { return getToken(SqlHiveParser.HAVING, 0); }
		public WindowsContext windows() {
			return getRuleContext(WindowsContext.class,0);
		}
		public SetQuantifierContext setQuantifier() {
			return getRuleContext(SetQuantifierContext.class,0);
		}
		public List<HintContext> hint() {
			return getRuleContexts(HintContext.class);
		}
		public HintContext hint(int i) {
			return getRuleContext(HintContext.class,i);
		}
		public QuerySpecificationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_querySpecification; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterQuerySpecification(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitQuerySpecification(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitQuerySpecification(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QuerySpecificationContext querySpecification() throws RecognitionException {
		QuerySpecificationContext _localctx = new QuerySpecificationContext(_ctx, getState());
		enterRule(_localctx, 84, RULE_querySpecification);
		int _la;
		try {
			int _alt;
			setState(1488);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,187,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				{
				{
				setState(1405);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case SELECT:
					{
					setState(1395);
					match(SELECT);
					setState(1396);
					((QuerySpecificationContext)_localctx).kind = match(TRANSFORM);
					setState(1397);
					match(T__0);
					setState(1398);
					namedExpressionSeq();
					setState(1399);
					match(T__1);
					}
					break;
				case MAP:
					{
					setState(1401);
					((QuerySpecificationContext)_localctx).kind = match(MAP);
					setState(1402);
					namedExpressionSeq();
					}
					break;
				case REDUCE:
					{
					setState(1403);
					((QuerySpecificationContext)_localctx).kind = match(REDUCE);
					setState(1404);
					namedExpressionSeq();
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				}
				setState(1408);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ROW) {
					{
					setState(1407);
					((QuerySpecificationContext)_localctx).inRowFormat = rowFormat();
					}
				}

				setState(1412);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==RECORDWRITER) {
					{
					setState(1410);
					match(RECORDWRITER);
					setState(1411);
					((QuerySpecificationContext)_localctx).recordWriter = match(STRING);
					}
				}

				setState(1414);
				match(USING);
				setState(1415);
				((QuerySpecificationContext)_localctx).script = match(STRING);
				setState(1428);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,171,_ctx) ) {
				case 1:
					{
					setState(1416);
					match(AS);
					setState(1426);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,170,_ctx) ) {
					case 1:
						{
						setState(1417);
						identifierSeq();
						}
						break;
					case 2:
						{
						setState(1418);
						colTypeList();
						}
						break;
					case 3:
						{
						{
						setState(1419);
						match(T__0);
						setState(1422);
						_errHandler.sync(this);
						switch ( getInterpreter().adaptivePredict(_input,169,_ctx) ) {
						case 1:
							{
							setState(1420);
							identifierSeq();
							}
							break;
						case 2:
							{
							setState(1421);
							colTypeList();
							}
							break;
						}
						setState(1424);
						match(T__1);
						}
						}
						break;
					}
					}
					break;
				}
				setState(1431);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,172,_ctx) ) {
				case 1:
					{
					setState(1430);
					((QuerySpecificationContext)_localctx).outRowFormat = rowFormat();
					}
					break;
				}
				setState(1435);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,173,_ctx) ) {
				case 1:
					{
					setState(1433);
					match(RECORDREADER);
					setState(1434);
					((QuerySpecificationContext)_localctx).recordReader = match(STRING);
					}
					break;
				}
				setState(1438);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,174,_ctx) ) {
				case 1:
					{
					setState(1437);
					fromClause();
					}
					break;
				}
				setState(1442);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,175,_ctx) ) {
				case 1:
					{
					setState(1440);
					match(WHERE);
					setState(1441);
					((QuerySpecificationContext)_localctx).where = booleanExpression(0);
					}
					break;
				}
				}
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				{
				setState(1466);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case SELECT:
					{
					setState(1444);
					((QuerySpecificationContext)_localctx).kind = match(SELECT);
					setState(1448);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__4) {
						{
						{
						setState(1445);
						((QuerySpecificationContext)_localctx).hint = hint();
						((QuerySpecificationContext)_localctx).hints.add(((QuerySpecificationContext)_localctx).hint);
						}
						}
						setState(1450);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					setState(1452);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,177,_ctx) ) {
					case 1:
						{
						setState(1451);
						setQuantifier();
						}
						break;
					}
					setState(1454);
					namedExpressionSeq();
					setState(1456);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,178,_ctx) ) {
					case 1:
						{
						setState(1455);
						fromClause();
						}
						break;
					}
					}
					break;
				case FROM:
					{
					setState(1458);
					fromClause();
					setState(1464);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,180,_ctx) ) {
					case 1:
						{
						setState(1459);
						((QuerySpecificationContext)_localctx).kind = match(SELECT);
						setState(1461);
						_errHandler.sync(this);
						switch ( getInterpreter().adaptivePredict(_input,179,_ctx) ) {
						case 1:
							{
							setState(1460);
							setQuantifier();
							}
							break;
						}
						setState(1463);
						namedExpressionSeq();
						}
						break;
					}
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				setState(1471);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,182,_ctx);
				while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						{
						{
						setState(1468);
						lateralView();
						}
						} 
					}
					setState(1473);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,182,_ctx);
				}
				setState(1476);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,183,_ctx) ) {
				case 1:
					{
					setState(1474);
					match(WHERE);
					setState(1475);
					((QuerySpecificationContext)_localctx).where = booleanExpression(0);
					}
					break;
				}
				setState(1479);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,184,_ctx) ) {
				case 1:
					{
					setState(1478);
					aggregation();
					}
					break;
				}
				setState(1483);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,185,_ctx) ) {
				case 1:
					{
					setState(1481);
					match(HAVING);
					setState(1482);
					((QuerySpecificationContext)_localctx).having = booleanExpression(0);
					}
					break;
				}
				setState(1486);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,186,_ctx) ) {
				case 1:
					{
					setState(1485);
					windows();
					}
					break;
				}
				}
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class HintContext extends ParserRuleContext {
		public HintStatementContext hintStatement;
		public List<HintStatementContext> hintStatements = new ArrayList<HintStatementContext>();
		public List<HintStatementContext> hintStatement() {
			return getRuleContexts(HintStatementContext.class);
		}
		public HintStatementContext hintStatement(int i) {
			return getRuleContext(HintStatementContext.class,i);
		}
		public HintContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_hint; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterHint(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitHint(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitHint(this);
			else return visitor.visitChildren(this);
		}
	}

	public final HintContext hint() throws RecognitionException {
		HintContext _localctx = new HintContext(_ctx, getState());
		enterRule(_localctx, 86, RULE_hint);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1490);
			match(T__4);
			setState(1491);
			((HintContext)_localctx).hintStatement = hintStatement();
			((HintContext)_localctx).hintStatements.add(((HintContext)_localctx).hintStatement);
			setState(1498);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__2) | (1L << SELECT) | (1L << FROM) | (1L << ADD) | (1L << AS) | (1L << ALL) | (1L << ANY) | (1L << DISTINCT) | (1L << WHERE) | (1L << GROUP) | (1L << BY) | (1L << GROUPING) | (1L << SETS) | (1L << CUBE) | (1L << ROLLUP) | (1L << ORDER) | (1L << HAVING) | (1L << LIMIT) | (1L << AT) | (1L << OR) | (1L << AND) | (1L << IN) | (1L << NOT) | (1L << NO) | (1L << EXISTS) | (1L << BETWEEN) | (1L << LIKE) | (1L << RLIKE) | (1L << IS) | (1L << NULL) | (1L << TRUE) | (1L << FALSE) | (1L << NULLS) | (1L << ASC) | (1L << DESC) | (1L << FOR) | (1L << INTERVAL) | (1L << CASE) | (1L << WHEN) | (1L << THEN) | (1L << ELSE) | (1L << END) | (1L << JOIN) | (1L << CROSS) | (1L << OUTER) | (1L << INNER) | (1L << LEFT) | (1L << SEMI) | (1L << RIGHT) | (1L << FULL) | (1L << NATURAL) | (1L << ON) | (1L << PIVOT) | (1L << LATERAL))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (WINDOW - 64)) | (1L << (OVER - 64)) | (1L << (PARTITION - 64)) | (1L << (RANGE - 64)) | (1L << (ROWS - 64)) | (1L << (UNBOUNDED - 64)) | (1L << (PRECEDING - 64)) | (1L << (FOLLOWING - 64)) | (1L << (CURRENT - 64)) | (1L << (FIRST - 64)) | (1L << (AFTER - 64)) | (1L << (LAST - 64)) | (1L << (ROW - 64)) | (1L << (WITH - 64)) | (1L << (VALUES - 64)) | (1L << (CREATE - 64)) | (1L << (TABLE - 64)) | (1L << (DIRECTORY - 64)) | (1L << (VIEW - 64)) | (1L << (REPLACE - 64)) | (1L << (INSERT - 64)) | (1L << (UPDATE - 64)) | (1L << (DELETE - 64)) | (1L << (INTO - 64)) | (1L << (DESCRIBE - 64)) | (1L << (EXPLAIN - 64)) | (1L << (FORMAT - 64)) | (1L << (LOGICAL - 64)) | (1L << (CODEGEN - 64)) | (1L << (COST - 64)) | (1L << (CAST - 64)) | (1L << (SHOW - 64)) | (1L << (TABLES - 64)) | (1L << (COLUMNS - 64)) | (1L << (COLUMN - 64)) | (1L << (USE - 64)) | (1L << (PARTITIONS - 64)) | (1L << (FUNCTIONS - 64)) | (1L << (DROP - 64)) | (1L << (UNION - 64)) | (1L << (EXCEPT - 64)) | (1L << (SETMINUS - 64)) | (1L << (INTERSECT - 64)) | (1L << (TO - 64)) | (1L << (TABLESAMPLE - 64)) | (1L << (STRATIFY - 64)) | (1L << (ALTER - 64)) | (1L << (RENAME - 64)) | (1L << (ARRAY - 64)) | (1L << (MAP - 64)) | (1L << (STRUCT - 64)) | (1L << (COMMENT - 64)) | (1L << (SET - 64)) | (1L << (RESET - 64)) | (1L << (DATA - 64)) | (1L << (START - 64)) | (1L << (TRANSACTION - 64)) | (1L << (COMMIT - 64)) | (1L << (ROLLBACK - 64)) | (1L << (MACRO - 64)) | (1L << (IGNORE - 64)) | (1L << (BOTH - 64)) | (1L << (LEADING - 64)) | (1L << (TRAILING - 64)))) != 0) || ((((_la - 128)) & ~0x3f) == 0 && ((1L << (_la - 128)) & ((1L << (IF - 128)) | (1L << (POSITION - 128)) | (1L << (EXTRACT - 128)) | (1L << (DIV - 128)) | (1L << (PERCENTLIT - 128)) | (1L << (BUCKET - 128)) | (1L << (OUT - 128)) | (1L << (OF - 128)) | (1L << (SORT - 128)) | (1L << (CLUSTER - 128)) | (1L << (DISTRIBUTE - 128)) | (1L << (OVERWRITE - 128)) | (1L << (TRANSFORM - 128)) | (1L << (REDUCE - 128)) | (1L << (SERDE - 128)) | (1L << (SERDEPROPERTIES - 128)) | (1L << (RECORDREADER - 128)) | (1L << (RECORDWRITER - 128)) | (1L << (DELIMITED - 128)) | (1L << (FIELDS - 128)) | (1L << (TERMINATED - 128)) | (1L << (COLLECTION - 128)) | (1L << (ITEMS - 128)) | (1L << (KEYS - 128)) | (1L << (ESCAPED - 128)) | (1L << (LINES - 128)) | (1L << (SEPARATED - 128)) | (1L << (FUNCTION - 128)) | (1L << (EXTENDED - 128)) | (1L << (REFRESH - 128)) | (1L << (CLEAR - 128)) | (1L << (CACHE - 128)) | (1L << (UNCACHE - 128)) | (1L << (LAZY - 128)) | (1L << (FORMATTED - 128)) | (1L << (GLOBAL - 128)) | (1L << (TEMPORARY - 128)) | (1L << (OPTIONS - 128)) | (1L << (UNSET - 128)) | (1L << (TBLPROPERTIES - 128)) | (1L << (DBPROPERTIES - 128)) | (1L << (BUCKETS - 128)) | (1L << (SKEWED - 128)) | (1L << (STORED - 128)) | (1L << (DIRECTORIES - 128)))) != 0) || ((((_la - 192)) & ~0x3f) == 0 && ((1L << (_la - 192)) & ((1L << (LOCATION - 192)) | (1L << (EXCHANGE - 192)) | (1L << (ARCHIVE - 192)) | (1L << (UNARCHIVE - 192)) | (1L << (FILEFORMAT - 192)) | (1L << (TOUCH - 192)) | (1L << (COMPACT - 192)) | (1L << (CONCATENATE - 192)) | (1L << (CHANGE - 192)) | (1L << (CASCADE - 192)) | (1L << (RESTRICT - 192)) | (1L << (CLUSTERED - 192)) | (1L << (SORTED - 192)) | (1L << (PURGE - 192)) | (1L << (INPUTFORMAT - 192)) | (1L << (OUTPUTFORMAT - 192)) | (1L << (DATABASE - 192)) | (1L << (DATABASES - 192)) | (1L << (DFS - 192)) | (1L << (TRUNCATE - 192)) | (1L << (ANALYZE - 192)) | (1L << (COMPUTE - 192)) | (1L << (LIST - 192)) | (1L << (STATISTICS - 192)) | (1L << (PARTITIONED - 192)) | (1L << (EXTERNAL - 192)) | (1L << (DEFINED - 192)) | (1L << (REVOKE - 192)) | (1L << (GRANT - 192)) | (1L << (LOCK - 192)) | (1L << (UNLOCK - 192)) | (1L << (MSCK - 192)) | (1L << (REPAIR - 192)) | (1L << (RECOVER - 192)) | (1L << (EXPORT - 192)) | (1L << (IMPORT - 192)) | (1L << (LOAD - 192)) | (1L << (ROLE - 192)) | (1L << (ROLES - 192)) | (1L << (COMPACTIONS - 192)) | (1L << (PRINCIPALS - 192)) | (1L << (TRANSACTIONS - 192)) | (1L << (INDEX - 192)) | (1L << (INDEXES - 192)) | (1L << (LOCKS - 192)) | (1L << (OPTION - 192)) | (1L << (ANTI - 192)) | (1L << (LOCAL - 192)) | (1L << (INPATH - 192)) | (1L << (IDENTIFIER - 192)) | (1L << (BACKQUOTED_IDENTIFIER - 192)))) != 0)) {
				{
				{
				setState(1493);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__2) {
					{
					setState(1492);
					match(T__2);
					}
				}

				setState(1495);
				((HintContext)_localctx).hintStatement = hintStatement();
				((HintContext)_localctx).hintStatements.add(((HintContext)_localctx).hintStatement);
				}
				}
				setState(1500);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(1501);
			match(T__5);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class HintStatementContext extends ParserRuleContext {
		public IdentifierContext hintName;
		public PrimaryExpressionContext primaryExpression;
		public List<PrimaryExpressionContext> parameters = new ArrayList<PrimaryExpressionContext>();
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public List<PrimaryExpressionContext> primaryExpression() {
			return getRuleContexts(PrimaryExpressionContext.class);
		}
		public PrimaryExpressionContext primaryExpression(int i) {
			return getRuleContext(PrimaryExpressionContext.class,i);
		}
		public HintStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_hintStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterHintStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitHintStatement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitHintStatement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final HintStatementContext hintStatement() throws RecognitionException {
		HintStatementContext _localctx = new HintStatementContext(_ctx, getState());
		enterRule(_localctx, 88, RULE_hintStatement);
		int _la;
		try {
			setState(1516);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,191,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1503);
				((HintStatementContext)_localctx).hintName = identifier();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1504);
				((HintStatementContext)_localctx).hintName = identifier();
				setState(1505);
				match(T__0);
				setState(1506);
				((HintStatementContext)_localctx).primaryExpression = primaryExpression(0);
				((HintStatementContext)_localctx).parameters.add(((HintStatementContext)_localctx).primaryExpression);
				setState(1511);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__2) {
					{
					{
					setState(1507);
					match(T__2);
					setState(1508);
					((HintStatementContext)_localctx).primaryExpression = primaryExpression(0);
					((HintStatementContext)_localctx).parameters.add(((HintStatementContext)_localctx).primaryExpression);
					}
					}
					setState(1513);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(1514);
				match(T__1);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class FromClauseContext extends ParserRuleContext {
		public TerminalNode FROM() { return getToken(SqlHiveParser.FROM, 0); }
		public List<RelationContext> relation() {
			return getRuleContexts(RelationContext.class);
		}
		public RelationContext relation(int i) {
			return getRuleContext(RelationContext.class,i);
		}
		public List<LateralViewContext> lateralView() {
			return getRuleContexts(LateralViewContext.class);
		}
		public LateralViewContext lateralView(int i) {
			return getRuleContext(LateralViewContext.class,i);
		}
		public PivotClauseContext pivotClause() {
			return getRuleContext(PivotClauseContext.class,0);
		}
		public FromClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_fromClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterFromClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitFromClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitFromClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final FromClauseContext fromClause() throws RecognitionException {
		FromClauseContext _localctx = new FromClauseContext(_ctx, getState());
		enterRule(_localctx, 90, RULE_fromClause);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1518);
			match(FROM);
			setState(1519);
			relation();
			setState(1524);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,192,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(1520);
					match(T__2);
					setState(1521);
					relation();
					}
					} 
				}
				setState(1526);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,192,_ctx);
			}
			setState(1530);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,193,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(1527);
					lateralView();
					}
					} 
				}
				setState(1532);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,193,_ctx);
			}
			setState(1534);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,194,_ctx) ) {
			case 1:
				{
				setState(1533);
				pivotClause();
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class AggregationContext extends ParserRuleContext {
		public ExpressionContext expression;
		public List<ExpressionContext> groupingExpressions = new ArrayList<ExpressionContext>();
		public Token kind;
		public TerminalNode GROUP() { return getToken(SqlHiveParser.GROUP, 0); }
		public TerminalNode BY() { return getToken(SqlHiveParser.BY, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TerminalNode WITH() { return getToken(SqlHiveParser.WITH, 0); }
		public TerminalNode SETS() { return getToken(SqlHiveParser.SETS, 0); }
		public List<GroupingSetContext> groupingSet() {
			return getRuleContexts(GroupingSetContext.class);
		}
		public GroupingSetContext groupingSet(int i) {
			return getRuleContext(GroupingSetContext.class,i);
		}
		public TerminalNode ROLLUP() { return getToken(SqlHiveParser.ROLLUP, 0); }
		public TerminalNode CUBE() { return getToken(SqlHiveParser.CUBE, 0); }
		public TerminalNode GROUPING() { return getToken(SqlHiveParser.GROUPING, 0); }
		public AggregationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_aggregation; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterAggregation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitAggregation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitAggregation(this);
			else return visitor.visitChildren(this);
		}
	}

	public final AggregationContext aggregation() throws RecognitionException {
		AggregationContext _localctx = new AggregationContext(_ctx, getState());
		enterRule(_localctx, 92, RULE_aggregation);
		int _la;
		try {
			int _alt;
			setState(1580);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,199,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1536);
				match(GROUP);
				setState(1537);
				match(BY);
				setState(1538);
				((AggregationContext)_localctx).expression = expression();
				((AggregationContext)_localctx).groupingExpressions.add(((AggregationContext)_localctx).expression);
				setState(1543);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,195,_ctx);
				while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						{
						{
						setState(1539);
						match(T__2);
						setState(1540);
						((AggregationContext)_localctx).expression = expression();
						((AggregationContext)_localctx).groupingExpressions.add(((AggregationContext)_localctx).expression);
						}
						} 
					}
					setState(1545);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,195,_ctx);
				}
				setState(1563);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,197,_ctx) ) {
				case 1:
					{
					setState(1546);
					match(WITH);
					setState(1547);
					((AggregationContext)_localctx).kind = match(ROLLUP);
					}
					break;
				case 2:
					{
					setState(1548);
					match(WITH);
					setState(1549);
					((AggregationContext)_localctx).kind = match(CUBE);
					}
					break;
				case 3:
					{
					setState(1550);
					((AggregationContext)_localctx).kind = match(GROUPING);
					setState(1551);
					match(SETS);
					setState(1552);
					match(T__0);
					setState(1553);
					groupingSet();
					setState(1558);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__2) {
						{
						{
						setState(1554);
						match(T__2);
						setState(1555);
						groupingSet();
						}
						}
						setState(1560);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					setState(1561);
					match(T__1);
					}
					break;
				}
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1565);
				match(GROUP);
				setState(1566);
				match(BY);
				setState(1567);
				((AggregationContext)_localctx).kind = match(GROUPING);
				setState(1568);
				match(SETS);
				setState(1569);
				match(T__0);
				setState(1570);
				groupingSet();
				setState(1575);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__2) {
					{
					{
					setState(1571);
					match(T__2);
					setState(1572);
					groupingSet();
					}
					}
					setState(1577);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(1578);
				match(T__1);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class GroupingSetContext extends ParserRuleContext {
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public GroupingSetContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_groupingSet; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterGroupingSet(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitGroupingSet(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitGroupingSet(this);
			else return visitor.visitChildren(this);
		}
	}

	public final GroupingSetContext groupingSet() throws RecognitionException {
		GroupingSetContext _localctx = new GroupingSetContext(_ctx, getState());
		enterRule(_localctx, 94, RULE_groupingSet);
		int _la;
		try {
			setState(1595);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,202,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1582);
				match(T__0);
				setState(1591);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__0) | (1L << SELECT) | (1L << FROM) | (1L << ADD) | (1L << AS) | (1L << ALL) | (1L << ANY) | (1L << DISTINCT) | (1L << WHERE) | (1L << GROUP) | (1L << BY) | (1L << GROUPING) | (1L << SETS) | (1L << CUBE) | (1L << ROLLUP) | (1L << ORDER) | (1L << HAVING) | (1L << LIMIT) | (1L << AT) | (1L << OR) | (1L << AND) | (1L << IN) | (1L << NOT) | (1L << NO) | (1L << EXISTS) | (1L << BETWEEN) | (1L << LIKE) | (1L << RLIKE) | (1L << IS) | (1L << NULL) | (1L << TRUE) | (1L << FALSE) | (1L << NULLS) | (1L << ASC) | (1L << DESC) | (1L << FOR) | (1L << INTERVAL) | (1L << CASE) | (1L << WHEN) | (1L << THEN) | (1L << ELSE) | (1L << END) | (1L << JOIN) | (1L << CROSS) | (1L << OUTER) | (1L << INNER) | (1L << LEFT) | (1L << SEMI) | (1L << RIGHT) | (1L << FULL) | (1L << NATURAL) | (1L << ON) | (1L << PIVOT) | (1L << LATERAL))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (WINDOW - 64)) | (1L << (OVER - 64)) | (1L << (PARTITION - 64)) | (1L << (RANGE - 64)) | (1L << (ROWS - 64)) | (1L << (UNBOUNDED - 64)) | (1L << (PRECEDING - 64)) | (1L << (FOLLOWING - 64)) | (1L << (CURRENT - 64)) | (1L << (FIRST - 64)) | (1L << (AFTER - 64)) | (1L << (LAST - 64)) | (1L << (ROW - 64)) | (1L << (WITH - 64)) | (1L << (VALUES - 64)) | (1L << (CREATE - 64)) | (1L << (TABLE - 64)) | (1L << (DIRECTORY - 64)) | (1L << (VIEW - 64)) | (1L << (REPLACE - 64)) | (1L << (INSERT - 64)) | (1L << (UPDATE - 64)) | (1L << (DELETE - 64)) | (1L << (INTO - 64)) | (1L << (DESCRIBE - 64)) | (1L << (EXPLAIN - 64)) | (1L << (FORMAT - 64)) | (1L << (LOGICAL - 64)) | (1L << (CODEGEN - 64)) | (1L << (COST - 64)) | (1L << (CAST - 64)) | (1L << (SHOW - 64)) | (1L << (TABLES - 64)) | (1L << (COLUMNS - 64)) | (1L << (COLUMN - 64)) | (1L << (USE - 64)) | (1L << (PARTITIONS - 64)) | (1L << (FUNCTIONS - 64)) | (1L << (DROP - 64)) | (1L << (UNION - 64)) | (1L << (EXCEPT - 64)) | (1L << (SETMINUS - 64)) | (1L << (INTERSECT - 64)) | (1L << (TO - 64)) | (1L << (TABLESAMPLE - 64)) | (1L << (STRATIFY - 64)) | (1L << (ALTER - 64)) | (1L << (RENAME - 64)) | (1L << (ARRAY - 64)) | (1L << (MAP - 64)) | (1L << (STRUCT - 64)) | (1L << (COMMENT - 64)) | (1L << (SET - 64)) | (1L << (RESET - 64)) | (1L << (DATA - 64)) | (1L << (START - 64)) | (1L << (TRANSACTION - 64)) | (1L << (COMMIT - 64)) | (1L << (ROLLBACK - 64)) | (1L << (MACRO - 64)) | (1L << (IGNORE - 64)) | (1L << (BOTH - 64)) | (1L << (LEADING - 64)) | (1L << (TRAILING - 64)))) != 0) || ((((_la - 128)) & ~0x3f) == 0 && ((1L << (_la - 128)) & ((1L << (IF - 128)) | (1L << (POSITION - 128)) | (1L << (EXTRACT - 128)) | (1L << (PLUS - 128)) | (1L << (MINUS - 128)) | (1L << (ASTERISK - 128)) | (1L << (DIV - 128)) | (1L << (TILDE - 128)) | (1L << (PERCENTLIT - 128)) | (1L << (BUCKET - 128)) | (1L << (OUT - 128)) | (1L << (OF - 128)) | (1L << (SORT - 128)) | (1L << (CLUSTER - 128)) | (1L << (DISTRIBUTE - 128)) | (1L << (OVERWRITE - 128)) | (1L << (TRANSFORM - 128)) | (1L << (REDUCE - 128)) | (1L << (SERDE - 128)) | (1L << (SERDEPROPERTIES - 128)) | (1L << (RECORDREADER - 128)) | (1L << (RECORDWRITER - 128)) | (1L << (DELIMITED - 128)) | (1L << (FIELDS - 128)) | (1L << (TERMINATED - 128)) | (1L << (COLLECTION - 128)) | (1L << (ITEMS - 128)) | (1L << (KEYS - 128)) | (1L << (ESCAPED - 128)) | (1L << (LINES - 128)) | (1L << (SEPARATED - 128)) | (1L << (FUNCTION - 128)) | (1L << (EXTENDED - 128)) | (1L << (REFRESH - 128)) | (1L << (CLEAR - 128)) | (1L << (CACHE - 128)) | (1L << (UNCACHE - 128)) | (1L << (LAZY - 128)) | (1L << (FORMATTED - 128)) | (1L << (GLOBAL - 128)) | (1L << (TEMPORARY - 128)) | (1L << (OPTIONS - 128)) | (1L << (UNSET - 128)) | (1L << (TBLPROPERTIES - 128)) | (1L << (DBPROPERTIES - 128)) | (1L << (BUCKETS - 128)) | (1L << (SKEWED - 128)) | (1L << (STORED - 128)) | (1L << (DIRECTORIES - 128)))) != 0) || ((((_la - 192)) & ~0x3f) == 0 && ((1L << (_la - 192)) & ((1L << (LOCATION - 192)) | (1L << (EXCHANGE - 192)) | (1L << (ARCHIVE - 192)) | (1L << (UNARCHIVE - 192)) | (1L << (FILEFORMAT - 192)) | (1L << (TOUCH - 192)) | (1L << (COMPACT - 192)) | (1L << (CONCATENATE - 192)) | (1L << (CHANGE - 192)) | (1L << (CASCADE - 192)) | (1L << (RESTRICT - 192)) | (1L << (CLUSTERED - 192)) | (1L << (SORTED - 192)) | (1L << (PURGE - 192)) | (1L << (INPUTFORMAT - 192)) | (1L << (OUTPUTFORMAT - 192)) | (1L << (DATABASE - 192)) | (1L << (DATABASES - 192)) | (1L << (DFS - 192)) | (1L << (TRUNCATE - 192)) | (1L << (ANALYZE - 192)) | (1L << (COMPUTE - 192)) | (1L << (LIST - 192)) | (1L << (STATISTICS - 192)) | (1L << (PARTITIONED - 192)) | (1L << (EXTERNAL - 192)) | (1L << (DEFINED - 192)) | (1L << (REVOKE - 192)) | (1L << (GRANT - 192)) | (1L << (LOCK - 192)) | (1L << (UNLOCK - 192)) | (1L << (MSCK - 192)) | (1L << (REPAIR - 192)) | (1L << (RECOVER - 192)) | (1L << (EXPORT - 192)) | (1L << (IMPORT - 192)) | (1L << (LOAD - 192)) | (1L << (ROLE - 192)) | (1L << (ROLES - 192)) | (1L << (COMPACTIONS - 192)) | (1L << (PRINCIPALS - 192)) | (1L << (TRANSACTIONS - 192)) | (1L << (INDEX - 192)) | (1L << (INDEXES - 192)) | (1L << (LOCKS - 192)) | (1L << (OPTION - 192)) | (1L << (ANTI - 192)) | (1L << (LOCAL - 192)) | (1L << (INPATH - 192)) | (1L << (STRING - 192)) | (1L << (BIGINT_LITERAL - 192)) | (1L << (SMALLINT_LITERAL - 192)) | (1L << (TINYINT_LITERAL - 192)) | (1L << (INTEGER_VALUE - 192)) | (1L << (DECIMAL_VALUE - 192)) | (1L << (DOUBLE_LITERAL - 192)) | (1L << (BIGDECIMAL_LITERAL - 192)) | (1L << (IDENTIFIER - 192)) | (1L << (BACKQUOTED_IDENTIFIER - 192)))) != 0)) {
					{
					setState(1583);
					expression();
					setState(1588);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__2) {
						{
						{
						setState(1584);
						match(T__2);
						setState(1585);
						expression();
						}
						}
						setState(1590);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				setState(1593);
				match(T__1);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1594);
				expression();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class PivotClauseContext extends ParserRuleContext {
		public NamedExpressionSeqContext aggregates;
		public PivotValueContext pivotValue;
		public List<PivotValueContext> pivotValues = new ArrayList<PivotValueContext>();
		public TerminalNode PIVOT() { return getToken(SqlHiveParser.PIVOT, 0); }
		public TerminalNode FOR() { return getToken(SqlHiveParser.FOR, 0); }
		public PivotColumnContext pivotColumn() {
			return getRuleContext(PivotColumnContext.class,0);
		}
		public TerminalNode IN() { return getToken(SqlHiveParser.IN, 0); }
		public NamedExpressionSeqContext namedExpressionSeq() {
			return getRuleContext(NamedExpressionSeqContext.class,0);
		}
		public List<PivotValueContext> pivotValue() {
			return getRuleContexts(PivotValueContext.class);
		}
		public PivotValueContext pivotValue(int i) {
			return getRuleContext(PivotValueContext.class,i);
		}
		public PivotClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_pivotClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterPivotClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitPivotClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitPivotClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PivotClauseContext pivotClause() throws RecognitionException {
		PivotClauseContext _localctx = new PivotClauseContext(_ctx, getState());
		enterRule(_localctx, 96, RULE_pivotClause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1597);
			match(PIVOT);
			setState(1598);
			match(T__0);
			setState(1599);
			((PivotClauseContext)_localctx).aggregates = namedExpressionSeq();
			setState(1600);
			match(FOR);
			setState(1601);
			pivotColumn();
			setState(1602);
			match(IN);
			setState(1603);
			match(T__0);
			setState(1604);
			((PivotClauseContext)_localctx).pivotValue = pivotValue();
			((PivotClauseContext)_localctx).pivotValues.add(((PivotClauseContext)_localctx).pivotValue);
			setState(1609);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__2) {
				{
				{
				setState(1605);
				match(T__2);
				setState(1606);
				((PivotClauseContext)_localctx).pivotValue = pivotValue();
				((PivotClauseContext)_localctx).pivotValues.add(((PivotClauseContext)_localctx).pivotValue);
				}
				}
				setState(1611);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(1612);
			match(T__1);
			setState(1613);
			match(T__1);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class PivotColumnContext extends ParserRuleContext {
		public IdentifierContext identifier;
		public List<IdentifierContext> identifiers = new ArrayList<IdentifierContext>();
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public PivotColumnContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_pivotColumn; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterPivotColumn(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitPivotColumn(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitPivotColumn(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PivotColumnContext pivotColumn() throws RecognitionException {
		PivotColumnContext _localctx = new PivotColumnContext(_ctx, getState());
		enterRule(_localctx, 98, RULE_pivotColumn);
		int _la;
		try {
			setState(1627);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case SELECT:
			case FROM:
			case ADD:
			case AS:
			case ALL:
			case ANY:
			case DISTINCT:
			case WHERE:
			case GROUP:
			case BY:
			case GROUPING:
			case SETS:
			case CUBE:
			case ROLLUP:
			case ORDER:
			case HAVING:
			case LIMIT:
			case AT:
			case OR:
			case AND:
			case IN:
			case NOT:
			case NO:
			case EXISTS:
			case BETWEEN:
			case LIKE:
			case RLIKE:
			case IS:
			case NULL:
			case TRUE:
			case FALSE:
			case NULLS:
			case ASC:
			case DESC:
			case FOR:
			case INTERVAL:
			case CASE:
			case WHEN:
			case THEN:
			case ELSE:
			case END:
			case JOIN:
			case CROSS:
			case OUTER:
			case INNER:
			case LEFT:
			case SEMI:
			case RIGHT:
			case FULL:
			case NATURAL:
			case ON:
			case PIVOT:
			case LATERAL:
			case WINDOW:
			case OVER:
			case PARTITION:
			case RANGE:
			case ROWS:
			case UNBOUNDED:
			case PRECEDING:
			case FOLLOWING:
			case CURRENT:
			case FIRST:
			case AFTER:
			case LAST:
			case ROW:
			case WITH:
			case VALUES:
			case CREATE:
			case TABLE:
			case DIRECTORY:
			case VIEW:
			case REPLACE:
			case INSERT:
			case UPDATE:
			case DELETE:
			case INTO:
			case DESCRIBE:
			case EXPLAIN:
			case FORMAT:
			case LOGICAL:
			case CODEGEN:
			case COST:
			case CAST:
			case SHOW:
			case TABLES:
			case COLUMNS:
			case COLUMN:
			case USE:
			case PARTITIONS:
			case FUNCTIONS:
			case DROP:
			case UNION:
			case EXCEPT:
			case SETMINUS:
			case INTERSECT:
			case TO:
			case TABLESAMPLE:
			case STRATIFY:
			case ALTER:
			case RENAME:
			case ARRAY:
			case MAP:
			case STRUCT:
			case COMMENT:
			case SET:
			case RESET:
			case DATA:
			case START:
			case TRANSACTION:
			case COMMIT:
			case ROLLBACK:
			case MACRO:
			case IGNORE:
			case BOTH:
			case LEADING:
			case TRAILING:
			case IF:
			case POSITION:
			case EXTRACT:
			case DIV:
			case PERCENTLIT:
			case BUCKET:
			case OUT:
			case OF:
			case SORT:
			case CLUSTER:
			case DISTRIBUTE:
			case OVERWRITE:
			case TRANSFORM:
			case REDUCE:
			case SERDE:
			case SERDEPROPERTIES:
			case RECORDREADER:
			case RECORDWRITER:
			case DELIMITED:
			case FIELDS:
			case TERMINATED:
			case COLLECTION:
			case ITEMS:
			case KEYS:
			case ESCAPED:
			case LINES:
			case SEPARATED:
			case FUNCTION:
			case EXTENDED:
			case REFRESH:
			case CLEAR:
			case CACHE:
			case UNCACHE:
			case LAZY:
			case FORMATTED:
			case GLOBAL:
			case TEMPORARY:
			case OPTIONS:
			case UNSET:
			case TBLPROPERTIES:
			case DBPROPERTIES:
			case BUCKETS:
			case SKEWED:
			case STORED:
			case DIRECTORIES:
			case LOCATION:
			case EXCHANGE:
			case ARCHIVE:
			case UNARCHIVE:
			case FILEFORMAT:
			case TOUCH:
			case COMPACT:
			case CONCATENATE:
			case CHANGE:
			case CASCADE:
			case RESTRICT:
			case CLUSTERED:
			case SORTED:
			case PURGE:
			case INPUTFORMAT:
			case OUTPUTFORMAT:
			case DATABASE:
			case DATABASES:
			case DFS:
			case TRUNCATE:
			case ANALYZE:
			case COMPUTE:
			case LIST:
			case STATISTICS:
			case PARTITIONED:
			case EXTERNAL:
			case DEFINED:
			case REVOKE:
			case GRANT:
			case LOCK:
			case UNLOCK:
			case MSCK:
			case REPAIR:
			case RECOVER:
			case EXPORT:
			case IMPORT:
			case LOAD:
			case ROLE:
			case ROLES:
			case COMPACTIONS:
			case PRINCIPALS:
			case TRANSACTIONS:
			case INDEX:
			case INDEXES:
			case LOCKS:
			case OPTION:
			case ANTI:
			case LOCAL:
			case INPATH:
			case IDENTIFIER:
			case BACKQUOTED_IDENTIFIER:
				enterOuterAlt(_localctx, 1);
				{
				setState(1615);
				((PivotColumnContext)_localctx).identifier = identifier();
				((PivotColumnContext)_localctx).identifiers.add(((PivotColumnContext)_localctx).identifier);
				}
				break;
			case T__0:
				enterOuterAlt(_localctx, 2);
				{
				setState(1616);
				match(T__0);
				setState(1617);
				((PivotColumnContext)_localctx).identifier = identifier();
				((PivotColumnContext)_localctx).identifiers.add(((PivotColumnContext)_localctx).identifier);
				setState(1622);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__2) {
					{
					{
					setState(1618);
					match(T__2);
					setState(1619);
					((PivotColumnContext)_localctx).identifier = identifier();
					((PivotColumnContext)_localctx).identifiers.add(((PivotColumnContext)_localctx).identifier);
					}
					}
					setState(1624);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(1625);
				match(T__1);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class PivotValueContext extends ParserRuleContext {
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlHiveParser.AS, 0); }
		public PivotValueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_pivotValue; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterPivotValue(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitPivotValue(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitPivotValue(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PivotValueContext pivotValue() throws RecognitionException {
		PivotValueContext _localctx = new PivotValueContext(_ctx, getState());
		enterRule(_localctx, 100, RULE_pivotValue);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1629);
			expression();
			setState(1634);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << SELECT) | (1L << FROM) | (1L << ADD) | (1L << AS) | (1L << ALL) | (1L << ANY) | (1L << DISTINCT) | (1L << WHERE) | (1L << GROUP) | (1L << BY) | (1L << GROUPING) | (1L << SETS) | (1L << CUBE) | (1L << ROLLUP) | (1L << ORDER) | (1L << HAVING) | (1L << LIMIT) | (1L << AT) | (1L << OR) | (1L << AND) | (1L << IN) | (1L << NOT) | (1L << NO) | (1L << EXISTS) | (1L << BETWEEN) | (1L << LIKE) | (1L << RLIKE) | (1L << IS) | (1L << NULL) | (1L << TRUE) | (1L << FALSE) | (1L << NULLS) | (1L << ASC) | (1L << DESC) | (1L << FOR) | (1L << INTERVAL) | (1L << CASE) | (1L << WHEN) | (1L << THEN) | (1L << ELSE) | (1L << END) | (1L << JOIN) | (1L << CROSS) | (1L << OUTER) | (1L << INNER) | (1L << LEFT) | (1L << SEMI) | (1L << RIGHT) | (1L << FULL) | (1L << NATURAL) | (1L << ON) | (1L << PIVOT) | (1L << LATERAL))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (WINDOW - 64)) | (1L << (OVER - 64)) | (1L << (PARTITION - 64)) | (1L << (RANGE - 64)) | (1L << (ROWS - 64)) | (1L << (UNBOUNDED - 64)) | (1L << (PRECEDING - 64)) | (1L << (FOLLOWING - 64)) | (1L << (CURRENT - 64)) | (1L << (FIRST - 64)) | (1L << (AFTER - 64)) | (1L << (LAST - 64)) | (1L << (ROW - 64)) | (1L << (WITH - 64)) | (1L << (VALUES - 64)) | (1L << (CREATE - 64)) | (1L << (TABLE - 64)) | (1L << (DIRECTORY - 64)) | (1L << (VIEW - 64)) | (1L << (REPLACE - 64)) | (1L << (INSERT - 64)) | (1L << (UPDATE - 64)) | (1L << (DELETE - 64)) | (1L << (INTO - 64)) | (1L << (DESCRIBE - 64)) | (1L << (EXPLAIN - 64)) | (1L << (FORMAT - 64)) | (1L << (LOGICAL - 64)) | (1L << (CODEGEN - 64)) | (1L << (COST - 64)) | (1L << (CAST - 64)) | (1L << (SHOW - 64)) | (1L << (TABLES - 64)) | (1L << (COLUMNS - 64)) | (1L << (COLUMN - 64)) | (1L << (USE - 64)) | (1L << (PARTITIONS - 64)) | (1L << (FUNCTIONS - 64)) | (1L << (DROP - 64)) | (1L << (UNION - 64)) | (1L << (EXCEPT - 64)) | (1L << (SETMINUS - 64)) | (1L << (INTERSECT - 64)) | (1L << (TO - 64)) | (1L << (TABLESAMPLE - 64)) | (1L << (STRATIFY - 64)) | (1L << (ALTER - 64)) | (1L << (RENAME - 64)) | (1L << (ARRAY - 64)) | (1L << (MAP - 64)) | (1L << (STRUCT - 64)) | (1L << (COMMENT - 64)) | (1L << (SET - 64)) | (1L << (RESET - 64)) | (1L << (DATA - 64)) | (1L << (START - 64)) | (1L << (TRANSACTION - 64)) | (1L << (COMMIT - 64)) | (1L << (ROLLBACK - 64)) | (1L << (MACRO - 64)) | (1L << (IGNORE - 64)) | (1L << (BOTH - 64)) | (1L << (LEADING - 64)) | (1L << (TRAILING - 64)))) != 0) || ((((_la - 128)) & ~0x3f) == 0 && ((1L << (_la - 128)) & ((1L << (IF - 128)) | (1L << (POSITION - 128)) | (1L << (EXTRACT - 128)) | (1L << (DIV - 128)) | (1L << (PERCENTLIT - 128)) | (1L << (BUCKET - 128)) | (1L << (OUT - 128)) | (1L << (OF - 128)) | (1L << (SORT - 128)) | (1L << (CLUSTER - 128)) | (1L << (DISTRIBUTE - 128)) | (1L << (OVERWRITE - 128)) | (1L << (TRANSFORM - 128)) | (1L << (REDUCE - 128)) | (1L << (SERDE - 128)) | (1L << (SERDEPROPERTIES - 128)) | (1L << (RECORDREADER - 128)) | (1L << (RECORDWRITER - 128)) | (1L << (DELIMITED - 128)) | (1L << (FIELDS - 128)) | (1L << (TERMINATED - 128)) | (1L << (COLLECTION - 128)) | (1L << (ITEMS - 128)) | (1L << (KEYS - 128)) | (1L << (ESCAPED - 128)) | (1L << (LINES - 128)) | (1L << (SEPARATED - 128)) | (1L << (FUNCTION - 128)) | (1L << (EXTENDED - 128)) | (1L << (REFRESH - 128)) | (1L << (CLEAR - 128)) | (1L << (CACHE - 128)) | (1L << (UNCACHE - 128)) | (1L << (LAZY - 128)) | (1L << (FORMATTED - 128)) | (1L << (GLOBAL - 128)) | (1L << (TEMPORARY - 128)) | (1L << (OPTIONS - 128)) | (1L << (UNSET - 128)) | (1L << (TBLPROPERTIES - 128)) | (1L << (DBPROPERTIES - 128)) | (1L << (BUCKETS - 128)) | (1L << (SKEWED - 128)) | (1L << (STORED - 128)) | (1L << (DIRECTORIES - 128)))) != 0) || ((((_la - 192)) & ~0x3f) == 0 && ((1L << (_la - 192)) & ((1L << (LOCATION - 192)) | (1L << (EXCHANGE - 192)) | (1L << (ARCHIVE - 192)) | (1L << (UNARCHIVE - 192)) | (1L << (FILEFORMAT - 192)) | (1L << (TOUCH - 192)) | (1L << (COMPACT - 192)) | (1L << (CONCATENATE - 192)) | (1L << (CHANGE - 192)) | (1L << (CASCADE - 192)) | (1L << (RESTRICT - 192)) | (1L << (CLUSTERED - 192)) | (1L << (SORTED - 192)) | (1L << (PURGE - 192)) | (1L << (INPUTFORMAT - 192)) | (1L << (OUTPUTFORMAT - 192)) | (1L << (DATABASE - 192)) | (1L << (DATABASES - 192)) | (1L << (DFS - 192)) | (1L << (TRUNCATE - 192)) | (1L << (ANALYZE - 192)) | (1L << (COMPUTE - 192)) | (1L << (LIST - 192)) | (1L << (STATISTICS - 192)) | (1L << (PARTITIONED - 192)) | (1L << (EXTERNAL - 192)) | (1L << (DEFINED - 192)) | (1L << (REVOKE - 192)) | (1L << (GRANT - 192)) | (1L << (LOCK - 192)) | (1L << (UNLOCK - 192)) | (1L << (MSCK - 192)) | (1L << (REPAIR - 192)) | (1L << (RECOVER - 192)) | (1L << (EXPORT - 192)) | (1L << (IMPORT - 192)) | (1L << (LOAD - 192)) | (1L << (ROLE - 192)) | (1L << (ROLES - 192)) | (1L << (COMPACTIONS - 192)) | (1L << (PRINCIPALS - 192)) | (1L << (TRANSACTIONS - 192)) | (1L << (INDEX - 192)) | (1L << (INDEXES - 192)) | (1L << (LOCKS - 192)) | (1L << (OPTION - 192)) | (1L << (ANTI - 192)) | (1L << (LOCAL - 192)) | (1L << (INPATH - 192)) | (1L << (IDENTIFIER - 192)) | (1L << (BACKQUOTED_IDENTIFIER - 192)))) != 0)) {
				{
				setState(1631);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,206,_ctx) ) {
				case 1:
					{
					setState(1630);
					match(AS);
					}
					break;
				}
				setState(1633);
				identifier();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class LateralViewContext extends ParserRuleContext {
		public IdentifierContext tblName;
		public IdentifierContext identifier;
		public List<IdentifierContext> colName = new ArrayList<IdentifierContext>();
		public TerminalNode LATERAL() { return getToken(SqlHiveParser.LATERAL, 0); }
		public TerminalNode VIEW() { return getToken(SqlHiveParser.VIEW, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public TerminalNode OUTER() { return getToken(SqlHiveParser.OUTER, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TerminalNode AS() { return getToken(SqlHiveParser.AS, 0); }
		public LateralViewContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_lateralView; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterLateralView(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitLateralView(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitLateralView(this);
			else return visitor.visitChildren(this);
		}
	}

	public final LateralViewContext lateralView() throws RecognitionException {
		LateralViewContext _localctx = new LateralViewContext(_ctx, getState());
		enterRule(_localctx, 102, RULE_lateralView);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1636);
			match(LATERAL);
			setState(1637);
			match(VIEW);
			setState(1639);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,208,_ctx) ) {
			case 1:
				{
				setState(1638);
				match(OUTER);
				}
				break;
			}
			setState(1641);
			qualifiedName();
			setState(1642);
			match(T__0);
			setState(1651);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__0) | (1L << SELECT) | (1L << FROM) | (1L << ADD) | (1L << AS) | (1L << ALL) | (1L << ANY) | (1L << DISTINCT) | (1L << WHERE) | (1L << GROUP) | (1L << BY) | (1L << GROUPING) | (1L << SETS) | (1L << CUBE) | (1L << ROLLUP) | (1L << ORDER) | (1L << HAVING) | (1L << LIMIT) | (1L << AT) | (1L << OR) | (1L << AND) | (1L << IN) | (1L << NOT) | (1L << NO) | (1L << EXISTS) | (1L << BETWEEN) | (1L << LIKE) | (1L << RLIKE) | (1L << IS) | (1L << NULL) | (1L << TRUE) | (1L << FALSE) | (1L << NULLS) | (1L << ASC) | (1L << DESC) | (1L << FOR) | (1L << INTERVAL) | (1L << CASE) | (1L << WHEN) | (1L << THEN) | (1L << ELSE) | (1L << END) | (1L << JOIN) | (1L << CROSS) | (1L << OUTER) | (1L << INNER) | (1L << LEFT) | (1L << SEMI) | (1L << RIGHT) | (1L << FULL) | (1L << NATURAL) | (1L << ON) | (1L << PIVOT) | (1L << LATERAL))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (WINDOW - 64)) | (1L << (OVER - 64)) | (1L << (PARTITION - 64)) | (1L << (RANGE - 64)) | (1L << (ROWS - 64)) | (1L << (UNBOUNDED - 64)) | (1L << (PRECEDING - 64)) | (1L << (FOLLOWING - 64)) | (1L << (CURRENT - 64)) | (1L << (FIRST - 64)) | (1L << (AFTER - 64)) | (1L << (LAST - 64)) | (1L << (ROW - 64)) | (1L << (WITH - 64)) | (1L << (VALUES - 64)) | (1L << (CREATE - 64)) | (1L << (TABLE - 64)) | (1L << (DIRECTORY - 64)) | (1L << (VIEW - 64)) | (1L << (REPLACE - 64)) | (1L << (INSERT - 64)) | (1L << (UPDATE - 64)) | (1L << (DELETE - 64)) | (1L << (INTO - 64)) | (1L << (DESCRIBE - 64)) | (1L << (EXPLAIN - 64)) | (1L << (FORMAT - 64)) | (1L << (LOGICAL - 64)) | (1L << (CODEGEN - 64)) | (1L << (COST - 64)) | (1L << (CAST - 64)) | (1L << (SHOW - 64)) | (1L << (TABLES - 64)) | (1L << (COLUMNS - 64)) | (1L << (COLUMN - 64)) | (1L << (USE - 64)) | (1L << (PARTITIONS - 64)) | (1L << (FUNCTIONS - 64)) | (1L << (DROP - 64)) | (1L << (UNION - 64)) | (1L << (EXCEPT - 64)) | (1L << (SETMINUS - 64)) | (1L << (INTERSECT - 64)) | (1L << (TO - 64)) | (1L << (TABLESAMPLE - 64)) | (1L << (STRATIFY - 64)) | (1L << (ALTER - 64)) | (1L << (RENAME - 64)) | (1L << (ARRAY - 64)) | (1L << (MAP - 64)) | (1L << (STRUCT - 64)) | (1L << (COMMENT - 64)) | (1L << (SET - 64)) | (1L << (RESET - 64)) | (1L << (DATA - 64)) | (1L << (START - 64)) | (1L << (TRANSACTION - 64)) | (1L << (COMMIT - 64)) | (1L << (ROLLBACK - 64)) | (1L << (MACRO - 64)) | (1L << (IGNORE - 64)) | (1L << (BOTH - 64)) | (1L << (LEADING - 64)) | (1L << (TRAILING - 64)))) != 0) || ((((_la - 128)) & ~0x3f) == 0 && ((1L << (_la - 128)) & ((1L << (IF - 128)) | (1L << (POSITION - 128)) | (1L << (EXTRACT - 128)) | (1L << (PLUS - 128)) | (1L << (MINUS - 128)) | (1L << (ASTERISK - 128)) | (1L << (DIV - 128)) | (1L << (TILDE - 128)) | (1L << (PERCENTLIT - 128)) | (1L << (BUCKET - 128)) | (1L << (OUT - 128)) | (1L << (OF - 128)) | (1L << (SORT - 128)) | (1L << (CLUSTER - 128)) | (1L << (DISTRIBUTE - 128)) | (1L << (OVERWRITE - 128)) | (1L << (TRANSFORM - 128)) | (1L << (REDUCE - 128)) | (1L << (SERDE - 128)) | (1L << (SERDEPROPERTIES - 128)) | (1L << (RECORDREADER - 128)) | (1L << (RECORDWRITER - 128)) | (1L << (DELIMITED - 128)) | (1L << (FIELDS - 128)) | (1L << (TERMINATED - 128)) | (1L << (COLLECTION - 128)) | (1L << (ITEMS - 128)) | (1L << (KEYS - 128)) | (1L << (ESCAPED - 128)) | (1L << (LINES - 128)) | (1L << (SEPARATED - 128)) | (1L << (FUNCTION - 128)) | (1L << (EXTENDED - 128)) | (1L << (REFRESH - 128)) | (1L << (CLEAR - 128)) | (1L << (CACHE - 128)) | (1L << (UNCACHE - 128)) | (1L << (LAZY - 128)) | (1L << (FORMATTED - 128)) | (1L << (GLOBAL - 128)) | (1L << (TEMPORARY - 128)) | (1L << (OPTIONS - 128)) | (1L << (UNSET - 128)) | (1L << (TBLPROPERTIES - 128)) | (1L << (DBPROPERTIES - 128)) | (1L << (BUCKETS - 128)) | (1L << (SKEWED - 128)) | (1L << (STORED - 128)) | (1L << (DIRECTORIES - 128)))) != 0) || ((((_la - 192)) & ~0x3f) == 0 && ((1L << (_la - 192)) & ((1L << (LOCATION - 192)) | (1L << (EXCHANGE - 192)) | (1L << (ARCHIVE - 192)) | (1L << (UNARCHIVE - 192)) | (1L << (FILEFORMAT - 192)) | (1L << (TOUCH - 192)) | (1L << (COMPACT - 192)) | (1L << (CONCATENATE - 192)) | (1L << (CHANGE - 192)) | (1L << (CASCADE - 192)) | (1L << (RESTRICT - 192)) | (1L << (CLUSTERED - 192)) | (1L << (SORTED - 192)) | (1L << (PURGE - 192)) | (1L << (INPUTFORMAT - 192)) | (1L << (OUTPUTFORMAT - 192)) | (1L << (DATABASE - 192)) | (1L << (DATABASES - 192)) | (1L << (DFS - 192)) | (1L << (TRUNCATE - 192)) | (1L << (ANALYZE - 192)) | (1L << (COMPUTE - 192)) | (1L << (LIST - 192)) | (1L << (STATISTICS - 192)) | (1L << (PARTITIONED - 192)) | (1L << (EXTERNAL - 192)) | (1L << (DEFINED - 192)) | (1L << (REVOKE - 192)) | (1L << (GRANT - 192)) | (1L << (LOCK - 192)) | (1L << (UNLOCK - 192)) | (1L << (MSCK - 192)) | (1L << (REPAIR - 192)) | (1L << (RECOVER - 192)) | (1L << (EXPORT - 192)) | (1L << (IMPORT - 192)) | (1L << (LOAD - 192)) | (1L << (ROLE - 192)) | (1L << (ROLES - 192)) | (1L << (COMPACTIONS - 192)) | (1L << (PRINCIPALS - 192)) | (1L << (TRANSACTIONS - 192)) | (1L << (INDEX - 192)) | (1L << (INDEXES - 192)) | (1L << (LOCKS - 192)) | (1L << (OPTION - 192)) | (1L << (ANTI - 192)) | (1L << (LOCAL - 192)) | (1L << (INPATH - 192)) | (1L << (STRING - 192)) | (1L << (BIGINT_LITERAL - 192)) | (1L << (SMALLINT_LITERAL - 192)) | (1L << (TINYINT_LITERAL - 192)) | (1L << (INTEGER_VALUE - 192)) | (1L << (DECIMAL_VALUE - 192)) | (1L << (DOUBLE_LITERAL - 192)) | (1L << (BIGDECIMAL_LITERAL - 192)) | (1L << (IDENTIFIER - 192)) | (1L << (BACKQUOTED_IDENTIFIER - 192)))) != 0)) {
				{
				setState(1643);
				expression();
				setState(1648);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__2) {
					{
					{
					setState(1644);
					match(T__2);
					setState(1645);
					expression();
					}
					}
					setState(1650);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
			}

			setState(1653);
			match(T__1);
			setState(1654);
			((LateralViewContext)_localctx).tblName = identifier();
			setState(1666);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,213,_ctx) ) {
			case 1:
				{
				setState(1656);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,211,_ctx) ) {
				case 1:
					{
					setState(1655);
					match(AS);
					}
					break;
				}
				setState(1658);
				((LateralViewContext)_localctx).identifier = identifier();
				((LateralViewContext)_localctx).colName.add(((LateralViewContext)_localctx).identifier);
				setState(1663);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,212,_ctx);
				while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						{
						{
						setState(1659);
						match(T__2);
						setState(1660);
						((LateralViewContext)_localctx).identifier = identifier();
						((LateralViewContext)_localctx).colName.add(((LateralViewContext)_localctx).identifier);
						}
						} 
					}
					setState(1665);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,212,_ctx);
				}
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SetQuantifierContext extends ParserRuleContext {
		public TerminalNode DISTINCT() { return getToken(SqlHiveParser.DISTINCT, 0); }
		public TerminalNode ALL() { return getToken(SqlHiveParser.ALL, 0); }
		public SetQuantifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_setQuantifier; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterSetQuantifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitSetQuantifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitSetQuantifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SetQuantifierContext setQuantifier() throws RecognitionException {
		SetQuantifierContext _localctx = new SetQuantifierContext(_ctx, getState());
		enterRule(_localctx, 104, RULE_setQuantifier);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1668);
			_la = _input.LA(1);
			if ( !(_la==ALL || _la==DISTINCT) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class RelationContext extends ParserRuleContext {
		public RelationPrimaryContext relationPrimary() {
			return getRuleContext(RelationPrimaryContext.class,0);
		}
		public List<JoinRelationContext> joinRelation() {
			return getRuleContexts(JoinRelationContext.class);
		}
		public JoinRelationContext joinRelation(int i) {
			return getRuleContext(JoinRelationContext.class,i);
		}
		public RelationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_relation; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterRelation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitRelation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitRelation(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RelationContext relation() throws RecognitionException {
		RelationContext _localctx = new RelationContext(_ctx, getState());
		enterRule(_localctx, 106, RULE_relation);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1670);
			relationPrimary();
			setState(1674);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,214,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(1671);
					joinRelation();
					}
					} 
				}
				setState(1676);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,214,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class JoinRelationContext extends ParserRuleContext {
		public RelationPrimaryContext right;
		public TerminalNode JOIN() { return getToken(SqlHiveParser.JOIN, 0); }
		public RelationPrimaryContext relationPrimary() {
			return getRuleContext(RelationPrimaryContext.class,0);
		}
		public JoinTypeContext joinType() {
			return getRuleContext(JoinTypeContext.class,0);
		}
		public JoinCriteriaContext joinCriteria() {
			return getRuleContext(JoinCriteriaContext.class,0);
		}
		public TerminalNode NATURAL() { return getToken(SqlHiveParser.NATURAL, 0); }
		public JoinRelationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_joinRelation; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterJoinRelation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitJoinRelation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitJoinRelation(this);
			else return visitor.visitChildren(this);
		}
	}

	public final JoinRelationContext joinRelation() throws RecognitionException {
		JoinRelationContext _localctx = new JoinRelationContext(_ctx, getState());
		enterRule(_localctx, 108, RULE_joinRelation);
		try {
			setState(1688);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case JOIN:
			case CROSS:
			case INNER:
			case LEFT:
			case RIGHT:
			case FULL:
			case ANTI:
				enterOuterAlt(_localctx, 1);
				{
				{
				setState(1677);
				joinType();
				}
				setState(1678);
				match(JOIN);
				setState(1679);
				((JoinRelationContext)_localctx).right = relationPrimary();
				setState(1681);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,215,_ctx) ) {
				case 1:
					{
					setState(1680);
					joinCriteria();
					}
					break;
				}
				}
				break;
			case NATURAL:
				enterOuterAlt(_localctx, 2);
				{
				setState(1683);
				match(NATURAL);
				setState(1684);
				joinType();
				setState(1685);
				match(JOIN);
				setState(1686);
				((JoinRelationContext)_localctx).right = relationPrimary();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class JoinTypeContext extends ParserRuleContext {
		public TerminalNode INNER() { return getToken(SqlHiveParser.INNER, 0); }
		public TerminalNode CROSS() { return getToken(SqlHiveParser.CROSS, 0); }
		public TerminalNode LEFT() { return getToken(SqlHiveParser.LEFT, 0); }
		public TerminalNode OUTER() { return getToken(SqlHiveParser.OUTER, 0); }
		public TerminalNode SEMI() { return getToken(SqlHiveParser.SEMI, 0); }
		public TerminalNode RIGHT() { return getToken(SqlHiveParser.RIGHT, 0); }
		public TerminalNode FULL() { return getToken(SqlHiveParser.FULL, 0); }
		public TerminalNode ANTI() { return getToken(SqlHiveParser.ANTI, 0); }
		public JoinTypeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_joinType; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterJoinType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitJoinType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitJoinType(this);
			else return visitor.visitChildren(this);
		}
	}

	public final JoinTypeContext joinType() throws RecognitionException {
		JoinTypeContext _localctx = new JoinTypeContext(_ctx, getState());
		enterRule(_localctx, 110, RULE_joinType);
		int _la;
		try {
			setState(1712);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,222,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1691);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==INNER) {
					{
					setState(1690);
					match(INNER);
					}
				}

				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1693);
				match(CROSS);
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(1694);
				match(LEFT);
				setState(1696);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OUTER) {
					{
					setState(1695);
					match(OUTER);
					}
				}

				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(1698);
				match(LEFT);
				setState(1699);
				match(SEMI);
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(1700);
				match(RIGHT);
				setState(1702);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OUTER) {
					{
					setState(1701);
					match(OUTER);
					}
				}

				}
				break;
			case 6:
				enterOuterAlt(_localctx, 6);
				{
				setState(1704);
				match(FULL);
				setState(1706);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OUTER) {
					{
					setState(1705);
					match(OUTER);
					}
				}

				}
				break;
			case 7:
				enterOuterAlt(_localctx, 7);
				{
				setState(1709);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LEFT) {
					{
					setState(1708);
					match(LEFT);
					}
				}

				setState(1711);
				match(ANTI);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class JoinCriteriaContext extends ParserRuleContext {
		public TerminalNode ON() { return getToken(SqlHiveParser.ON, 0); }
		public BooleanExpressionContext booleanExpression() {
			return getRuleContext(BooleanExpressionContext.class,0);
		}
		public TerminalNode USING() { return getToken(SqlHiveParser.USING, 0); }
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public JoinCriteriaContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_joinCriteria; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterJoinCriteria(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitJoinCriteria(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitJoinCriteria(this);
			else return visitor.visitChildren(this);
		}
	}

	public final JoinCriteriaContext joinCriteria() throws RecognitionException {
		JoinCriteriaContext _localctx = new JoinCriteriaContext(_ctx, getState());
		enterRule(_localctx, 112, RULE_joinCriteria);
		int _la;
		try {
			setState(1728);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case ON:
				enterOuterAlt(_localctx, 1);
				{
				setState(1714);
				match(ON);
				setState(1715);
				booleanExpression(0);
				}
				break;
			case USING:
				enterOuterAlt(_localctx, 2);
				{
				setState(1716);
				match(USING);
				setState(1717);
				match(T__0);
				setState(1718);
				identifier();
				setState(1723);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__2) {
					{
					{
					setState(1719);
					match(T__2);
					setState(1720);
					identifier();
					}
					}
					setState(1725);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(1726);
				match(T__1);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SampleContext extends ParserRuleContext {
		public TerminalNode TABLESAMPLE() { return getToken(SqlHiveParser.TABLESAMPLE, 0); }
		public SampleMethodContext sampleMethod() {
			return getRuleContext(SampleMethodContext.class,0);
		}
		public SampleContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sample; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterSample(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitSample(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitSample(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SampleContext sample() throws RecognitionException {
		SampleContext _localctx = new SampleContext(_ctx, getState());
		enterRule(_localctx, 114, RULE_sample);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1730);
			match(TABLESAMPLE);
			setState(1731);
			match(T__0);
			setState(1733);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__0) | (1L << SELECT) | (1L << FROM) | (1L << ADD) | (1L << AS) | (1L << ALL) | (1L << ANY) | (1L << DISTINCT) | (1L << WHERE) | (1L << GROUP) | (1L << BY) | (1L << GROUPING) | (1L << SETS) | (1L << CUBE) | (1L << ROLLUP) | (1L << ORDER) | (1L << HAVING) | (1L << LIMIT) | (1L << AT) | (1L << OR) | (1L << AND) | (1L << IN) | (1L << NOT) | (1L << NO) | (1L << EXISTS) | (1L << BETWEEN) | (1L << LIKE) | (1L << RLIKE) | (1L << IS) | (1L << NULL) | (1L << TRUE) | (1L << FALSE) | (1L << NULLS) | (1L << ASC) | (1L << DESC) | (1L << FOR) | (1L << INTERVAL) | (1L << CASE) | (1L << WHEN) | (1L << THEN) | (1L << ELSE) | (1L << END) | (1L << JOIN) | (1L << CROSS) | (1L << OUTER) | (1L << INNER) | (1L << LEFT) | (1L << SEMI) | (1L << RIGHT) | (1L << FULL) | (1L << NATURAL) | (1L << ON) | (1L << PIVOT) | (1L << LATERAL))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (WINDOW - 64)) | (1L << (OVER - 64)) | (1L << (PARTITION - 64)) | (1L << (RANGE - 64)) | (1L << (ROWS - 64)) | (1L << (UNBOUNDED - 64)) | (1L << (PRECEDING - 64)) | (1L << (FOLLOWING - 64)) | (1L << (CURRENT - 64)) | (1L << (FIRST - 64)) | (1L << (AFTER - 64)) | (1L << (LAST - 64)) | (1L << (ROW - 64)) | (1L << (WITH - 64)) | (1L << (VALUES - 64)) | (1L << (CREATE - 64)) | (1L << (TABLE - 64)) | (1L << (DIRECTORY - 64)) | (1L << (VIEW - 64)) | (1L << (REPLACE - 64)) | (1L << (INSERT - 64)) | (1L << (UPDATE - 64)) | (1L << (DELETE - 64)) | (1L << (INTO - 64)) | (1L << (DESCRIBE - 64)) | (1L << (EXPLAIN - 64)) | (1L << (FORMAT - 64)) | (1L << (LOGICAL - 64)) | (1L << (CODEGEN - 64)) | (1L << (COST - 64)) | (1L << (CAST - 64)) | (1L << (SHOW - 64)) | (1L << (TABLES - 64)) | (1L << (COLUMNS - 64)) | (1L << (COLUMN - 64)) | (1L << (USE - 64)) | (1L << (PARTITIONS - 64)) | (1L << (FUNCTIONS - 64)) | (1L << (DROP - 64)) | (1L << (UNION - 64)) | (1L << (EXCEPT - 64)) | (1L << (SETMINUS - 64)) | (1L << (INTERSECT - 64)) | (1L << (TO - 64)) | (1L << (TABLESAMPLE - 64)) | (1L << (STRATIFY - 64)) | (1L << (ALTER - 64)) | (1L << (RENAME - 64)) | (1L << (ARRAY - 64)) | (1L << (MAP - 64)) | (1L << (STRUCT - 64)) | (1L << (COMMENT - 64)) | (1L << (SET - 64)) | (1L << (RESET - 64)) | (1L << (DATA - 64)) | (1L << (START - 64)) | (1L << (TRANSACTION - 64)) | (1L << (COMMIT - 64)) | (1L << (ROLLBACK - 64)) | (1L << (MACRO - 64)) | (1L << (IGNORE - 64)) | (1L << (BOTH - 64)) | (1L << (LEADING - 64)) | (1L << (TRAILING - 64)))) != 0) || ((((_la - 128)) & ~0x3f) == 0 && ((1L << (_la - 128)) & ((1L << (IF - 128)) | (1L << (POSITION - 128)) | (1L << (EXTRACT - 128)) | (1L << (PLUS - 128)) | (1L << (MINUS - 128)) | (1L << (ASTERISK - 128)) | (1L << (DIV - 128)) | (1L << (TILDE - 128)) | (1L << (PERCENTLIT - 128)) | (1L << (BUCKET - 128)) | (1L << (OUT - 128)) | (1L << (OF - 128)) | (1L << (SORT - 128)) | (1L << (CLUSTER - 128)) | (1L << (DISTRIBUTE - 128)) | (1L << (OVERWRITE - 128)) | (1L << (TRANSFORM - 128)) | (1L << (REDUCE - 128)) | (1L << (SERDE - 128)) | (1L << (SERDEPROPERTIES - 128)) | (1L << (RECORDREADER - 128)) | (1L << (RECORDWRITER - 128)) | (1L << (DELIMITED - 128)) | (1L << (FIELDS - 128)) | (1L << (TERMINATED - 128)) | (1L << (COLLECTION - 128)) | (1L << (ITEMS - 128)) | (1L << (KEYS - 128)) | (1L << (ESCAPED - 128)) | (1L << (LINES - 128)) | (1L << (SEPARATED - 128)) | (1L << (FUNCTION - 128)) | (1L << (EXTENDED - 128)) | (1L << (REFRESH - 128)) | (1L << (CLEAR - 128)) | (1L << (CACHE - 128)) | (1L << (UNCACHE - 128)) | (1L << (LAZY - 128)) | (1L << (FORMATTED - 128)) | (1L << (GLOBAL - 128)) | (1L << (TEMPORARY - 128)) | (1L << (OPTIONS - 128)) | (1L << (UNSET - 128)) | (1L << (TBLPROPERTIES - 128)) | (1L << (DBPROPERTIES - 128)) | (1L << (BUCKETS - 128)) | (1L << (SKEWED - 128)) | (1L << (STORED - 128)) | (1L << (DIRECTORIES - 128)))) != 0) || ((((_la - 192)) & ~0x3f) == 0 && ((1L << (_la - 192)) & ((1L << (LOCATION - 192)) | (1L << (EXCHANGE - 192)) | (1L << (ARCHIVE - 192)) | (1L << (UNARCHIVE - 192)) | (1L << (FILEFORMAT - 192)) | (1L << (TOUCH - 192)) | (1L << (COMPACT - 192)) | (1L << (CONCATENATE - 192)) | (1L << (CHANGE - 192)) | (1L << (CASCADE - 192)) | (1L << (RESTRICT - 192)) | (1L << (CLUSTERED - 192)) | (1L << (SORTED - 192)) | (1L << (PURGE - 192)) | (1L << (INPUTFORMAT - 192)) | (1L << (OUTPUTFORMAT - 192)) | (1L << (DATABASE - 192)) | (1L << (DATABASES - 192)) | (1L << (DFS - 192)) | (1L << (TRUNCATE - 192)) | (1L << (ANALYZE - 192)) | (1L << (COMPUTE - 192)) | (1L << (LIST - 192)) | (1L << (STATISTICS - 192)) | (1L << (PARTITIONED - 192)) | (1L << (EXTERNAL - 192)) | (1L << (DEFINED - 192)) | (1L << (REVOKE - 192)) | (1L << (GRANT - 192)) | (1L << (LOCK - 192)) | (1L << (UNLOCK - 192)) | (1L << (MSCK - 192)) | (1L << (REPAIR - 192)) | (1L << (RECOVER - 192)) | (1L << (EXPORT - 192)) | (1L << (IMPORT - 192)) | (1L << (LOAD - 192)) | (1L << (ROLE - 192)) | (1L << (ROLES - 192)) | (1L << (COMPACTIONS - 192)) | (1L << (PRINCIPALS - 192)) | (1L << (TRANSACTIONS - 192)) | (1L << (INDEX - 192)) | (1L << (INDEXES - 192)) | (1L << (LOCKS - 192)) | (1L << (OPTION - 192)) | (1L << (ANTI - 192)) | (1L << (LOCAL - 192)) | (1L << (INPATH - 192)) | (1L << (STRING - 192)) | (1L << (BIGINT_LITERAL - 192)) | (1L << (SMALLINT_LITERAL - 192)) | (1L << (TINYINT_LITERAL - 192)) | (1L << (INTEGER_VALUE - 192)) | (1L << (DECIMAL_VALUE - 192)) | (1L << (DOUBLE_LITERAL - 192)) | (1L << (BIGDECIMAL_LITERAL - 192)) | (1L << (IDENTIFIER - 192)) | (1L << (BACKQUOTED_IDENTIFIER - 192)))) != 0)) {
				{
				setState(1732);
				sampleMethod();
				}
			}

			setState(1735);
			match(T__1);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SampleMethodContext extends ParserRuleContext {
		public SampleMethodContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sampleMethod; }
	 
		public SampleMethodContext() { }
		public void copyFrom(SampleMethodContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class SampleByRowsContext extends SampleMethodContext {
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode ROWS() { return getToken(SqlHiveParser.ROWS, 0); }
		public SampleByRowsContext(SampleMethodContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterSampleByRows(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitSampleByRows(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitSampleByRows(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class SampleByPercentileContext extends SampleMethodContext {
		public Token negativeSign;
		public Token percentage;
		public TerminalNode PERCENTLIT() { return getToken(SqlHiveParser.PERCENTLIT, 0); }
		public TerminalNode INTEGER_VALUE() { return getToken(SqlHiveParser.INTEGER_VALUE, 0); }
		public TerminalNode DECIMAL_VALUE() { return getToken(SqlHiveParser.DECIMAL_VALUE, 0); }
		public TerminalNode MINUS() { return getToken(SqlHiveParser.MINUS, 0); }
		public SampleByPercentileContext(SampleMethodContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterSampleByPercentile(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitSampleByPercentile(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitSampleByPercentile(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class SampleByBucketContext extends SampleMethodContext {
		public Token sampleType;
		public Token numerator;
		public Token denominator;
		public TerminalNode OUT() { return getToken(SqlHiveParser.OUT, 0); }
		public TerminalNode OF() { return getToken(SqlHiveParser.OF, 0); }
		public TerminalNode BUCKET() { return getToken(SqlHiveParser.BUCKET, 0); }
		public List<TerminalNode> INTEGER_VALUE() { return getTokens(SqlHiveParser.INTEGER_VALUE); }
		public TerminalNode INTEGER_VALUE(int i) {
			return getToken(SqlHiveParser.INTEGER_VALUE, i);
		}
		public TerminalNode ON() { return getToken(SqlHiveParser.ON, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public SampleByBucketContext(SampleMethodContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterSampleByBucket(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitSampleByBucket(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitSampleByBucket(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class SampleByBytesContext extends SampleMethodContext {
		public ExpressionContext bytes;
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public SampleByBytesContext(SampleMethodContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterSampleByBytes(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitSampleByBytes(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitSampleByBytes(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SampleMethodContext sampleMethod() throws RecognitionException {
		SampleMethodContext _localctx = new SampleMethodContext(_ctx, getState());
		enterRule(_localctx, 116, RULE_sampleMethod);
		int _la;
		try {
			setState(1761);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,229,_ctx) ) {
			case 1:
				_localctx = new SampleByPercentileContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1738);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(1737);
					((SampleByPercentileContext)_localctx).negativeSign = match(MINUS);
					}
				}

				setState(1740);
				((SampleByPercentileContext)_localctx).percentage = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==INTEGER_VALUE || _la==DECIMAL_VALUE) ) {
					((SampleByPercentileContext)_localctx).percentage = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(1741);
				match(PERCENTLIT);
				}
				break;
			case 2:
				_localctx = new SampleByRowsContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1742);
				expression();
				setState(1743);
				match(ROWS);
				}
				break;
			case 3:
				_localctx = new SampleByBucketContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(1745);
				((SampleByBucketContext)_localctx).sampleType = match(BUCKET);
				setState(1746);
				((SampleByBucketContext)_localctx).numerator = match(INTEGER_VALUE);
				setState(1747);
				match(OUT);
				setState(1748);
				match(OF);
				setState(1749);
				((SampleByBucketContext)_localctx).denominator = match(INTEGER_VALUE);
				setState(1758);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ON) {
					{
					setState(1750);
					match(ON);
					setState(1756);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,227,_ctx) ) {
					case 1:
						{
						setState(1751);
						identifier();
						}
						break;
					case 2:
						{
						setState(1752);
						qualifiedName();
						setState(1753);
						match(T__0);
						setState(1754);
						match(T__1);
						}
						break;
					}
					}
				}

				}
				break;
			case 4:
				_localctx = new SampleByBytesContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(1760);
				((SampleByBytesContext)_localctx).bytes = expression();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class IdentifierListContext extends ParserRuleContext {
		public IdentifierSeqContext identifierSeq() {
			return getRuleContext(IdentifierSeqContext.class,0);
		}
		public IdentifierListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_identifierList; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterIdentifierList(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitIdentifierList(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitIdentifierList(this);
			else return visitor.visitChildren(this);
		}
	}

	public final IdentifierListContext identifierList() throws RecognitionException {
		IdentifierListContext _localctx = new IdentifierListContext(_ctx, getState());
		enterRule(_localctx, 118, RULE_identifierList);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1763);
			match(T__0);
			setState(1764);
			identifierSeq();
			setState(1765);
			match(T__1);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class IdentifierSeqContext extends ParserRuleContext {
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public IdentifierSeqContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_identifierSeq; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterIdentifierSeq(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitIdentifierSeq(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitIdentifierSeq(this);
			else return visitor.visitChildren(this);
		}
	}

	public final IdentifierSeqContext identifierSeq() throws RecognitionException {
		IdentifierSeqContext _localctx = new IdentifierSeqContext(_ctx, getState());
		enterRule(_localctx, 120, RULE_identifierSeq);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1767);
			identifier();
			setState(1772);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,230,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(1768);
					match(T__2);
					setState(1769);
					identifier();
					}
					} 
				}
				setState(1774);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,230,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OrderedIdentifierListContext extends ParserRuleContext {
		public List<OrderedIdentifierContext> orderedIdentifier() {
			return getRuleContexts(OrderedIdentifierContext.class);
		}
		public OrderedIdentifierContext orderedIdentifier(int i) {
			return getRuleContext(OrderedIdentifierContext.class,i);
		}
		public OrderedIdentifierListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_orderedIdentifierList; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterOrderedIdentifierList(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitOrderedIdentifierList(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitOrderedIdentifierList(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OrderedIdentifierListContext orderedIdentifierList() throws RecognitionException {
		OrderedIdentifierListContext _localctx = new OrderedIdentifierListContext(_ctx, getState());
		enterRule(_localctx, 122, RULE_orderedIdentifierList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1775);
			match(T__0);
			setState(1776);
			orderedIdentifier();
			setState(1781);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__2) {
				{
				{
				setState(1777);
				match(T__2);
				setState(1778);
				orderedIdentifier();
				}
				}
				setState(1783);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(1784);
			match(T__1);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OrderedIdentifierContext extends ParserRuleContext {
		public Token ordering;
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode ASC() { return getToken(SqlHiveParser.ASC, 0); }
		public TerminalNode DESC() { return getToken(SqlHiveParser.DESC, 0); }
		public OrderedIdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_orderedIdentifier; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterOrderedIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitOrderedIdentifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitOrderedIdentifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OrderedIdentifierContext orderedIdentifier() throws RecognitionException {
		OrderedIdentifierContext _localctx = new OrderedIdentifierContext(_ctx, getState());
		enterRule(_localctx, 124, RULE_orderedIdentifier);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1786);
			identifier();
			setState(1788);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ASC || _la==DESC) {
				{
				setState(1787);
				((OrderedIdentifierContext)_localctx).ordering = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==ASC || _la==DESC) ) {
					((OrderedIdentifierContext)_localctx).ordering = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class IdentifierCommentListContext extends ParserRuleContext {
		public List<IdentifierCommentContext> identifierComment() {
			return getRuleContexts(IdentifierCommentContext.class);
		}
		public IdentifierCommentContext identifierComment(int i) {
			return getRuleContext(IdentifierCommentContext.class,i);
		}
		public IdentifierCommentListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_identifierCommentList; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterIdentifierCommentList(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitIdentifierCommentList(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitIdentifierCommentList(this);
			else return visitor.visitChildren(this);
		}
	}

	public final IdentifierCommentListContext identifierCommentList() throws RecognitionException {
		IdentifierCommentListContext _localctx = new IdentifierCommentListContext(_ctx, getState());
		enterRule(_localctx, 126, RULE_identifierCommentList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1790);
			match(T__0);
			setState(1791);
			identifierComment();
			setState(1796);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__2) {
				{
				{
				setState(1792);
				match(T__2);
				setState(1793);
				identifierComment();
				}
				}
				setState(1798);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(1799);
			match(T__1);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class IdentifierCommentContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode COMMENT() { return getToken(SqlHiveParser.COMMENT, 0); }
		public TerminalNode STRING() { return getToken(SqlHiveParser.STRING, 0); }
		public IdentifierCommentContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_identifierComment; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterIdentifierComment(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitIdentifierComment(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitIdentifierComment(this);
			else return visitor.visitChildren(this);
		}
	}

	public final IdentifierCommentContext identifierComment() throws RecognitionException {
		IdentifierCommentContext _localctx = new IdentifierCommentContext(_ctx, getState());
		enterRule(_localctx, 128, RULE_identifierComment);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1801);
			identifier();
			setState(1804);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==COMMENT) {
				{
				setState(1802);
				match(COMMENT);
				setState(1803);
				match(STRING);
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class RelationPrimaryContext extends ParserRuleContext {
		public RelationPrimaryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_relationPrimary; }
	 
		public RelationPrimaryContext() { }
		public void copyFrom(RelationPrimaryContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class TableValuedFunctionContext extends RelationPrimaryContext {
		public FunctionTableContext functionTable() {
			return getRuleContext(FunctionTableContext.class,0);
		}
		public TableValuedFunctionContext(RelationPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterTableValuedFunction(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitTableValuedFunction(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitTableValuedFunction(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class InlineTableDefault2Context extends RelationPrimaryContext {
		public InlineTableContext inlineTable() {
			return getRuleContext(InlineTableContext.class,0);
		}
		public InlineTableDefault2Context(RelationPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterInlineTableDefault2(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitInlineTableDefault2(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitInlineTableDefault2(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class AliasedRelationContext extends RelationPrimaryContext {
		public RelationContext relation() {
			return getRuleContext(RelationContext.class,0);
		}
		public TableAliasContext tableAlias() {
			return getRuleContext(TableAliasContext.class,0);
		}
		public SampleContext sample() {
			return getRuleContext(SampleContext.class,0);
		}
		public AliasedRelationContext(RelationPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterAliasedRelation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitAliasedRelation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitAliasedRelation(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class AliasedQueryContext extends RelationPrimaryContext {
		public QueryNoWithContext queryNoWith() {
			return getRuleContext(QueryNoWithContext.class,0);
		}
		public TableAliasContext tableAlias() {
			return getRuleContext(TableAliasContext.class,0);
		}
		public SampleContext sample() {
			return getRuleContext(SampleContext.class,0);
		}
		public AliasedQueryContext(RelationPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterAliasedQuery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitAliasedQuery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitAliasedQuery(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class TableNameContext extends RelationPrimaryContext {
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TableAliasContext tableAlias() {
			return getRuleContext(TableAliasContext.class,0);
		}
		public SampleContext sample() {
			return getRuleContext(SampleContext.class,0);
		}
		public TableNameContext(RelationPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterTableName(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitTableName(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitTableName(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RelationPrimaryContext relationPrimary() throws RecognitionException {
		RelationPrimaryContext _localctx = new RelationPrimaryContext(_ctx, getState());
		enterRule(_localctx, 130, RULE_relationPrimary);
		try {
			setState(1830);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,238,_ctx) ) {
			case 1:
				_localctx = new TableNameContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1806);
				tableIdentifier();
				setState(1808);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,235,_ctx) ) {
				case 1:
					{
					setState(1807);
					sample();
					}
					break;
				}
				setState(1810);
				tableAlias();
				}
				break;
			case 2:
				_localctx = new AliasedQueryContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1812);
				match(T__0);
				setState(1813);
				queryNoWith();
				setState(1814);
				match(T__1);
				setState(1816);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,236,_ctx) ) {
				case 1:
					{
					setState(1815);
					sample();
					}
					break;
				}
				setState(1818);
				tableAlias();
				}
				break;
			case 3:
				_localctx = new AliasedRelationContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(1820);
				match(T__0);
				setState(1821);
				relation();
				setState(1822);
				match(T__1);
				setState(1824);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,237,_ctx) ) {
				case 1:
					{
					setState(1823);
					sample();
					}
					break;
				}
				setState(1826);
				tableAlias();
				}
				break;
			case 4:
				_localctx = new InlineTableDefault2Context(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(1828);
				inlineTable();
				}
				break;
			case 5:
				_localctx = new TableValuedFunctionContext(_localctx);
				enterOuterAlt(_localctx, 5);
				{
				setState(1829);
				functionTable();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class InlineTableContext extends ParserRuleContext {
		public TerminalNode VALUES() { return getToken(SqlHiveParser.VALUES, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TableAliasContext tableAlias() {
			return getRuleContext(TableAliasContext.class,0);
		}
		public InlineTableContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_inlineTable; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterInlineTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitInlineTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitInlineTable(this);
			else return visitor.visitChildren(this);
		}
	}

	public final InlineTableContext inlineTable() throws RecognitionException {
		InlineTableContext _localctx = new InlineTableContext(_ctx, getState());
		enterRule(_localctx, 132, RULE_inlineTable);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1832);
			match(VALUES);
			setState(1833);
			expression();
			setState(1838);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,239,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(1834);
					match(T__2);
					setState(1835);
					expression();
					}
					} 
				}
				setState(1840);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,239,_ctx);
			}
			setState(1841);
			tableAlias();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class FunctionTableContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TableAliasContext tableAlias() {
			return getRuleContext(TableAliasContext.class,0);
		}
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public FunctionTableContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_functionTable; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterFunctionTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitFunctionTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitFunctionTable(this);
			else return visitor.visitChildren(this);
		}
	}

	public final FunctionTableContext functionTable() throws RecognitionException {
		FunctionTableContext _localctx = new FunctionTableContext(_ctx, getState());
		enterRule(_localctx, 134, RULE_functionTable);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1843);
			identifier();
			setState(1844);
			match(T__0);
			setState(1853);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__0) | (1L << SELECT) | (1L << FROM) | (1L << ADD) | (1L << AS) | (1L << ALL) | (1L << ANY) | (1L << DISTINCT) | (1L << WHERE) | (1L << GROUP) | (1L << BY) | (1L << GROUPING) | (1L << SETS) | (1L << CUBE) | (1L << ROLLUP) | (1L << ORDER) | (1L << HAVING) | (1L << LIMIT) | (1L << AT) | (1L << OR) | (1L << AND) | (1L << IN) | (1L << NOT) | (1L << NO) | (1L << EXISTS) | (1L << BETWEEN) | (1L << LIKE) | (1L << RLIKE) | (1L << IS) | (1L << NULL) | (1L << TRUE) | (1L << FALSE) | (1L << NULLS) | (1L << ASC) | (1L << DESC) | (1L << FOR) | (1L << INTERVAL) | (1L << CASE) | (1L << WHEN) | (1L << THEN) | (1L << ELSE) | (1L << END) | (1L << JOIN) | (1L << CROSS) | (1L << OUTER) | (1L << INNER) | (1L << LEFT) | (1L << SEMI) | (1L << RIGHT) | (1L << FULL) | (1L << NATURAL) | (1L << ON) | (1L << PIVOT) | (1L << LATERAL))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (WINDOW - 64)) | (1L << (OVER - 64)) | (1L << (PARTITION - 64)) | (1L << (RANGE - 64)) | (1L << (ROWS - 64)) | (1L << (UNBOUNDED - 64)) | (1L << (PRECEDING - 64)) | (1L << (FOLLOWING - 64)) | (1L << (CURRENT - 64)) | (1L << (FIRST - 64)) | (1L << (AFTER - 64)) | (1L << (LAST - 64)) | (1L << (ROW - 64)) | (1L << (WITH - 64)) | (1L << (VALUES - 64)) | (1L << (CREATE - 64)) | (1L << (TABLE - 64)) | (1L << (DIRECTORY - 64)) | (1L << (VIEW - 64)) | (1L << (REPLACE - 64)) | (1L << (INSERT - 64)) | (1L << (UPDATE - 64)) | (1L << (DELETE - 64)) | (1L << (INTO - 64)) | (1L << (DESCRIBE - 64)) | (1L << (EXPLAIN - 64)) | (1L << (FORMAT - 64)) | (1L << (LOGICAL - 64)) | (1L << (CODEGEN - 64)) | (1L << (COST - 64)) | (1L << (CAST - 64)) | (1L << (SHOW - 64)) | (1L << (TABLES - 64)) | (1L << (COLUMNS - 64)) | (1L << (COLUMN - 64)) | (1L << (USE - 64)) | (1L << (PARTITIONS - 64)) | (1L << (FUNCTIONS - 64)) | (1L << (DROP - 64)) | (1L << (UNION - 64)) | (1L << (EXCEPT - 64)) | (1L << (SETMINUS - 64)) | (1L << (INTERSECT - 64)) | (1L << (TO - 64)) | (1L << (TABLESAMPLE - 64)) | (1L << (STRATIFY - 64)) | (1L << (ALTER - 64)) | (1L << (RENAME - 64)) | (1L << (ARRAY - 64)) | (1L << (MAP - 64)) | (1L << (STRUCT - 64)) | (1L << (COMMENT - 64)) | (1L << (SET - 64)) | (1L << (RESET - 64)) | (1L << (DATA - 64)) | (1L << (START - 64)) | (1L << (TRANSACTION - 64)) | (1L << (COMMIT - 64)) | (1L << (ROLLBACK - 64)) | (1L << (MACRO - 64)) | (1L << (IGNORE - 64)) | (1L << (BOTH - 64)) | (1L << (LEADING - 64)) | (1L << (TRAILING - 64)))) != 0) || ((((_la - 128)) & ~0x3f) == 0 && ((1L << (_la - 128)) & ((1L << (IF - 128)) | (1L << (POSITION - 128)) | (1L << (EXTRACT - 128)) | (1L << (PLUS - 128)) | (1L << (MINUS - 128)) | (1L << (ASTERISK - 128)) | (1L << (DIV - 128)) | (1L << (TILDE - 128)) | (1L << (PERCENTLIT - 128)) | (1L << (BUCKET - 128)) | (1L << (OUT - 128)) | (1L << (OF - 128)) | (1L << (SORT - 128)) | (1L << (CLUSTER - 128)) | (1L << (DISTRIBUTE - 128)) | (1L << (OVERWRITE - 128)) | (1L << (TRANSFORM - 128)) | (1L << (REDUCE - 128)) | (1L << (SERDE - 128)) | (1L << (SERDEPROPERTIES - 128)) | (1L << (RECORDREADER - 128)) | (1L << (RECORDWRITER - 128)) | (1L << (DELIMITED - 128)) | (1L << (FIELDS - 128)) | (1L << (TERMINATED - 128)) | (1L << (COLLECTION - 128)) | (1L << (ITEMS - 128)) | (1L << (KEYS - 128)) | (1L << (ESCAPED - 128)) | (1L << (LINES - 128)) | (1L << (SEPARATED - 128)) | (1L << (FUNCTION - 128)) | (1L << (EXTENDED - 128)) | (1L << (REFRESH - 128)) | (1L << (CLEAR - 128)) | (1L << (CACHE - 128)) | (1L << (UNCACHE - 128)) | (1L << (LAZY - 128)) | (1L << (FORMATTED - 128)) | (1L << (GLOBAL - 128)) | (1L << (TEMPORARY - 128)) | (1L << (OPTIONS - 128)) | (1L << (UNSET - 128)) | (1L << (TBLPROPERTIES - 128)) | (1L << (DBPROPERTIES - 128)) | (1L << (BUCKETS - 128)) | (1L << (SKEWED - 128)) | (1L << (STORED - 128)) | (1L << (DIRECTORIES - 128)))) != 0) || ((((_la - 192)) & ~0x3f) == 0 && ((1L << (_la - 192)) & ((1L << (LOCATION - 192)) | (1L << (EXCHANGE - 192)) | (1L << (ARCHIVE - 192)) | (1L << (UNARCHIVE - 192)) | (1L << (FILEFORMAT - 192)) | (1L << (TOUCH - 192)) | (1L << (COMPACT - 192)) | (1L << (CONCATENATE - 192)) | (1L << (CHANGE - 192)) | (1L << (CASCADE - 192)) | (1L << (RESTRICT - 192)) | (1L << (CLUSTERED - 192)) | (1L << (SORTED - 192)) | (1L << (PURGE - 192)) | (1L << (INPUTFORMAT - 192)) | (1L << (OUTPUTFORMAT - 192)) | (1L << (DATABASE - 192)) | (1L << (DATABASES - 192)) | (1L << (DFS - 192)) | (1L << (TRUNCATE - 192)) | (1L << (ANALYZE - 192)) | (1L << (COMPUTE - 192)) | (1L << (LIST - 192)) | (1L << (STATISTICS - 192)) | (1L << (PARTITIONED - 192)) | (1L << (EXTERNAL - 192)) | (1L << (DEFINED - 192)) | (1L << (REVOKE - 192)) | (1L << (GRANT - 192)) | (1L << (LOCK - 192)) | (1L << (UNLOCK - 192)) | (1L << (MSCK - 192)) | (1L << (REPAIR - 192)) | (1L << (RECOVER - 192)) | (1L << (EXPORT - 192)) | (1L << (IMPORT - 192)) | (1L << (LOAD - 192)) | (1L << (ROLE - 192)) | (1L << (ROLES - 192)) | (1L << (COMPACTIONS - 192)) | (1L << (PRINCIPALS - 192)) | (1L << (TRANSACTIONS - 192)) | (1L << (INDEX - 192)) | (1L << (INDEXES - 192)) | (1L << (LOCKS - 192)) | (1L << (OPTION - 192)) | (1L << (ANTI - 192)) | (1L << (LOCAL - 192)) | (1L << (INPATH - 192)) | (1L << (STRING - 192)) | (1L << (BIGINT_LITERAL - 192)) | (1L << (SMALLINT_LITERAL - 192)) | (1L << (TINYINT_LITERAL - 192)) | (1L << (INTEGER_VALUE - 192)) | (1L << (DECIMAL_VALUE - 192)) | (1L << (DOUBLE_LITERAL - 192)) | (1L << (BIGDECIMAL_LITERAL - 192)) | (1L << (IDENTIFIER - 192)) | (1L << (BACKQUOTED_IDENTIFIER - 192)))) != 0)) {
				{
				setState(1845);
				expression();
				setState(1850);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__2) {
					{
					{
					setState(1846);
					match(T__2);
					setState(1847);
					expression();
					}
					}
					setState(1852);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
			}

			setState(1855);
			match(T__1);
			setState(1856);
			tableAlias();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class TableAliasContext extends ParserRuleContext {
		public StrictIdentifierContext strictIdentifier() {
			return getRuleContext(StrictIdentifierContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlHiveParser.AS, 0); }
		public IdentifierListContext identifierList() {
			return getRuleContext(IdentifierListContext.class,0);
		}
		public TableAliasContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_tableAlias; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterTableAlias(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitTableAlias(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitTableAlias(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TableAliasContext tableAlias() throws RecognitionException {
		TableAliasContext _localctx = new TableAliasContext(_ctx, getState());
		enterRule(_localctx, 136, RULE_tableAlias);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1865);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,244,_ctx) ) {
			case 1:
				{
				setState(1859);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,242,_ctx) ) {
				case 1:
					{
					setState(1858);
					match(AS);
					}
					break;
				}
				setState(1861);
				strictIdentifier();
				setState(1863);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,243,_ctx) ) {
				case 1:
					{
					setState(1862);
					identifierList();
					}
					break;
				}
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class RowFormatContext extends ParserRuleContext {
		public RowFormatContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_rowFormat; }
	 
		public RowFormatContext() { }
		public void copyFrom(RowFormatContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class RowFormatSerdeContext extends RowFormatContext {
		public Token name;
		public TablePropertyListContext props;
		public TerminalNode ROW() { return getToken(SqlHiveParser.ROW, 0); }
		public TerminalNode FORMAT() { return getToken(SqlHiveParser.FORMAT, 0); }
		public TerminalNode SERDE() { return getToken(SqlHiveParser.SERDE, 0); }
		public TerminalNode STRING() { return getToken(SqlHiveParser.STRING, 0); }
		public TerminalNode WITH() { return getToken(SqlHiveParser.WITH, 0); }
		public TerminalNode SERDEPROPERTIES() { return getToken(SqlHiveParser.SERDEPROPERTIES, 0); }
		public TablePropertyListContext tablePropertyList() {
			return getRuleContext(TablePropertyListContext.class,0);
		}
		public RowFormatSerdeContext(RowFormatContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterRowFormatSerde(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitRowFormatSerde(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitRowFormatSerde(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class RowFormatDelimitedContext extends RowFormatContext {
		public Token fieldsTerminatedBy;
		public Token escapedBy;
		public Token collectionItemsTerminatedBy;
		public Token keysTerminatedBy;
		public Token linesSeparatedBy;
		public Token nullDefinedAs;
		public TerminalNode ROW() { return getToken(SqlHiveParser.ROW, 0); }
		public TerminalNode FORMAT() { return getToken(SqlHiveParser.FORMAT, 0); }
		public TerminalNode DELIMITED() { return getToken(SqlHiveParser.DELIMITED, 0); }
		public TerminalNode FIELDS() { return getToken(SqlHiveParser.FIELDS, 0); }
		public List<TerminalNode> TERMINATED() { return getTokens(SqlHiveParser.TERMINATED); }
		public TerminalNode TERMINATED(int i) {
			return getToken(SqlHiveParser.TERMINATED, i);
		}
		public List<TerminalNode> BY() { return getTokens(SqlHiveParser.BY); }
		public TerminalNode BY(int i) {
			return getToken(SqlHiveParser.BY, i);
		}
		public TerminalNode COLLECTION() { return getToken(SqlHiveParser.COLLECTION, 0); }
		public TerminalNode ITEMS() { return getToken(SqlHiveParser.ITEMS, 0); }
		public TerminalNode MAP() { return getToken(SqlHiveParser.MAP, 0); }
		public TerminalNode KEYS() { return getToken(SqlHiveParser.KEYS, 0); }
		public TerminalNode LINES() { return getToken(SqlHiveParser.LINES, 0); }
		public TerminalNode NULL() { return getToken(SqlHiveParser.NULL, 0); }
		public TerminalNode DEFINED() { return getToken(SqlHiveParser.DEFINED, 0); }
		public TerminalNode AS() { return getToken(SqlHiveParser.AS, 0); }
		public List<TerminalNode> STRING() { return getTokens(SqlHiveParser.STRING); }
		public TerminalNode STRING(int i) {
			return getToken(SqlHiveParser.STRING, i);
		}
		public TerminalNode ESCAPED() { return getToken(SqlHiveParser.ESCAPED, 0); }
		public RowFormatDelimitedContext(RowFormatContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterRowFormatDelimited(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitRowFormatDelimited(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitRowFormatDelimited(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RowFormatContext rowFormat() throws RecognitionException {
		RowFormatContext _localctx = new RowFormatContext(_ctx, getState());
		enterRule(_localctx, 138, RULE_rowFormat);
		try {
			setState(1916);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,252,_ctx) ) {
			case 1:
				_localctx = new RowFormatSerdeContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1867);
				match(ROW);
				setState(1868);
				match(FORMAT);
				setState(1869);
				match(SERDE);
				setState(1870);
				((RowFormatSerdeContext)_localctx).name = match(STRING);
				setState(1874);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,245,_ctx) ) {
				case 1:
					{
					setState(1871);
					match(WITH);
					setState(1872);
					match(SERDEPROPERTIES);
					setState(1873);
					((RowFormatSerdeContext)_localctx).props = tablePropertyList();
					}
					break;
				}
				}
				break;
			case 2:
				_localctx = new RowFormatDelimitedContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1876);
				match(ROW);
				setState(1877);
				match(FORMAT);
				setState(1878);
				match(DELIMITED);
				setState(1888);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,247,_ctx) ) {
				case 1:
					{
					setState(1879);
					match(FIELDS);
					setState(1880);
					match(TERMINATED);
					setState(1881);
					match(BY);
					setState(1882);
					((RowFormatDelimitedContext)_localctx).fieldsTerminatedBy = match(STRING);
					setState(1886);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,246,_ctx) ) {
					case 1:
						{
						setState(1883);
						match(ESCAPED);
						setState(1884);
						match(BY);
						setState(1885);
						((RowFormatDelimitedContext)_localctx).escapedBy = match(STRING);
						}
						break;
					}
					}
					break;
				}
				setState(1895);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,248,_ctx) ) {
				case 1:
					{
					setState(1890);
					match(COLLECTION);
					setState(1891);
					match(ITEMS);
					setState(1892);
					match(TERMINATED);
					setState(1893);
					match(BY);
					setState(1894);
					((RowFormatDelimitedContext)_localctx).collectionItemsTerminatedBy = match(STRING);
					}
					break;
				}
				setState(1902);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,249,_ctx) ) {
				case 1:
					{
					setState(1897);
					match(MAP);
					setState(1898);
					match(KEYS);
					setState(1899);
					match(TERMINATED);
					setState(1900);
					match(BY);
					setState(1901);
					((RowFormatDelimitedContext)_localctx).keysTerminatedBy = match(STRING);
					}
					break;
				}
				setState(1908);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,250,_ctx) ) {
				case 1:
					{
					setState(1904);
					match(LINES);
					setState(1905);
					match(TERMINATED);
					setState(1906);
					match(BY);
					setState(1907);
					((RowFormatDelimitedContext)_localctx).linesSeparatedBy = match(STRING);
					}
					break;
				}
				setState(1914);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,251,_ctx) ) {
				case 1:
					{
					setState(1910);
					match(NULL);
					setState(1911);
					match(DEFINED);
					setState(1912);
					match(AS);
					setState(1913);
					((RowFormatDelimitedContext)_localctx).nullDefinedAs = match(STRING);
					}
					break;
				}
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class TableIdentifierContext extends ParserRuleContext {
		public IdentifierContext db;
		public IdentifierContext table;
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public TableIdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_tableIdentifier; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterTableIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitTableIdentifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitTableIdentifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TableIdentifierContext tableIdentifier() throws RecognitionException {
		TableIdentifierContext _localctx = new TableIdentifierContext(_ctx, getState());
		enterRule(_localctx, 140, RULE_tableIdentifier);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1921);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,253,_ctx) ) {
			case 1:
				{
				setState(1918);
				((TableIdentifierContext)_localctx).db = identifier();
				setState(1919);
				match(T__3);
				}
				break;
			}
			setState(1923);
			((TableIdentifierContext)_localctx).table = identifier();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class FunctionIdentifierContext extends ParserRuleContext {
		public IdentifierContext db;
		public IdentifierContext function;
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public FunctionIdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_functionIdentifier; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterFunctionIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitFunctionIdentifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitFunctionIdentifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final FunctionIdentifierContext functionIdentifier() throws RecognitionException {
		FunctionIdentifierContext _localctx = new FunctionIdentifierContext(_ctx, getState());
		enterRule(_localctx, 142, RULE_functionIdentifier);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1928);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,254,_ctx) ) {
			case 1:
				{
				setState(1925);
				((FunctionIdentifierContext)_localctx).db = identifier();
				setState(1926);
				match(T__3);
				}
				break;
			}
			setState(1930);
			((FunctionIdentifierContext)_localctx).function = identifier();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class NamedExpressionContext extends ParserRuleContext {
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public IdentifierListContext identifierList() {
			return getRuleContext(IdentifierListContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlHiveParser.AS, 0); }
		public NamedExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_namedExpression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterNamedExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitNamedExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitNamedExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NamedExpressionContext namedExpression() throws RecognitionException {
		NamedExpressionContext _localctx = new NamedExpressionContext(_ctx, getState());
		enterRule(_localctx, 144, RULE_namedExpression);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1932);
			expression();
			setState(1940);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,257,_ctx) ) {
			case 1:
				{
				setState(1934);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,255,_ctx) ) {
				case 1:
					{
					setState(1933);
					match(AS);
					}
					break;
				}
				setState(1938);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case SELECT:
				case FROM:
				case ADD:
				case AS:
				case ALL:
				case ANY:
				case DISTINCT:
				case WHERE:
				case GROUP:
				case BY:
				case GROUPING:
				case SETS:
				case CUBE:
				case ROLLUP:
				case ORDER:
				case HAVING:
				case LIMIT:
				case AT:
				case OR:
				case AND:
				case IN:
				case NOT:
				case NO:
				case EXISTS:
				case BETWEEN:
				case LIKE:
				case RLIKE:
				case IS:
				case NULL:
				case TRUE:
				case FALSE:
				case NULLS:
				case ASC:
				case DESC:
				case FOR:
				case INTERVAL:
				case CASE:
				case WHEN:
				case THEN:
				case ELSE:
				case END:
				case JOIN:
				case CROSS:
				case OUTER:
				case INNER:
				case LEFT:
				case SEMI:
				case RIGHT:
				case FULL:
				case NATURAL:
				case ON:
				case PIVOT:
				case LATERAL:
				case WINDOW:
				case OVER:
				case PARTITION:
				case RANGE:
				case ROWS:
				case UNBOUNDED:
				case PRECEDING:
				case FOLLOWING:
				case CURRENT:
				case FIRST:
				case AFTER:
				case LAST:
				case ROW:
				case WITH:
				case VALUES:
				case CREATE:
				case TABLE:
				case DIRECTORY:
				case VIEW:
				case REPLACE:
				case INSERT:
				case UPDATE:
				case DELETE:
				case INTO:
				case DESCRIBE:
				case EXPLAIN:
				case FORMAT:
				case LOGICAL:
				case CODEGEN:
				case COST:
				case CAST:
				case SHOW:
				case TABLES:
				case COLUMNS:
				case COLUMN:
				case USE:
				case PARTITIONS:
				case FUNCTIONS:
				case DROP:
				case UNION:
				case EXCEPT:
				case SETMINUS:
				case INTERSECT:
				case TO:
				case TABLESAMPLE:
				case STRATIFY:
				case ALTER:
				case RENAME:
				case ARRAY:
				case MAP:
				case STRUCT:
				case COMMENT:
				case SET:
				case RESET:
				case DATA:
				case START:
				case TRANSACTION:
				case COMMIT:
				case ROLLBACK:
				case MACRO:
				case IGNORE:
				case BOTH:
				case LEADING:
				case TRAILING:
				case IF:
				case POSITION:
				case EXTRACT:
				case DIV:
				case PERCENTLIT:
				case BUCKET:
				case OUT:
				case OF:
				case SORT:
				case CLUSTER:
				case DISTRIBUTE:
				case OVERWRITE:
				case TRANSFORM:
				case REDUCE:
				case SERDE:
				case SERDEPROPERTIES:
				case RECORDREADER:
				case RECORDWRITER:
				case DELIMITED:
				case FIELDS:
				case TERMINATED:
				case COLLECTION:
				case ITEMS:
				case KEYS:
				case ESCAPED:
				case LINES:
				case SEPARATED:
				case FUNCTION:
				case EXTENDED:
				case REFRESH:
				case CLEAR:
				case CACHE:
				case UNCACHE:
				case LAZY:
				case FORMATTED:
				case GLOBAL:
				case TEMPORARY:
				case OPTIONS:
				case UNSET:
				case TBLPROPERTIES:
				case DBPROPERTIES:
				case BUCKETS:
				case SKEWED:
				case STORED:
				case DIRECTORIES:
				case LOCATION:
				case EXCHANGE:
				case ARCHIVE:
				case UNARCHIVE:
				case FILEFORMAT:
				case TOUCH:
				case COMPACT:
				case CONCATENATE:
				case CHANGE:
				case CASCADE:
				case RESTRICT:
				case CLUSTERED:
				case SORTED:
				case PURGE:
				case INPUTFORMAT:
				case OUTPUTFORMAT:
				case DATABASE:
				case DATABASES:
				case DFS:
				case TRUNCATE:
				case ANALYZE:
				case COMPUTE:
				case LIST:
				case STATISTICS:
				case PARTITIONED:
				case EXTERNAL:
				case DEFINED:
				case REVOKE:
				case GRANT:
				case LOCK:
				case UNLOCK:
				case MSCK:
				case REPAIR:
				case RECOVER:
				case EXPORT:
				case IMPORT:
				case LOAD:
				case ROLE:
				case ROLES:
				case COMPACTIONS:
				case PRINCIPALS:
				case TRANSACTIONS:
				case INDEX:
				case INDEXES:
				case LOCKS:
				case OPTION:
				case ANTI:
				case LOCAL:
				case INPATH:
				case IDENTIFIER:
				case BACKQUOTED_IDENTIFIER:
					{
					setState(1936);
					identifier();
					}
					break;
				case T__0:
					{
					setState(1937);
					identifierList();
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class NamedExpressionSeqContext extends ParserRuleContext {
		public List<NamedExpressionContext> namedExpression() {
			return getRuleContexts(NamedExpressionContext.class);
		}
		public NamedExpressionContext namedExpression(int i) {
			return getRuleContext(NamedExpressionContext.class,i);
		}
		public NamedExpressionSeqContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_namedExpressionSeq; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterNamedExpressionSeq(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitNamedExpressionSeq(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitNamedExpressionSeq(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NamedExpressionSeqContext namedExpressionSeq() throws RecognitionException {
		NamedExpressionSeqContext _localctx = new NamedExpressionSeqContext(_ctx, getState());
		enterRule(_localctx, 146, RULE_namedExpressionSeq);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1942);
			namedExpression();
			setState(1947);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,258,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(1943);
					match(T__2);
					setState(1944);
					namedExpression();
					}
					} 
				}
				setState(1949);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,258,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ExpressionContext extends ParserRuleContext {
		public BooleanExpressionContext booleanExpression() {
			return getRuleContext(BooleanExpressionContext.class,0);
		}
		public ExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_expression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ExpressionContext expression() throws RecognitionException {
		ExpressionContext _localctx = new ExpressionContext(_ctx, getState());
		enterRule(_localctx, 148, RULE_expression);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1950);
			booleanExpression(0);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class BooleanExpressionContext extends ParserRuleContext {
		public BooleanExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_booleanExpression; }
	 
		public BooleanExpressionContext() { }
		public void copyFrom(BooleanExpressionContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class LogicalNotContext extends BooleanExpressionContext {
		public TerminalNode NOT() { return getToken(SqlHiveParser.NOT, 0); }
		public BooleanExpressionContext booleanExpression() {
			return getRuleContext(BooleanExpressionContext.class,0);
		}
		public LogicalNotContext(BooleanExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterLogicalNot(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitLogicalNot(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitLogicalNot(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class PredicatedContext extends BooleanExpressionContext {
		public ValueExpressionContext valueExpression() {
			return getRuleContext(ValueExpressionContext.class,0);
		}
		public PredicateContext predicate() {
			return getRuleContext(PredicateContext.class,0);
		}
		public PredicatedContext(BooleanExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterPredicated(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitPredicated(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitPredicated(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ExistsContext extends BooleanExpressionContext {
		public TerminalNode EXISTS() { return getToken(SqlHiveParser.EXISTS, 0); }
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public ExistsContext(BooleanExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterExists(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitExists(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitExists(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class LogicalBinaryContext extends BooleanExpressionContext {
		public BooleanExpressionContext left;
		public Token operator;
		public BooleanExpressionContext right;
		public List<BooleanExpressionContext> booleanExpression() {
			return getRuleContexts(BooleanExpressionContext.class);
		}
		public BooleanExpressionContext booleanExpression(int i) {
			return getRuleContext(BooleanExpressionContext.class,i);
		}
		public TerminalNode AND() { return getToken(SqlHiveParser.AND, 0); }
		public TerminalNode OR() { return getToken(SqlHiveParser.OR, 0); }
		public LogicalBinaryContext(BooleanExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterLogicalBinary(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitLogicalBinary(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitLogicalBinary(this);
			else return visitor.visitChildren(this);
		}
	}

	public final BooleanExpressionContext booleanExpression() throws RecognitionException {
		return booleanExpression(0);
	}

	private BooleanExpressionContext booleanExpression(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		BooleanExpressionContext _localctx = new BooleanExpressionContext(_ctx, _parentState);
		BooleanExpressionContext _prevctx = _localctx;
		int _startState = 150;
		enterRecursionRule(_localctx, 150, RULE_booleanExpression, _p);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1964);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,260,_ctx) ) {
			case 1:
				{
				_localctx = new LogicalNotContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;

				setState(1953);
				match(NOT);
				setState(1954);
				booleanExpression(5);
				}
				break;
			case 2:
				{
				_localctx = new ExistsContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1955);
				match(EXISTS);
				setState(1956);
				match(T__0);
				setState(1957);
				query();
				setState(1958);
				match(T__1);
				}
				break;
			case 3:
				{
				_localctx = new PredicatedContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1960);
				valueExpression(0);
				setState(1962);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,259,_ctx) ) {
				case 1:
					{
					setState(1961);
					predicate();
					}
					break;
				}
				}
				break;
			}
			_ctx.stop = _input.LT(-1);
			setState(1974);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,262,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(1972);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,261,_ctx) ) {
					case 1:
						{
						_localctx = new LogicalBinaryContext(new BooleanExpressionContext(_parentctx, _parentState));
						((LogicalBinaryContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_booleanExpression);
						setState(1966);
						if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
						setState(1967);
						((LogicalBinaryContext)_localctx).operator = match(AND);
						setState(1968);
						((LogicalBinaryContext)_localctx).right = booleanExpression(3);
						}
						break;
					case 2:
						{
						_localctx = new LogicalBinaryContext(new BooleanExpressionContext(_parentctx, _parentState));
						((LogicalBinaryContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_booleanExpression);
						setState(1969);
						if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
						setState(1970);
						((LogicalBinaryContext)_localctx).operator = match(OR);
						setState(1971);
						((LogicalBinaryContext)_localctx).right = booleanExpression(2);
						}
						break;
					}
					} 
				}
				setState(1976);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,262,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	public static class PredicateContext extends ParserRuleContext {
		public Token kind;
		public ValueExpressionContext lower;
		public ValueExpressionContext upper;
		public ValueExpressionContext pattern;
		public ValueExpressionContext right;
		public TerminalNode AND() { return getToken(SqlHiveParser.AND, 0); }
		public TerminalNode BETWEEN() { return getToken(SqlHiveParser.BETWEEN, 0); }
		public List<ValueExpressionContext> valueExpression() {
			return getRuleContexts(ValueExpressionContext.class);
		}
		public ValueExpressionContext valueExpression(int i) {
			return getRuleContext(ValueExpressionContext.class,i);
		}
		public TerminalNode NOT() { return getToken(SqlHiveParser.NOT, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TerminalNode IN() { return getToken(SqlHiveParser.IN, 0); }
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public TerminalNode RLIKE() { return getToken(SqlHiveParser.RLIKE, 0); }
		public TerminalNode LIKE() { return getToken(SqlHiveParser.LIKE, 0); }
		public TerminalNode IS() { return getToken(SqlHiveParser.IS, 0); }
		public TerminalNode NULL() { return getToken(SqlHiveParser.NULL, 0); }
		public TerminalNode FROM() { return getToken(SqlHiveParser.FROM, 0); }
		public TerminalNode DISTINCT() { return getToken(SqlHiveParser.DISTINCT, 0); }
		public PredicateContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_predicate; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterPredicate(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitPredicate(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitPredicate(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PredicateContext predicate() throws RecognitionException {
		PredicateContext _localctx = new PredicateContext(_ctx, getState());
		enterRule(_localctx, 152, RULE_predicate);
		int _la;
		try {
			setState(2025);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,270,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1978);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(1977);
					match(NOT);
					}
				}

				setState(1980);
				((PredicateContext)_localctx).kind = match(BETWEEN);
				setState(1981);
				((PredicateContext)_localctx).lower = valueExpression(0);
				setState(1982);
				match(AND);
				setState(1983);
				((PredicateContext)_localctx).upper = valueExpression(0);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1986);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(1985);
					match(NOT);
					}
				}

				setState(1988);
				((PredicateContext)_localctx).kind = match(IN);
				setState(1989);
				match(T__0);
				setState(1990);
				expression();
				setState(1995);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__2) {
					{
					{
					setState(1991);
					match(T__2);
					setState(1992);
					expression();
					}
					}
					setState(1997);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(1998);
				match(T__1);
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(2001);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(2000);
					match(NOT);
					}
				}

				setState(2003);
				((PredicateContext)_localctx).kind = match(IN);
				setState(2004);
				match(T__0);
				setState(2005);
				query();
				setState(2006);
				match(T__1);
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(2009);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(2008);
					match(NOT);
					}
				}

				setState(2011);
				((PredicateContext)_localctx).kind = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==LIKE || _la==RLIKE) ) {
					((PredicateContext)_localctx).kind = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(2012);
				((PredicateContext)_localctx).pattern = valueExpression(0);
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(2013);
				match(IS);
				setState(2015);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(2014);
					match(NOT);
					}
				}

				setState(2017);
				((PredicateContext)_localctx).kind = match(NULL);
				}
				break;
			case 6:
				enterOuterAlt(_localctx, 6);
				{
				setState(2018);
				match(IS);
				setState(2020);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(2019);
					match(NOT);
					}
				}

				setState(2022);
				((PredicateContext)_localctx).kind = match(DISTINCT);
				setState(2023);
				match(FROM);
				setState(2024);
				((PredicateContext)_localctx).right = valueExpression(0);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ValueExpressionContext extends ParserRuleContext {
		public ValueExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_valueExpression; }
	 
		public ValueExpressionContext() { }
		public void copyFrom(ValueExpressionContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class ValueExpressionDefaultContext extends ValueExpressionContext {
		public PrimaryExpressionContext primaryExpression() {
			return getRuleContext(PrimaryExpressionContext.class,0);
		}
		public ValueExpressionDefaultContext(ValueExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterValueExpressionDefault(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitValueExpressionDefault(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitValueExpressionDefault(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ComparisonContext extends ValueExpressionContext {
		public ValueExpressionContext left;
		public ValueExpressionContext right;
		public ComparisonOperatorContext comparisonOperator() {
			return getRuleContext(ComparisonOperatorContext.class,0);
		}
		public List<ValueExpressionContext> valueExpression() {
			return getRuleContexts(ValueExpressionContext.class);
		}
		public ValueExpressionContext valueExpression(int i) {
			return getRuleContext(ValueExpressionContext.class,i);
		}
		public ComparisonContext(ValueExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterComparison(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitComparison(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitComparison(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ArithmeticBinaryContext extends ValueExpressionContext {
		public ValueExpressionContext left;
		public Token operator;
		public ValueExpressionContext right;
		public List<ValueExpressionContext> valueExpression() {
			return getRuleContexts(ValueExpressionContext.class);
		}
		public ValueExpressionContext valueExpression(int i) {
			return getRuleContext(ValueExpressionContext.class,i);
		}
		public TerminalNode ASTERISK() { return getToken(SqlHiveParser.ASTERISK, 0); }
		public TerminalNode SLASH() { return getToken(SqlHiveParser.SLASH, 0); }
		public TerminalNode PERCENT() { return getToken(SqlHiveParser.PERCENT, 0); }
		public TerminalNode DIV() { return getToken(SqlHiveParser.DIV, 0); }
		public TerminalNode PLUS() { return getToken(SqlHiveParser.PLUS, 0); }
		public TerminalNode MINUS() { return getToken(SqlHiveParser.MINUS, 0); }
		public TerminalNode CONCAT_PIPE() { return getToken(SqlHiveParser.CONCAT_PIPE, 0); }
		public TerminalNode AMPERSAND() { return getToken(SqlHiveParser.AMPERSAND, 0); }
		public TerminalNode HAT() { return getToken(SqlHiveParser.HAT, 0); }
		public TerminalNode PIPE() { return getToken(SqlHiveParser.PIPE, 0); }
		public ArithmeticBinaryContext(ValueExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterArithmeticBinary(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitArithmeticBinary(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitArithmeticBinary(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ArithmeticUnaryContext extends ValueExpressionContext {
		public Token operator;
		public ValueExpressionContext valueExpression() {
			return getRuleContext(ValueExpressionContext.class,0);
		}
		public TerminalNode MINUS() { return getToken(SqlHiveParser.MINUS, 0); }
		public TerminalNode PLUS() { return getToken(SqlHiveParser.PLUS, 0); }
		public TerminalNode TILDE() { return getToken(SqlHiveParser.TILDE, 0); }
		public ArithmeticUnaryContext(ValueExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterArithmeticUnary(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitArithmeticUnary(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitArithmeticUnary(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ValueExpressionContext valueExpression() throws RecognitionException {
		return valueExpression(0);
	}

	private ValueExpressionContext valueExpression(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		ValueExpressionContext _localctx = new ValueExpressionContext(_ctx, _parentState);
		ValueExpressionContext _prevctx = _localctx;
		int _startState = 154;
		enterRecursionRule(_localctx, 154, RULE_valueExpression, _p);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(2031);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,271,_ctx) ) {
			case 1:
				{
				_localctx = new ValueExpressionDefaultContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;

				setState(2028);
				primaryExpression(0);
				}
				break;
			case 2:
				{
				_localctx = new ArithmeticUnaryContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2029);
				((ArithmeticUnaryContext)_localctx).operator = _input.LT(1);
				_la = _input.LA(1);
				if ( !(((((_la - 139)) & ~0x3f) == 0 && ((1L << (_la - 139)) & ((1L << (PLUS - 139)) | (1L << (MINUS - 139)) | (1L << (TILDE - 139)))) != 0)) ) {
					((ArithmeticUnaryContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(2030);
				valueExpression(7);
				}
				break;
			}
			_ctx.stop = _input.LT(-1);
			setState(2054);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,273,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(2052);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,272,_ctx) ) {
					case 1:
						{
						_localctx = new ArithmeticBinaryContext(new ValueExpressionContext(_parentctx, _parentState));
						((ArithmeticBinaryContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
						setState(2033);
						if (!(precpred(_ctx, 6))) throw new FailedPredicateException(this, "precpred(_ctx, 6)");
						setState(2034);
						((ArithmeticBinaryContext)_localctx).operator = _input.LT(1);
						_la = _input.LA(1);
						if ( !(((((_la - 141)) & ~0x3f) == 0 && ((1L << (_la - 141)) & ((1L << (ASTERISK - 141)) | (1L << (SLASH - 141)) | (1L << (PERCENT - 141)) | (1L << (DIV - 141)))) != 0)) ) {
							((ArithmeticBinaryContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						setState(2035);
						((ArithmeticBinaryContext)_localctx).right = valueExpression(7);
						}
						break;
					case 2:
						{
						_localctx = new ArithmeticBinaryContext(new ValueExpressionContext(_parentctx, _parentState));
						((ArithmeticBinaryContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
						setState(2036);
						if (!(precpred(_ctx, 5))) throw new FailedPredicateException(this, "precpred(_ctx, 5)");
						setState(2037);
						((ArithmeticBinaryContext)_localctx).operator = _input.LT(1);
						_la = _input.LA(1);
						if ( !(((((_la - 139)) & ~0x3f) == 0 && ((1L << (_la - 139)) & ((1L << (PLUS - 139)) | (1L << (MINUS - 139)) | (1L << (CONCAT_PIPE - 139)))) != 0)) ) {
							((ArithmeticBinaryContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						setState(2038);
						((ArithmeticBinaryContext)_localctx).right = valueExpression(6);
						}
						break;
					case 3:
						{
						_localctx = new ArithmeticBinaryContext(new ValueExpressionContext(_parentctx, _parentState));
						((ArithmeticBinaryContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
						setState(2039);
						if (!(precpred(_ctx, 4))) throw new FailedPredicateException(this, "precpred(_ctx, 4)");
						setState(2040);
						((ArithmeticBinaryContext)_localctx).operator = match(AMPERSAND);
						setState(2041);
						((ArithmeticBinaryContext)_localctx).right = valueExpression(5);
						}
						break;
					case 4:
						{
						_localctx = new ArithmeticBinaryContext(new ValueExpressionContext(_parentctx, _parentState));
						((ArithmeticBinaryContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
						setState(2042);
						if (!(precpred(_ctx, 3))) throw new FailedPredicateException(this, "precpred(_ctx, 3)");
						setState(2043);
						((ArithmeticBinaryContext)_localctx).operator = match(HAT);
						setState(2044);
						((ArithmeticBinaryContext)_localctx).right = valueExpression(4);
						}
						break;
					case 5:
						{
						_localctx = new ArithmeticBinaryContext(new ValueExpressionContext(_parentctx, _parentState));
						((ArithmeticBinaryContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
						setState(2045);
						if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
						setState(2046);
						((ArithmeticBinaryContext)_localctx).operator = match(PIPE);
						setState(2047);
						((ArithmeticBinaryContext)_localctx).right = valueExpression(3);
						}
						break;
					case 6:
						{
						_localctx = new ComparisonContext(new ValueExpressionContext(_parentctx, _parentState));
						((ComparisonContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
						setState(2048);
						if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
						setState(2049);
						comparisonOperator();
						setState(2050);
						((ComparisonContext)_localctx).right = valueExpression(2);
						}
						break;
					}
					} 
				}
				setState(2056);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,273,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	public static class PrimaryExpressionContext extends ParserRuleContext {
		public PrimaryExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_primaryExpression; }
	 
		public PrimaryExpressionContext() { }
		public void copyFrom(PrimaryExpressionContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class StructContext extends PrimaryExpressionContext {
		public NamedExpressionContext namedExpression;
		public List<NamedExpressionContext> argument = new ArrayList<NamedExpressionContext>();
		public TerminalNode STRUCT() { return getToken(SqlHiveParser.STRUCT, 0); }
		public List<NamedExpressionContext> namedExpression() {
			return getRuleContexts(NamedExpressionContext.class);
		}
		public NamedExpressionContext namedExpression(int i) {
			return getRuleContext(NamedExpressionContext.class,i);
		}
		public StructContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterStruct(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitStruct(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitStruct(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class DereferenceContext extends PrimaryExpressionContext {
		public PrimaryExpressionContext base;
		public IdentifierContext fieldName;
		public PrimaryExpressionContext primaryExpression() {
			return getRuleContext(PrimaryExpressionContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public DereferenceContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterDereference(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitDereference(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitDereference(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class SimpleCaseContext extends PrimaryExpressionContext {
		public ExpressionContext value;
		public ExpressionContext elseExpression;
		public TerminalNode CASE() { return getToken(SqlHiveParser.CASE, 0); }
		public TerminalNode END() { return getToken(SqlHiveParser.END, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public List<WhenClauseContext> whenClause() {
			return getRuleContexts(WhenClauseContext.class);
		}
		public WhenClauseContext whenClause(int i) {
			return getRuleContext(WhenClauseContext.class,i);
		}
		public TerminalNode ELSE() { return getToken(SqlHiveParser.ELSE, 0); }
		public SimpleCaseContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterSimpleCase(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitSimpleCase(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitSimpleCase(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ColumnReferenceContext extends PrimaryExpressionContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public ColumnReferenceContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterColumnReference(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitColumnReference(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitColumnReference(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class RowConstructorContext extends PrimaryExpressionContext {
		public List<NamedExpressionContext> namedExpression() {
			return getRuleContexts(NamedExpressionContext.class);
		}
		public NamedExpressionContext namedExpression(int i) {
			return getRuleContext(NamedExpressionContext.class,i);
		}
		public RowConstructorContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterRowConstructor(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitRowConstructor(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitRowConstructor(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class LastContext extends PrimaryExpressionContext {
		public TerminalNode LAST() { return getToken(SqlHiveParser.LAST, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode IGNORE() { return getToken(SqlHiveParser.IGNORE, 0); }
		public TerminalNode NULLS() { return getToken(SqlHiveParser.NULLS, 0); }
		public LastContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterLast(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitLast(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitLast(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class StarContext extends PrimaryExpressionContext {
		public TerminalNode ASTERISK() { return getToken(SqlHiveParser.ASTERISK, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public StarContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterStar(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitStar(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitStar(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class SubscriptContext extends PrimaryExpressionContext {
		public PrimaryExpressionContext value;
		public ValueExpressionContext index;
		public PrimaryExpressionContext primaryExpression() {
			return getRuleContext(PrimaryExpressionContext.class,0);
		}
		public ValueExpressionContext valueExpression() {
			return getRuleContext(ValueExpressionContext.class,0);
		}
		public SubscriptContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterSubscript(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitSubscript(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitSubscript(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class SubqueryExpressionContext extends PrimaryExpressionContext {
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public SubqueryExpressionContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterSubqueryExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitSubqueryExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitSubqueryExpression(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class CastContext extends PrimaryExpressionContext {
		public TerminalNode CAST() { return getToken(SqlHiveParser.CAST, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlHiveParser.AS, 0); }
		public DataTypeContext dataType() {
			return getRuleContext(DataTypeContext.class,0);
		}
		public CastContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterCast(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitCast(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitCast(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ConstantDefaultContext extends PrimaryExpressionContext {
		public ConstantContext constant() {
			return getRuleContext(ConstantContext.class,0);
		}
		public ConstantDefaultContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterConstantDefault(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitConstantDefault(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitConstantDefault(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class LambdaContext extends PrimaryExpressionContext {
		public List<TerminalNode> IDENTIFIER() { return getTokens(SqlHiveParser.IDENTIFIER); }
		public TerminalNode IDENTIFIER(int i) {
			return getToken(SqlHiveParser.IDENTIFIER, i);
		}
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public LambdaContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterLambda(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitLambda(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitLambda(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ParenthesizedExpressionContext extends PrimaryExpressionContext {
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public ParenthesizedExpressionContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterParenthesizedExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitParenthesizedExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitParenthesizedExpression(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ExtractContext extends PrimaryExpressionContext {
		public IdentifierContext field;
		public ValueExpressionContext source;
		public TerminalNode EXTRACT() { return getToken(SqlHiveParser.EXTRACT, 0); }
		public TerminalNode FROM() { return getToken(SqlHiveParser.FROM, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public ValueExpressionContext valueExpression() {
			return getRuleContext(ValueExpressionContext.class,0);
		}
		public ExtractContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterExtract(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitExtract(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitExtract(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class FunctionCallContext extends PrimaryExpressionContext {
		public ExpressionContext expression;
		public List<ExpressionContext> argument = new ArrayList<ExpressionContext>();
		public Token trimOption;
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode OVER() { return getToken(SqlHiveParser.OVER, 0); }
		public WindowSpecContext windowSpec() {
			return getRuleContext(WindowSpecContext.class,0);
		}
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public SetQuantifierContext setQuantifier() {
			return getRuleContext(SetQuantifierContext.class,0);
		}
		public TerminalNode FROM() { return getToken(SqlHiveParser.FROM, 0); }
		public TerminalNode BOTH() { return getToken(SqlHiveParser.BOTH, 0); }
		public TerminalNode LEADING() { return getToken(SqlHiveParser.LEADING, 0); }
		public TerminalNode TRAILING() { return getToken(SqlHiveParser.TRAILING, 0); }
		public FunctionCallContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterFunctionCall(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitFunctionCall(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitFunctionCall(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class SearchedCaseContext extends PrimaryExpressionContext {
		public ExpressionContext elseExpression;
		public TerminalNode CASE() { return getToken(SqlHiveParser.CASE, 0); }
		public TerminalNode END() { return getToken(SqlHiveParser.END, 0); }
		public List<WhenClauseContext> whenClause() {
			return getRuleContexts(WhenClauseContext.class);
		}
		public WhenClauseContext whenClause(int i) {
			return getRuleContext(WhenClauseContext.class,i);
		}
		public TerminalNode ELSE() { return getToken(SqlHiveParser.ELSE, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public SearchedCaseContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterSearchedCase(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitSearchedCase(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitSearchedCase(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class PositionContext extends PrimaryExpressionContext {
		public ValueExpressionContext substr;
		public ValueExpressionContext str;
		public TerminalNode POSITION() { return getToken(SqlHiveParser.POSITION, 0); }
		public TerminalNode IN() { return getToken(SqlHiveParser.IN, 0); }
		public List<ValueExpressionContext> valueExpression() {
			return getRuleContexts(ValueExpressionContext.class);
		}
		public ValueExpressionContext valueExpression(int i) {
			return getRuleContext(ValueExpressionContext.class,i);
		}
		public PositionContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterPosition(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitPosition(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitPosition(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class FirstContext extends PrimaryExpressionContext {
		public TerminalNode FIRST() { return getToken(SqlHiveParser.FIRST, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode IGNORE() { return getToken(SqlHiveParser.IGNORE, 0); }
		public TerminalNode NULLS() { return getToken(SqlHiveParser.NULLS, 0); }
		public FirstContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterFirst(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitFirst(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitFirst(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PrimaryExpressionContext primaryExpression() throws RecognitionException {
		return primaryExpression(0);
	}

	private PrimaryExpressionContext primaryExpression(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		PrimaryExpressionContext _localctx = new PrimaryExpressionContext(_ctx, _parentState);
		PrimaryExpressionContext _prevctx = _localctx;
		int _startState = 156;
		enterRecursionRule(_localctx, 156, RULE_primaryExpression, _p);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(2202);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,288,_ctx) ) {
			case 1:
				{
				_localctx = new SearchedCaseContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;

				setState(2058);
				match(CASE);
				setState(2060); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(2059);
					whenClause();
					}
					}
					setState(2062); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==WHEN );
				setState(2066);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ELSE) {
					{
					setState(2064);
					match(ELSE);
					setState(2065);
					((SearchedCaseContext)_localctx).elseExpression = expression();
					}
				}

				setState(2068);
				match(END);
				}
				break;
			case 2:
				{
				_localctx = new SimpleCaseContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2070);
				match(CASE);
				setState(2071);
				((SimpleCaseContext)_localctx).value = expression();
				setState(2073); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(2072);
					whenClause();
					}
					}
					setState(2075); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==WHEN );
				setState(2079);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ELSE) {
					{
					setState(2077);
					match(ELSE);
					setState(2078);
					((SimpleCaseContext)_localctx).elseExpression = expression();
					}
				}

				setState(2081);
				match(END);
				}
				break;
			case 3:
				{
				_localctx = new CastContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2083);
				match(CAST);
				setState(2084);
				match(T__0);
				setState(2085);
				expression();
				setState(2086);
				match(AS);
				setState(2087);
				dataType();
				setState(2088);
				match(T__1);
				}
				break;
			case 4:
				{
				_localctx = new StructContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2090);
				match(STRUCT);
				setState(2091);
				match(T__0);
				setState(2100);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__0) | (1L << SELECT) | (1L << FROM) | (1L << ADD) | (1L << AS) | (1L << ALL) | (1L << ANY) | (1L << DISTINCT) | (1L << WHERE) | (1L << GROUP) | (1L << BY) | (1L << GROUPING) | (1L << SETS) | (1L << CUBE) | (1L << ROLLUP) | (1L << ORDER) | (1L << HAVING) | (1L << LIMIT) | (1L << AT) | (1L << OR) | (1L << AND) | (1L << IN) | (1L << NOT) | (1L << NO) | (1L << EXISTS) | (1L << BETWEEN) | (1L << LIKE) | (1L << RLIKE) | (1L << IS) | (1L << NULL) | (1L << TRUE) | (1L << FALSE) | (1L << NULLS) | (1L << ASC) | (1L << DESC) | (1L << FOR) | (1L << INTERVAL) | (1L << CASE) | (1L << WHEN) | (1L << THEN) | (1L << ELSE) | (1L << END) | (1L << JOIN) | (1L << CROSS) | (1L << OUTER) | (1L << INNER) | (1L << LEFT) | (1L << SEMI) | (1L << RIGHT) | (1L << FULL) | (1L << NATURAL) | (1L << ON) | (1L << PIVOT) | (1L << LATERAL))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (WINDOW - 64)) | (1L << (OVER - 64)) | (1L << (PARTITION - 64)) | (1L << (RANGE - 64)) | (1L << (ROWS - 64)) | (1L << (UNBOUNDED - 64)) | (1L << (PRECEDING - 64)) | (1L << (FOLLOWING - 64)) | (1L << (CURRENT - 64)) | (1L << (FIRST - 64)) | (1L << (AFTER - 64)) | (1L << (LAST - 64)) | (1L << (ROW - 64)) | (1L << (WITH - 64)) | (1L << (VALUES - 64)) | (1L << (CREATE - 64)) | (1L << (TABLE - 64)) | (1L << (DIRECTORY - 64)) | (1L << (VIEW - 64)) | (1L << (REPLACE - 64)) | (1L << (INSERT - 64)) | (1L << (UPDATE - 64)) | (1L << (DELETE - 64)) | (1L << (INTO - 64)) | (1L << (DESCRIBE - 64)) | (1L << (EXPLAIN - 64)) | (1L << (FORMAT - 64)) | (1L << (LOGICAL - 64)) | (1L << (CODEGEN - 64)) | (1L << (COST - 64)) | (1L << (CAST - 64)) | (1L << (SHOW - 64)) | (1L << (TABLES - 64)) | (1L << (COLUMNS - 64)) | (1L << (COLUMN - 64)) | (1L << (USE - 64)) | (1L << (PARTITIONS - 64)) | (1L << (FUNCTIONS - 64)) | (1L << (DROP - 64)) | (1L << (UNION - 64)) | (1L << (EXCEPT - 64)) | (1L << (SETMINUS - 64)) | (1L << (INTERSECT - 64)) | (1L << (TO - 64)) | (1L << (TABLESAMPLE - 64)) | (1L << (STRATIFY - 64)) | (1L << (ALTER - 64)) | (1L << (RENAME - 64)) | (1L << (ARRAY - 64)) | (1L << (MAP - 64)) | (1L << (STRUCT - 64)) | (1L << (COMMENT - 64)) | (1L << (SET - 64)) | (1L << (RESET - 64)) | (1L << (DATA - 64)) | (1L << (START - 64)) | (1L << (TRANSACTION - 64)) | (1L << (COMMIT - 64)) | (1L << (ROLLBACK - 64)) | (1L << (MACRO - 64)) | (1L << (IGNORE - 64)) | (1L << (BOTH - 64)) | (1L << (LEADING - 64)) | (1L << (TRAILING - 64)))) != 0) || ((((_la - 128)) & ~0x3f) == 0 && ((1L << (_la - 128)) & ((1L << (IF - 128)) | (1L << (POSITION - 128)) | (1L << (EXTRACT - 128)) | (1L << (PLUS - 128)) | (1L << (MINUS - 128)) | (1L << (ASTERISK - 128)) | (1L << (DIV - 128)) | (1L << (TILDE - 128)) | (1L << (PERCENTLIT - 128)) | (1L << (BUCKET - 128)) | (1L << (OUT - 128)) | (1L << (OF - 128)) | (1L << (SORT - 128)) | (1L << (CLUSTER - 128)) | (1L << (DISTRIBUTE - 128)) | (1L << (OVERWRITE - 128)) | (1L << (TRANSFORM - 128)) | (1L << (REDUCE - 128)) | (1L << (SERDE - 128)) | (1L << (SERDEPROPERTIES - 128)) | (1L << (RECORDREADER - 128)) | (1L << (RECORDWRITER - 128)) | (1L << (DELIMITED - 128)) | (1L << (FIELDS - 128)) | (1L << (TERMINATED - 128)) | (1L << (COLLECTION - 128)) | (1L << (ITEMS - 128)) | (1L << (KEYS - 128)) | (1L << (ESCAPED - 128)) | (1L << (LINES - 128)) | (1L << (SEPARATED - 128)) | (1L << (FUNCTION - 128)) | (1L << (EXTENDED - 128)) | (1L << (REFRESH - 128)) | (1L << (CLEAR - 128)) | (1L << (CACHE - 128)) | (1L << (UNCACHE - 128)) | (1L << (LAZY - 128)) | (1L << (FORMATTED - 128)) | (1L << (GLOBAL - 128)) | (1L << (TEMPORARY - 128)) | (1L << (OPTIONS - 128)) | (1L << (UNSET - 128)) | (1L << (TBLPROPERTIES - 128)) | (1L << (DBPROPERTIES - 128)) | (1L << (BUCKETS - 128)) | (1L << (SKEWED - 128)) | (1L << (STORED - 128)) | (1L << (DIRECTORIES - 128)))) != 0) || ((((_la - 192)) & ~0x3f) == 0 && ((1L << (_la - 192)) & ((1L << (LOCATION - 192)) | (1L << (EXCHANGE - 192)) | (1L << (ARCHIVE - 192)) | (1L << (UNARCHIVE - 192)) | (1L << (FILEFORMAT - 192)) | (1L << (TOUCH - 192)) | (1L << (COMPACT - 192)) | (1L << (CONCATENATE - 192)) | (1L << (CHANGE - 192)) | (1L << (CASCADE - 192)) | (1L << (RESTRICT - 192)) | (1L << (CLUSTERED - 192)) | (1L << (SORTED - 192)) | (1L << (PURGE - 192)) | (1L << (INPUTFORMAT - 192)) | (1L << (OUTPUTFORMAT - 192)) | (1L << (DATABASE - 192)) | (1L << (DATABASES - 192)) | (1L << (DFS - 192)) | (1L << (TRUNCATE - 192)) | (1L << (ANALYZE - 192)) | (1L << (COMPUTE - 192)) | (1L << (LIST - 192)) | (1L << (STATISTICS - 192)) | (1L << (PARTITIONED - 192)) | (1L << (EXTERNAL - 192)) | (1L << (DEFINED - 192)) | (1L << (REVOKE - 192)) | (1L << (GRANT - 192)) | (1L << (LOCK - 192)) | (1L << (UNLOCK - 192)) | (1L << (MSCK - 192)) | (1L << (REPAIR - 192)) | (1L << (RECOVER - 192)) | (1L << (EXPORT - 192)) | (1L << (IMPORT - 192)) | (1L << (LOAD - 192)) | (1L << (ROLE - 192)) | (1L << (ROLES - 192)) | (1L << (COMPACTIONS - 192)) | (1L << (PRINCIPALS - 192)) | (1L << (TRANSACTIONS - 192)) | (1L << (INDEX - 192)) | (1L << (INDEXES - 192)) | (1L << (LOCKS - 192)) | (1L << (OPTION - 192)) | (1L << (ANTI - 192)) | (1L << (LOCAL - 192)) | (1L << (INPATH - 192)) | (1L << (STRING - 192)) | (1L << (BIGINT_LITERAL - 192)) | (1L << (SMALLINT_LITERAL - 192)) | (1L << (TINYINT_LITERAL - 192)) | (1L << (INTEGER_VALUE - 192)) | (1L << (DECIMAL_VALUE - 192)) | (1L << (DOUBLE_LITERAL - 192)) | (1L << (BIGDECIMAL_LITERAL - 192)) | (1L << (IDENTIFIER - 192)) | (1L << (BACKQUOTED_IDENTIFIER - 192)))) != 0)) {
					{
					setState(2092);
					((StructContext)_localctx).namedExpression = namedExpression();
					((StructContext)_localctx).argument.add(((StructContext)_localctx).namedExpression);
					setState(2097);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__2) {
						{
						{
						setState(2093);
						match(T__2);
						setState(2094);
						((StructContext)_localctx).namedExpression = namedExpression();
						((StructContext)_localctx).argument.add(((StructContext)_localctx).namedExpression);
						}
						}
						setState(2099);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				setState(2102);
				match(T__1);
				}
				break;
			case 5:
				{
				_localctx = new FirstContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2103);
				match(FIRST);
				setState(2104);
				match(T__0);
				setState(2105);
				expression();
				setState(2108);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==IGNORE) {
					{
					setState(2106);
					match(IGNORE);
					setState(2107);
					match(NULLS);
					}
				}

				setState(2110);
				match(T__1);
				}
				break;
			case 6:
				{
				_localctx = new LastContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2112);
				match(LAST);
				setState(2113);
				match(T__0);
				setState(2114);
				expression();
				setState(2117);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==IGNORE) {
					{
					setState(2115);
					match(IGNORE);
					setState(2116);
					match(NULLS);
					}
				}

				setState(2119);
				match(T__1);
				}
				break;
			case 7:
				{
				_localctx = new PositionContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2121);
				match(POSITION);
				setState(2122);
				match(T__0);
				setState(2123);
				((PositionContext)_localctx).substr = valueExpression(0);
				setState(2124);
				match(IN);
				setState(2125);
				((PositionContext)_localctx).str = valueExpression(0);
				setState(2126);
				match(T__1);
				}
				break;
			case 8:
				{
				_localctx = new ConstantDefaultContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2128);
				constant();
				}
				break;
			case 9:
				{
				_localctx = new StarContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2129);
				match(ASTERISK);
				}
				break;
			case 10:
				{
				_localctx = new StarContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2130);
				qualifiedName();
				setState(2131);
				match(T__3);
				setState(2132);
				match(ASTERISK);
				}
				break;
			case 11:
				{
				_localctx = new RowConstructorContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2134);
				match(T__0);
				setState(2135);
				namedExpression();
				setState(2138); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(2136);
					match(T__2);
					setState(2137);
					namedExpression();
					}
					}
					setState(2140); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==T__2 );
				setState(2142);
				match(T__1);
				}
				break;
			case 12:
				{
				_localctx = new SubqueryExpressionContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2144);
				match(T__0);
				setState(2145);
				query();
				setState(2146);
				match(T__1);
				}
				break;
			case 13:
				{
				_localctx = new FunctionCallContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2148);
				qualifiedName();
				setState(2149);
				match(T__0);
				setState(2161);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__0) | (1L << SELECT) | (1L << FROM) | (1L << ADD) | (1L << AS) | (1L << ALL) | (1L << ANY) | (1L << DISTINCT) | (1L << WHERE) | (1L << GROUP) | (1L << BY) | (1L << GROUPING) | (1L << SETS) | (1L << CUBE) | (1L << ROLLUP) | (1L << ORDER) | (1L << HAVING) | (1L << LIMIT) | (1L << AT) | (1L << OR) | (1L << AND) | (1L << IN) | (1L << NOT) | (1L << NO) | (1L << EXISTS) | (1L << BETWEEN) | (1L << LIKE) | (1L << RLIKE) | (1L << IS) | (1L << NULL) | (1L << TRUE) | (1L << FALSE) | (1L << NULLS) | (1L << ASC) | (1L << DESC) | (1L << FOR) | (1L << INTERVAL) | (1L << CASE) | (1L << WHEN) | (1L << THEN) | (1L << ELSE) | (1L << END) | (1L << JOIN) | (1L << CROSS) | (1L << OUTER) | (1L << INNER) | (1L << LEFT) | (1L << SEMI) | (1L << RIGHT) | (1L << FULL) | (1L << NATURAL) | (1L << ON) | (1L << PIVOT) | (1L << LATERAL))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (WINDOW - 64)) | (1L << (OVER - 64)) | (1L << (PARTITION - 64)) | (1L << (RANGE - 64)) | (1L << (ROWS - 64)) | (1L << (UNBOUNDED - 64)) | (1L << (PRECEDING - 64)) | (1L << (FOLLOWING - 64)) | (1L << (CURRENT - 64)) | (1L << (FIRST - 64)) | (1L << (AFTER - 64)) | (1L << (LAST - 64)) | (1L << (ROW - 64)) | (1L << (WITH - 64)) | (1L << (VALUES - 64)) | (1L << (CREATE - 64)) | (1L << (TABLE - 64)) | (1L << (DIRECTORY - 64)) | (1L << (VIEW - 64)) | (1L << (REPLACE - 64)) | (1L << (INSERT - 64)) | (1L << (UPDATE - 64)) | (1L << (DELETE - 64)) | (1L << (INTO - 64)) | (1L << (DESCRIBE - 64)) | (1L << (EXPLAIN - 64)) | (1L << (FORMAT - 64)) | (1L << (LOGICAL - 64)) | (1L << (CODEGEN - 64)) | (1L << (COST - 64)) | (1L << (CAST - 64)) | (1L << (SHOW - 64)) | (1L << (TABLES - 64)) | (1L << (COLUMNS - 64)) | (1L << (COLUMN - 64)) | (1L << (USE - 64)) | (1L << (PARTITIONS - 64)) | (1L << (FUNCTIONS - 64)) | (1L << (DROP - 64)) | (1L << (UNION - 64)) | (1L << (EXCEPT - 64)) | (1L << (SETMINUS - 64)) | (1L << (INTERSECT - 64)) | (1L << (TO - 64)) | (1L << (TABLESAMPLE - 64)) | (1L << (STRATIFY - 64)) | (1L << (ALTER - 64)) | (1L << (RENAME - 64)) | (1L << (ARRAY - 64)) | (1L << (MAP - 64)) | (1L << (STRUCT - 64)) | (1L << (COMMENT - 64)) | (1L << (SET - 64)) | (1L << (RESET - 64)) | (1L << (DATA - 64)) | (1L << (START - 64)) | (1L << (TRANSACTION - 64)) | (1L << (COMMIT - 64)) | (1L << (ROLLBACK - 64)) | (1L << (MACRO - 64)) | (1L << (IGNORE - 64)) | (1L << (BOTH - 64)) | (1L << (LEADING - 64)) | (1L << (TRAILING - 64)))) != 0) || ((((_la - 128)) & ~0x3f) == 0 && ((1L << (_la - 128)) & ((1L << (IF - 128)) | (1L << (POSITION - 128)) | (1L << (EXTRACT - 128)) | (1L << (PLUS - 128)) | (1L << (MINUS - 128)) | (1L << (ASTERISK - 128)) | (1L << (DIV - 128)) | (1L << (TILDE - 128)) | (1L << (PERCENTLIT - 128)) | (1L << (BUCKET - 128)) | (1L << (OUT - 128)) | (1L << (OF - 128)) | (1L << (SORT - 128)) | (1L << (CLUSTER - 128)) | (1L << (DISTRIBUTE - 128)) | (1L << (OVERWRITE - 128)) | (1L << (TRANSFORM - 128)) | (1L << (REDUCE - 128)) | (1L << (SERDE - 128)) | (1L << (SERDEPROPERTIES - 128)) | (1L << (RECORDREADER - 128)) | (1L << (RECORDWRITER - 128)) | (1L << (DELIMITED - 128)) | (1L << (FIELDS - 128)) | (1L << (TERMINATED - 128)) | (1L << (COLLECTION - 128)) | (1L << (ITEMS - 128)) | (1L << (KEYS - 128)) | (1L << (ESCAPED - 128)) | (1L << (LINES - 128)) | (1L << (SEPARATED - 128)) | (1L << (FUNCTION - 128)) | (1L << (EXTENDED - 128)) | (1L << (REFRESH - 128)) | (1L << (CLEAR - 128)) | (1L << (CACHE - 128)) | (1L << (UNCACHE - 128)) | (1L << (LAZY - 128)) | (1L << (FORMATTED - 128)) | (1L << (GLOBAL - 128)) | (1L << (TEMPORARY - 128)) | (1L << (OPTIONS - 128)) | (1L << (UNSET - 128)) | (1L << (TBLPROPERTIES - 128)) | (1L << (DBPROPERTIES - 128)) | (1L << (BUCKETS - 128)) | (1L << (SKEWED - 128)) | (1L << (STORED - 128)) | (1L << (DIRECTORIES - 128)))) != 0) || ((((_la - 192)) & ~0x3f) == 0 && ((1L << (_la - 192)) & ((1L << (LOCATION - 192)) | (1L << (EXCHANGE - 192)) | (1L << (ARCHIVE - 192)) | (1L << (UNARCHIVE - 192)) | (1L << (FILEFORMAT - 192)) | (1L << (TOUCH - 192)) | (1L << (COMPACT - 192)) | (1L << (CONCATENATE - 192)) | (1L << (CHANGE - 192)) | (1L << (CASCADE - 192)) | (1L << (RESTRICT - 192)) | (1L << (CLUSTERED - 192)) | (1L << (SORTED - 192)) | (1L << (PURGE - 192)) | (1L << (INPUTFORMAT - 192)) | (1L << (OUTPUTFORMAT - 192)) | (1L << (DATABASE - 192)) | (1L << (DATABASES - 192)) | (1L << (DFS - 192)) | (1L << (TRUNCATE - 192)) | (1L << (ANALYZE - 192)) | (1L << (COMPUTE - 192)) | (1L << (LIST - 192)) | (1L << (STATISTICS - 192)) | (1L << (PARTITIONED - 192)) | (1L << (EXTERNAL - 192)) | (1L << (DEFINED - 192)) | (1L << (REVOKE - 192)) | (1L << (GRANT - 192)) | (1L << (LOCK - 192)) | (1L << (UNLOCK - 192)) | (1L << (MSCK - 192)) | (1L << (REPAIR - 192)) | (1L << (RECOVER - 192)) | (1L << (EXPORT - 192)) | (1L << (IMPORT - 192)) | (1L << (LOAD - 192)) | (1L << (ROLE - 192)) | (1L << (ROLES - 192)) | (1L << (COMPACTIONS - 192)) | (1L << (PRINCIPALS - 192)) | (1L << (TRANSACTIONS - 192)) | (1L << (INDEX - 192)) | (1L << (INDEXES - 192)) | (1L << (LOCKS - 192)) | (1L << (OPTION - 192)) | (1L << (ANTI - 192)) | (1L << (LOCAL - 192)) | (1L << (INPATH - 192)) | (1L << (STRING - 192)) | (1L << (BIGINT_LITERAL - 192)) | (1L << (SMALLINT_LITERAL - 192)) | (1L << (TINYINT_LITERAL - 192)) | (1L << (INTEGER_VALUE - 192)) | (1L << (DECIMAL_VALUE - 192)) | (1L << (DOUBLE_LITERAL - 192)) | (1L << (BIGDECIMAL_LITERAL - 192)) | (1L << (IDENTIFIER - 192)) | (1L << (BACKQUOTED_IDENTIFIER - 192)))) != 0)) {
					{
					setState(2151);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,283,_ctx) ) {
					case 1:
						{
						setState(2150);
						setQuantifier();
						}
						break;
					}
					setState(2153);
					((FunctionCallContext)_localctx).expression = expression();
					((FunctionCallContext)_localctx).argument.add(((FunctionCallContext)_localctx).expression);
					setState(2158);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__2) {
						{
						{
						setState(2154);
						match(T__2);
						setState(2155);
						((FunctionCallContext)_localctx).expression = expression();
						((FunctionCallContext)_localctx).argument.add(((FunctionCallContext)_localctx).expression);
						}
						}
						setState(2160);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				setState(2163);
				match(T__1);
				setState(2166);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,286,_ctx) ) {
				case 1:
					{
					setState(2164);
					match(OVER);
					setState(2165);
					windowSpec();
					}
					break;
				}
				}
				break;
			case 14:
				{
				_localctx = new FunctionCallContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2168);
				qualifiedName();
				setState(2169);
				match(T__0);
				setState(2170);
				((FunctionCallContext)_localctx).trimOption = _input.LT(1);
				_la = _input.LA(1);
				if ( !(((((_la - 125)) & ~0x3f) == 0 && ((1L << (_la - 125)) & ((1L << (BOTH - 125)) | (1L << (LEADING - 125)) | (1L << (TRAILING - 125)))) != 0)) ) {
					((FunctionCallContext)_localctx).trimOption = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(2171);
				((FunctionCallContext)_localctx).expression = expression();
				((FunctionCallContext)_localctx).argument.add(((FunctionCallContext)_localctx).expression);
				setState(2172);
				match(FROM);
				setState(2173);
				((FunctionCallContext)_localctx).expression = expression();
				((FunctionCallContext)_localctx).argument.add(((FunctionCallContext)_localctx).expression);
				setState(2174);
				match(T__1);
				}
				break;
			case 15:
				{
				_localctx = new LambdaContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2176);
				match(IDENTIFIER);
				setState(2177);
				match(T__6);
				setState(2178);
				expression();
				}
				break;
			case 16:
				{
				_localctx = new LambdaContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2179);
				match(T__0);
				setState(2180);
				match(IDENTIFIER);
				setState(2183); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(2181);
					match(T__2);
					setState(2182);
					match(IDENTIFIER);
					}
					}
					setState(2185); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==T__2 );
				setState(2187);
				match(T__1);
				setState(2188);
				match(T__6);
				setState(2189);
				expression();
				}
				break;
			case 17:
				{
				_localctx = new ColumnReferenceContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2190);
				identifier();
				}
				break;
			case 18:
				{
				_localctx = new ParenthesizedExpressionContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2191);
				match(T__0);
				setState(2192);
				expression();
				setState(2193);
				match(T__1);
				}
				break;
			case 19:
				{
				_localctx = new ExtractContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2195);
				match(EXTRACT);
				setState(2196);
				match(T__0);
				setState(2197);
				((ExtractContext)_localctx).field = identifier();
				setState(2198);
				match(FROM);
				setState(2199);
				((ExtractContext)_localctx).source = valueExpression(0);
				setState(2200);
				match(T__1);
				}
				break;
			}
			_ctx.stop = _input.LT(-1);
			setState(2214);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,290,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(2212);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,289,_ctx) ) {
					case 1:
						{
						_localctx = new SubscriptContext(new PrimaryExpressionContext(_parentctx, _parentState));
						((SubscriptContext)_localctx).value = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_primaryExpression);
						setState(2204);
						if (!(precpred(_ctx, 5))) throw new FailedPredicateException(this, "precpred(_ctx, 5)");
						setState(2205);
						match(T__7);
						setState(2206);
						((SubscriptContext)_localctx).index = valueExpression(0);
						setState(2207);
						match(T__8);
						}
						break;
					case 2:
						{
						_localctx = new DereferenceContext(new PrimaryExpressionContext(_parentctx, _parentState));
						((DereferenceContext)_localctx).base = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_primaryExpression);
						setState(2209);
						if (!(precpred(_ctx, 3))) throw new FailedPredicateException(this, "precpred(_ctx, 3)");
						setState(2210);
						match(T__3);
						setState(2211);
						((DereferenceContext)_localctx).fieldName = identifier();
						}
						break;
					}
					} 
				}
				setState(2216);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,290,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	public static class ConstantContext extends ParserRuleContext {
		public ConstantContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_constant; }
	 
		public ConstantContext() { }
		public void copyFrom(ConstantContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class NullLiteralContext extends ConstantContext {
		public TerminalNode NULL() { return getToken(SqlHiveParser.NULL, 0); }
		public NullLiteralContext(ConstantContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterNullLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitNullLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitNullLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class StringLiteralContext extends ConstantContext {
		public List<TerminalNode> STRING() { return getTokens(SqlHiveParser.STRING); }
		public TerminalNode STRING(int i) {
			return getToken(SqlHiveParser.STRING, i);
		}
		public StringLiteralContext(ConstantContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterStringLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitStringLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitStringLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class TypeConstructorContext extends ConstantContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode STRING() { return getToken(SqlHiveParser.STRING, 0); }
		public TypeConstructorContext(ConstantContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterTypeConstructor(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitTypeConstructor(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitTypeConstructor(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class IntervalLiteralContext extends ConstantContext {
		public IntervalContext interval() {
			return getRuleContext(IntervalContext.class,0);
		}
		public IntervalLiteralContext(ConstantContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterIntervalLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitIntervalLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitIntervalLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class NumericLiteralContext extends ConstantContext {
		public NumberContext number() {
			return getRuleContext(NumberContext.class,0);
		}
		public NumericLiteralContext(ConstantContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterNumericLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitNumericLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitNumericLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class BooleanLiteralContext extends ConstantContext {
		public BooleanValueContext booleanValue() {
			return getRuleContext(BooleanValueContext.class,0);
		}
		public BooleanLiteralContext(ConstantContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterBooleanLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitBooleanLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitBooleanLiteral(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ConstantContext constant() throws RecognitionException {
		ConstantContext _localctx = new ConstantContext(_ctx, getState());
		enterRule(_localctx, 158, RULE_constant);
		try {
			int _alt;
			setState(2229);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,292,_ctx) ) {
			case 1:
				_localctx = new NullLiteralContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(2217);
				match(NULL);
				}
				break;
			case 2:
				_localctx = new IntervalLiteralContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(2218);
				interval();
				}
				break;
			case 3:
				_localctx = new TypeConstructorContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(2219);
				identifier();
				setState(2220);
				match(STRING);
				}
				break;
			case 4:
				_localctx = new NumericLiteralContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(2222);
				number();
				}
				break;
			case 5:
				_localctx = new BooleanLiteralContext(_localctx);
				enterOuterAlt(_localctx, 5);
				{
				setState(2223);
				booleanValue();
				}
				break;
			case 6:
				_localctx = new StringLiteralContext(_localctx);
				enterOuterAlt(_localctx, 6);
				{
				setState(2225); 
				_errHandler.sync(this);
				_alt = 1;
				do {
					switch (_alt) {
					case 1:
						{
						{
						setState(2224);
						match(STRING);
						}
						}
						break;
					default:
						throw new NoViableAltException(this);
					}
					setState(2227); 
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,291,_ctx);
				} while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER );
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ComparisonOperatorContext extends ParserRuleContext {
		public TerminalNode EQ() { return getToken(SqlHiveParser.EQ, 0); }
		public TerminalNode NEQ() { return getToken(SqlHiveParser.NEQ, 0); }
		public TerminalNode NEQJ() { return getToken(SqlHiveParser.NEQJ, 0); }
		public TerminalNode LT() { return getToken(SqlHiveParser.LT, 0); }
		public TerminalNode LTE() { return getToken(SqlHiveParser.LTE, 0); }
		public TerminalNode GT() { return getToken(SqlHiveParser.GT, 0); }
		public TerminalNode GTE() { return getToken(SqlHiveParser.GTE, 0); }
		public TerminalNode NSEQ() { return getToken(SqlHiveParser.NSEQ, 0); }
		public ComparisonOperatorContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_comparisonOperator; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterComparisonOperator(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitComparisonOperator(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitComparisonOperator(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ComparisonOperatorContext comparisonOperator() throws RecognitionException {
		ComparisonOperatorContext _localctx = new ComparisonOperatorContext(_ctx, getState());
		enterRule(_localctx, 160, RULE_comparisonOperator);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2231);
			_la = _input.LA(1);
			if ( !(((((_la - 131)) & ~0x3f) == 0 && ((1L << (_la - 131)) & ((1L << (EQ - 131)) | (1L << (NSEQ - 131)) | (1L << (NEQ - 131)) | (1L << (NEQJ - 131)) | (1L << (LT - 131)) | (1L << (LTE - 131)) | (1L << (GT - 131)) | (1L << (GTE - 131)))) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ArithmeticOperatorContext extends ParserRuleContext {
		public TerminalNode PLUS() { return getToken(SqlHiveParser.PLUS, 0); }
		public TerminalNode MINUS() { return getToken(SqlHiveParser.MINUS, 0); }
		public TerminalNode ASTERISK() { return getToken(SqlHiveParser.ASTERISK, 0); }
		public TerminalNode SLASH() { return getToken(SqlHiveParser.SLASH, 0); }
		public TerminalNode PERCENT() { return getToken(SqlHiveParser.PERCENT, 0); }
		public TerminalNode DIV() { return getToken(SqlHiveParser.DIV, 0); }
		public TerminalNode TILDE() { return getToken(SqlHiveParser.TILDE, 0); }
		public TerminalNode AMPERSAND() { return getToken(SqlHiveParser.AMPERSAND, 0); }
		public TerminalNode PIPE() { return getToken(SqlHiveParser.PIPE, 0); }
		public TerminalNode CONCAT_PIPE() { return getToken(SqlHiveParser.CONCAT_PIPE, 0); }
		public TerminalNode HAT() { return getToken(SqlHiveParser.HAT, 0); }
		public ArithmeticOperatorContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_arithmeticOperator; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterArithmeticOperator(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitArithmeticOperator(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitArithmeticOperator(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ArithmeticOperatorContext arithmeticOperator() throws RecognitionException {
		ArithmeticOperatorContext _localctx = new ArithmeticOperatorContext(_ctx, getState());
		enterRule(_localctx, 162, RULE_arithmeticOperator);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2233);
			_la = _input.LA(1);
			if ( !(((((_la - 139)) & ~0x3f) == 0 && ((1L << (_la - 139)) & ((1L << (PLUS - 139)) | (1L << (MINUS - 139)) | (1L << (ASTERISK - 139)) | (1L << (SLASH - 139)) | (1L << (PERCENT - 139)) | (1L << (DIV - 139)) | (1L << (TILDE - 139)) | (1L << (AMPERSAND - 139)) | (1L << (PIPE - 139)) | (1L << (CONCAT_PIPE - 139)) | (1L << (HAT - 139)))) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class PredicateOperatorContext extends ParserRuleContext {
		public TerminalNode OR() { return getToken(SqlHiveParser.OR, 0); }
		public TerminalNode AND() { return getToken(SqlHiveParser.AND, 0); }
		public TerminalNode IN() { return getToken(SqlHiveParser.IN, 0); }
		public TerminalNode NOT() { return getToken(SqlHiveParser.NOT, 0); }
		public PredicateOperatorContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_predicateOperator; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterPredicateOperator(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitPredicateOperator(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitPredicateOperator(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PredicateOperatorContext predicateOperator() throws RecognitionException {
		PredicateOperatorContext _localctx = new PredicateOperatorContext(_ctx, getState());
		enterRule(_localctx, 164, RULE_predicateOperator);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2235);
			_la = _input.LA(1);
			if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << OR) | (1L << AND) | (1L << IN) | (1L << NOT))) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class BooleanValueContext extends ParserRuleContext {
		public TerminalNode TRUE() { return getToken(SqlHiveParser.TRUE, 0); }
		public TerminalNode FALSE() { return getToken(SqlHiveParser.FALSE, 0); }
		public BooleanValueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_booleanValue; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterBooleanValue(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitBooleanValue(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitBooleanValue(this);
			else return visitor.visitChildren(this);
		}
	}

	public final BooleanValueContext booleanValue() throws RecognitionException {
		BooleanValueContext _localctx = new BooleanValueContext(_ctx, getState());
		enterRule(_localctx, 166, RULE_booleanValue);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2237);
			_la = _input.LA(1);
			if ( !(_la==TRUE || _la==FALSE) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class IntervalContext extends ParserRuleContext {
		public TerminalNode INTERVAL() { return getToken(SqlHiveParser.INTERVAL, 0); }
		public List<IntervalFieldContext> intervalField() {
			return getRuleContexts(IntervalFieldContext.class);
		}
		public IntervalFieldContext intervalField(int i) {
			return getRuleContext(IntervalFieldContext.class,i);
		}
		public IntervalContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_interval; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterInterval(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitInterval(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitInterval(this);
			else return visitor.visitChildren(this);
		}
	}

	public final IntervalContext interval() throws RecognitionException {
		IntervalContext _localctx = new IntervalContext(_ctx, getState());
		enterRule(_localctx, 168, RULE_interval);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(2239);
			match(INTERVAL);
			setState(2243);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,293,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(2240);
					intervalField();
					}
					} 
				}
				setState(2245);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,293,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class IntervalFieldContext extends ParserRuleContext {
		public IntervalValueContext value;
		public IdentifierContext unit;
		public IdentifierContext to;
		public IntervalValueContext intervalValue() {
			return getRuleContext(IntervalValueContext.class,0);
		}
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public TerminalNode TO() { return getToken(SqlHiveParser.TO, 0); }
		public IntervalFieldContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_intervalField; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterIntervalField(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitIntervalField(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitIntervalField(this);
			else return visitor.visitChildren(this);
		}
	}

	public final IntervalFieldContext intervalField() throws RecognitionException {
		IntervalFieldContext _localctx = new IntervalFieldContext(_ctx, getState());
		enterRule(_localctx, 170, RULE_intervalField);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2246);
			((IntervalFieldContext)_localctx).value = intervalValue();
			setState(2247);
			((IntervalFieldContext)_localctx).unit = identifier();
			setState(2250);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,294,_ctx) ) {
			case 1:
				{
				setState(2248);
				match(TO);
				setState(2249);
				((IntervalFieldContext)_localctx).to = identifier();
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class IntervalValueContext extends ParserRuleContext {
		public TerminalNode INTEGER_VALUE() { return getToken(SqlHiveParser.INTEGER_VALUE, 0); }
		public TerminalNode DECIMAL_VALUE() { return getToken(SqlHiveParser.DECIMAL_VALUE, 0); }
		public TerminalNode PLUS() { return getToken(SqlHiveParser.PLUS, 0); }
		public TerminalNode MINUS() { return getToken(SqlHiveParser.MINUS, 0); }
		public TerminalNode STRING() { return getToken(SqlHiveParser.STRING, 0); }
		public IntervalValueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_intervalValue; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterIntervalValue(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitIntervalValue(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitIntervalValue(this);
			else return visitor.visitChildren(this);
		}
	}

	public final IntervalValueContext intervalValue() throws RecognitionException {
		IntervalValueContext _localctx = new IntervalValueContext(_ctx, getState());
		enterRule(_localctx, 172, RULE_intervalValue);
		int _la;
		try {
			setState(2257);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case PLUS:
			case MINUS:
			case INTEGER_VALUE:
			case DECIMAL_VALUE:
				enterOuterAlt(_localctx, 1);
				{
				setState(2253);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PLUS || _la==MINUS) {
					{
					setState(2252);
					_la = _input.LA(1);
					if ( !(_la==PLUS || _la==MINUS) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
				}

				setState(2255);
				_la = _input.LA(1);
				if ( !(_la==INTEGER_VALUE || _la==DECIMAL_VALUE) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
				break;
			case STRING:
				enterOuterAlt(_localctx, 2);
				{
				setState(2256);
				match(STRING);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ColPositionContext extends ParserRuleContext {
		public TerminalNode FIRST() { return getToken(SqlHiveParser.FIRST, 0); }
		public TerminalNode AFTER() { return getToken(SqlHiveParser.AFTER, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public ColPositionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_colPosition; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterColPosition(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitColPosition(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitColPosition(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ColPositionContext colPosition() throws RecognitionException {
		ColPositionContext _localctx = new ColPositionContext(_ctx, getState());
		enterRule(_localctx, 174, RULE_colPosition);
		try {
			setState(2262);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case FIRST:
				enterOuterAlt(_localctx, 1);
				{
				setState(2259);
				match(FIRST);
				}
				break;
			case AFTER:
				enterOuterAlt(_localctx, 2);
				{
				setState(2260);
				match(AFTER);
				setState(2261);
				identifier();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class DataTypeContext extends ParserRuleContext {
		public DataTypeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_dataType; }
	 
		public DataTypeContext() { }
		public void copyFrom(DataTypeContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class ComplexDataTypeContext extends DataTypeContext {
		public Token complex;
		public TerminalNode LT() { return getToken(SqlHiveParser.LT, 0); }
		public List<DataTypeContext> dataType() {
			return getRuleContexts(DataTypeContext.class);
		}
		public DataTypeContext dataType(int i) {
			return getRuleContext(DataTypeContext.class,i);
		}
		public TerminalNode GT() { return getToken(SqlHiveParser.GT, 0); }
		public TerminalNode ARRAY() { return getToken(SqlHiveParser.ARRAY, 0); }
		public TerminalNode MAP() { return getToken(SqlHiveParser.MAP, 0); }
		public TerminalNode STRUCT() { return getToken(SqlHiveParser.STRUCT, 0); }
		public TerminalNode NEQ() { return getToken(SqlHiveParser.NEQ, 0); }
		public ComplexColTypeListContext complexColTypeList() {
			return getRuleContext(ComplexColTypeListContext.class,0);
		}
		public ComplexDataTypeContext(DataTypeContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterComplexDataType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitComplexDataType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitComplexDataType(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class PrimitiveDataTypeContext extends DataTypeContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public List<TerminalNode> INTEGER_VALUE() { return getTokens(SqlHiveParser.INTEGER_VALUE); }
		public TerminalNode INTEGER_VALUE(int i) {
			return getToken(SqlHiveParser.INTEGER_VALUE, i);
		}
		public PrimitiveDataTypeContext(DataTypeContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterPrimitiveDataType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitPrimitiveDataType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitPrimitiveDataType(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DataTypeContext dataType() throws RecognitionException {
		DataTypeContext _localctx = new DataTypeContext(_ctx, getState());
		enterRule(_localctx, 176, RULE_dataType);
		int _la;
		try {
			setState(2298);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,302,_ctx) ) {
			case 1:
				_localctx = new ComplexDataTypeContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(2264);
				((ComplexDataTypeContext)_localctx).complex = match(ARRAY);
				setState(2265);
				match(LT);
				setState(2266);
				dataType();
				setState(2267);
				match(GT);
				}
				break;
			case 2:
				_localctx = new ComplexDataTypeContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(2269);
				((ComplexDataTypeContext)_localctx).complex = match(MAP);
				setState(2270);
				match(LT);
				setState(2271);
				dataType();
				setState(2272);
				match(T__2);
				setState(2273);
				dataType();
				setState(2274);
				match(GT);
				}
				break;
			case 3:
				_localctx = new ComplexDataTypeContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(2276);
				((ComplexDataTypeContext)_localctx).complex = match(STRUCT);
				setState(2283);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case LT:
					{
					setState(2277);
					match(LT);
					setState(2279);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << SELECT) | (1L << FROM) | (1L << ADD) | (1L << AS) | (1L << ALL) | (1L << ANY) | (1L << DISTINCT) | (1L << WHERE) | (1L << GROUP) | (1L << BY) | (1L << GROUPING) | (1L << SETS) | (1L << CUBE) | (1L << ROLLUP) | (1L << ORDER) | (1L << HAVING) | (1L << LIMIT) | (1L << AT) | (1L << OR) | (1L << AND) | (1L << IN) | (1L << NOT) | (1L << NO) | (1L << EXISTS) | (1L << BETWEEN) | (1L << LIKE) | (1L << RLIKE) | (1L << IS) | (1L << NULL) | (1L << TRUE) | (1L << FALSE) | (1L << NULLS) | (1L << ASC) | (1L << DESC) | (1L << FOR) | (1L << INTERVAL) | (1L << CASE) | (1L << WHEN) | (1L << THEN) | (1L << ELSE) | (1L << END) | (1L << JOIN) | (1L << CROSS) | (1L << OUTER) | (1L << INNER) | (1L << LEFT) | (1L << SEMI) | (1L << RIGHT) | (1L << FULL) | (1L << NATURAL) | (1L << ON) | (1L << PIVOT) | (1L << LATERAL))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (WINDOW - 64)) | (1L << (OVER - 64)) | (1L << (PARTITION - 64)) | (1L << (RANGE - 64)) | (1L << (ROWS - 64)) | (1L << (UNBOUNDED - 64)) | (1L << (PRECEDING - 64)) | (1L << (FOLLOWING - 64)) | (1L << (CURRENT - 64)) | (1L << (FIRST - 64)) | (1L << (AFTER - 64)) | (1L << (LAST - 64)) | (1L << (ROW - 64)) | (1L << (WITH - 64)) | (1L << (VALUES - 64)) | (1L << (CREATE - 64)) | (1L << (TABLE - 64)) | (1L << (DIRECTORY - 64)) | (1L << (VIEW - 64)) | (1L << (REPLACE - 64)) | (1L << (INSERT - 64)) | (1L << (UPDATE - 64)) | (1L << (DELETE - 64)) | (1L << (INTO - 64)) | (1L << (DESCRIBE - 64)) | (1L << (EXPLAIN - 64)) | (1L << (FORMAT - 64)) | (1L << (LOGICAL - 64)) | (1L << (CODEGEN - 64)) | (1L << (COST - 64)) | (1L << (CAST - 64)) | (1L << (SHOW - 64)) | (1L << (TABLES - 64)) | (1L << (COLUMNS - 64)) | (1L << (COLUMN - 64)) | (1L << (USE - 64)) | (1L << (PARTITIONS - 64)) | (1L << (FUNCTIONS - 64)) | (1L << (DROP - 64)) | (1L << (UNION - 64)) | (1L << (EXCEPT - 64)) | (1L << (SETMINUS - 64)) | (1L << (INTERSECT - 64)) | (1L << (TO - 64)) | (1L << (TABLESAMPLE - 64)) | (1L << (STRATIFY - 64)) | (1L << (ALTER - 64)) | (1L << (RENAME - 64)) | (1L << (ARRAY - 64)) | (1L << (MAP - 64)) | (1L << (STRUCT - 64)) | (1L << (COMMENT - 64)) | (1L << (SET - 64)) | (1L << (RESET - 64)) | (1L << (DATA - 64)) | (1L << (START - 64)) | (1L << (TRANSACTION - 64)) | (1L << (COMMIT - 64)) | (1L << (ROLLBACK - 64)) | (1L << (MACRO - 64)) | (1L << (IGNORE - 64)) | (1L << (BOTH - 64)) | (1L << (LEADING - 64)) | (1L << (TRAILING - 64)))) != 0) || ((((_la - 128)) & ~0x3f) == 0 && ((1L << (_la - 128)) & ((1L << (IF - 128)) | (1L << (POSITION - 128)) | (1L << (EXTRACT - 128)) | (1L << (DIV - 128)) | (1L << (PERCENTLIT - 128)) | (1L << (BUCKET - 128)) | (1L << (OUT - 128)) | (1L << (OF - 128)) | (1L << (SORT - 128)) | (1L << (CLUSTER - 128)) | (1L << (DISTRIBUTE - 128)) | (1L << (OVERWRITE - 128)) | (1L << (TRANSFORM - 128)) | (1L << (REDUCE - 128)) | (1L << (SERDE - 128)) | (1L << (SERDEPROPERTIES - 128)) | (1L << (RECORDREADER - 128)) | (1L << (RECORDWRITER - 128)) | (1L << (DELIMITED - 128)) | (1L << (FIELDS - 128)) | (1L << (TERMINATED - 128)) | (1L << (COLLECTION - 128)) | (1L << (ITEMS - 128)) | (1L << (KEYS - 128)) | (1L << (ESCAPED - 128)) | (1L << (LINES - 128)) | (1L << (SEPARATED - 128)) | (1L << (FUNCTION - 128)) | (1L << (EXTENDED - 128)) | (1L << (REFRESH - 128)) | (1L << (CLEAR - 128)) | (1L << (CACHE - 128)) | (1L << (UNCACHE - 128)) | (1L << (LAZY - 128)) | (1L << (FORMATTED - 128)) | (1L << (GLOBAL - 128)) | (1L << (TEMPORARY - 128)) | (1L << (OPTIONS - 128)) | (1L << (UNSET - 128)) | (1L << (TBLPROPERTIES - 128)) | (1L << (DBPROPERTIES - 128)) | (1L << (BUCKETS - 128)) | (1L << (SKEWED - 128)) | (1L << (STORED - 128)) | (1L << (DIRECTORIES - 128)))) != 0) || ((((_la - 192)) & ~0x3f) == 0 && ((1L << (_la - 192)) & ((1L << (LOCATION - 192)) | (1L << (EXCHANGE - 192)) | (1L << (ARCHIVE - 192)) | (1L << (UNARCHIVE - 192)) | (1L << (FILEFORMAT - 192)) | (1L << (TOUCH - 192)) | (1L << (COMPACT - 192)) | (1L << (CONCATENATE - 192)) | (1L << (CHANGE - 192)) | (1L << (CASCADE - 192)) | (1L << (RESTRICT - 192)) | (1L << (CLUSTERED - 192)) | (1L << (SORTED - 192)) | (1L << (PURGE - 192)) | (1L << (INPUTFORMAT - 192)) | (1L << (OUTPUTFORMAT - 192)) | (1L << (DATABASE - 192)) | (1L << (DATABASES - 192)) | (1L << (DFS - 192)) | (1L << (TRUNCATE - 192)) | (1L << (ANALYZE - 192)) | (1L << (COMPUTE - 192)) | (1L << (LIST - 192)) | (1L << (STATISTICS - 192)) | (1L << (PARTITIONED - 192)) | (1L << (EXTERNAL - 192)) | (1L << (DEFINED - 192)) | (1L << (REVOKE - 192)) | (1L << (GRANT - 192)) | (1L << (LOCK - 192)) | (1L << (UNLOCK - 192)) | (1L << (MSCK - 192)) | (1L << (REPAIR - 192)) | (1L << (RECOVER - 192)) | (1L << (EXPORT - 192)) | (1L << (IMPORT - 192)) | (1L << (LOAD - 192)) | (1L << (ROLE - 192)) | (1L << (ROLES - 192)) | (1L << (COMPACTIONS - 192)) | (1L << (PRINCIPALS - 192)) | (1L << (TRANSACTIONS - 192)) | (1L << (INDEX - 192)) | (1L << (INDEXES - 192)) | (1L << (LOCKS - 192)) | (1L << (OPTION - 192)) | (1L << (ANTI - 192)) | (1L << (LOCAL - 192)) | (1L << (INPATH - 192)) | (1L << (IDENTIFIER - 192)) | (1L << (BACKQUOTED_IDENTIFIER - 192)))) != 0)) {
						{
						setState(2278);
						complexColTypeList();
						}
					}

					setState(2281);
					match(GT);
					}
					break;
				case NEQ:
					{
					setState(2282);
					match(NEQ);
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				}
				break;
			case 4:
				_localctx = new PrimitiveDataTypeContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(2285);
				identifier();
				setState(2296);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,301,_ctx) ) {
				case 1:
					{
					setState(2286);
					match(T__0);
					setState(2287);
					match(INTEGER_VALUE);
					setState(2292);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__2) {
						{
						{
						setState(2288);
						match(T__2);
						setState(2289);
						match(INTEGER_VALUE);
						}
						}
						setState(2294);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					setState(2295);
					match(T__1);
					}
					break;
				}
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ColTypeListContext extends ParserRuleContext {
		public List<ColTypeContext> colType() {
			return getRuleContexts(ColTypeContext.class);
		}
		public ColTypeContext colType(int i) {
			return getRuleContext(ColTypeContext.class,i);
		}
		public ColTypeListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_colTypeList; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterColTypeList(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitColTypeList(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitColTypeList(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ColTypeListContext colTypeList() throws RecognitionException {
		ColTypeListContext _localctx = new ColTypeListContext(_ctx, getState());
		enterRule(_localctx, 178, RULE_colTypeList);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(2300);
			colType();
			setState(2305);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,303,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(2301);
					match(T__2);
					setState(2302);
					colType();
					}
					} 
				}
				setState(2307);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,303,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ColTypeContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public DataTypeContext dataType() {
			return getRuleContext(DataTypeContext.class,0);
		}
		public TerminalNode COMMENT() { return getToken(SqlHiveParser.COMMENT, 0); }
		public TerminalNode STRING() { return getToken(SqlHiveParser.STRING, 0); }
		public ColTypeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_colType; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterColType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitColType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitColType(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ColTypeContext colType() throws RecognitionException {
		ColTypeContext _localctx = new ColTypeContext(_ctx, getState());
		enterRule(_localctx, 180, RULE_colType);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2308);
			identifier();
			setState(2309);
			dataType();
			setState(2312);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,304,_ctx) ) {
			case 1:
				{
				setState(2310);
				match(COMMENT);
				setState(2311);
				match(STRING);
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ComplexColTypeListContext extends ParserRuleContext {
		public List<ComplexColTypeContext> complexColType() {
			return getRuleContexts(ComplexColTypeContext.class);
		}
		public ComplexColTypeContext complexColType(int i) {
			return getRuleContext(ComplexColTypeContext.class,i);
		}
		public ComplexColTypeListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_complexColTypeList; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterComplexColTypeList(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitComplexColTypeList(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitComplexColTypeList(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ComplexColTypeListContext complexColTypeList() throws RecognitionException {
		ComplexColTypeListContext _localctx = new ComplexColTypeListContext(_ctx, getState());
		enterRule(_localctx, 182, RULE_complexColTypeList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2314);
			complexColType();
			setState(2319);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__2) {
				{
				{
				setState(2315);
				match(T__2);
				setState(2316);
				complexColType();
				}
				}
				setState(2321);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ComplexColTypeContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public DataTypeContext dataType() {
			return getRuleContext(DataTypeContext.class,0);
		}
		public TerminalNode COMMENT() { return getToken(SqlHiveParser.COMMENT, 0); }
		public TerminalNode STRING() { return getToken(SqlHiveParser.STRING, 0); }
		public ComplexColTypeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_complexColType; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterComplexColType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitComplexColType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitComplexColType(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ComplexColTypeContext complexColType() throws RecognitionException {
		ComplexColTypeContext _localctx = new ComplexColTypeContext(_ctx, getState());
		enterRule(_localctx, 184, RULE_complexColType);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2322);
			identifier();
			setState(2323);
			match(T__9);
			setState(2324);
			dataType();
			setState(2327);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==COMMENT) {
				{
				setState(2325);
				match(COMMENT);
				setState(2326);
				match(STRING);
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class WhenClauseContext extends ParserRuleContext {
		public ExpressionContext condition;
		public ExpressionContext result;
		public TerminalNode WHEN() { return getToken(SqlHiveParser.WHEN, 0); }
		public TerminalNode THEN() { return getToken(SqlHiveParser.THEN, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public WhenClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_whenClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterWhenClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitWhenClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitWhenClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final WhenClauseContext whenClause() throws RecognitionException {
		WhenClauseContext _localctx = new WhenClauseContext(_ctx, getState());
		enterRule(_localctx, 186, RULE_whenClause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2329);
			match(WHEN);
			setState(2330);
			((WhenClauseContext)_localctx).condition = expression();
			setState(2331);
			match(THEN);
			setState(2332);
			((WhenClauseContext)_localctx).result = expression();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class WindowsContext extends ParserRuleContext {
		public TerminalNode WINDOW() { return getToken(SqlHiveParser.WINDOW, 0); }
		public List<NamedWindowContext> namedWindow() {
			return getRuleContexts(NamedWindowContext.class);
		}
		public NamedWindowContext namedWindow(int i) {
			return getRuleContext(NamedWindowContext.class,i);
		}
		public WindowsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_windows; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterWindows(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitWindows(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitWindows(this);
			else return visitor.visitChildren(this);
		}
	}

	public final WindowsContext windows() throws RecognitionException {
		WindowsContext _localctx = new WindowsContext(_ctx, getState());
		enterRule(_localctx, 188, RULE_windows);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(2334);
			match(WINDOW);
			setState(2335);
			namedWindow();
			setState(2340);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,307,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(2336);
					match(T__2);
					setState(2337);
					namedWindow();
					}
					} 
				}
				setState(2342);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,307,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class NamedWindowContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlHiveParser.AS, 0); }
		public WindowSpecContext windowSpec() {
			return getRuleContext(WindowSpecContext.class,0);
		}
		public NamedWindowContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_namedWindow; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterNamedWindow(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitNamedWindow(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitNamedWindow(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NamedWindowContext namedWindow() throws RecognitionException {
		NamedWindowContext _localctx = new NamedWindowContext(_ctx, getState());
		enterRule(_localctx, 190, RULE_namedWindow);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2343);
			identifier();
			setState(2344);
			match(AS);
			setState(2345);
			windowSpec();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class WindowSpecContext extends ParserRuleContext {
		public WindowSpecContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_windowSpec; }
	 
		public WindowSpecContext() { }
		public void copyFrom(WindowSpecContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class WindowRefContext extends WindowSpecContext {
		public IdentifierContext name;
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public WindowRefContext(WindowSpecContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterWindowRef(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitWindowRef(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitWindowRef(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class WindowDefContext extends WindowSpecContext {
		public ExpressionContext expression;
		public List<ExpressionContext> partition = new ArrayList<ExpressionContext>();
		public TerminalNode CLUSTER() { return getToken(SqlHiveParser.CLUSTER, 0); }
		public List<TerminalNode> BY() { return getTokens(SqlHiveParser.BY); }
		public TerminalNode BY(int i) {
			return getToken(SqlHiveParser.BY, i);
		}
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public WindowFrameContext windowFrame() {
			return getRuleContext(WindowFrameContext.class,0);
		}
		public List<SortItemContext> sortItem() {
			return getRuleContexts(SortItemContext.class);
		}
		public SortItemContext sortItem(int i) {
			return getRuleContext(SortItemContext.class,i);
		}
		public TerminalNode PARTITION() { return getToken(SqlHiveParser.PARTITION, 0); }
		public TerminalNode DISTRIBUTE() { return getToken(SqlHiveParser.DISTRIBUTE, 0); }
		public TerminalNode ORDER() { return getToken(SqlHiveParser.ORDER, 0); }
		public TerminalNode SORT() { return getToken(SqlHiveParser.SORT, 0); }
		public WindowDefContext(WindowSpecContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterWindowDef(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitWindowDef(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitWindowDef(this);
			else return visitor.visitChildren(this);
		}
	}

	public final WindowSpecContext windowSpec() throws RecognitionException {
		WindowSpecContext _localctx = new WindowSpecContext(_ctx, getState());
		enterRule(_localctx, 192, RULE_windowSpec);
		int _la;
		try {
			setState(2389);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case SELECT:
			case FROM:
			case ADD:
			case AS:
			case ALL:
			case ANY:
			case DISTINCT:
			case WHERE:
			case GROUP:
			case BY:
			case GROUPING:
			case SETS:
			case CUBE:
			case ROLLUP:
			case ORDER:
			case HAVING:
			case LIMIT:
			case AT:
			case OR:
			case AND:
			case IN:
			case NOT:
			case NO:
			case EXISTS:
			case BETWEEN:
			case LIKE:
			case RLIKE:
			case IS:
			case NULL:
			case TRUE:
			case FALSE:
			case NULLS:
			case ASC:
			case DESC:
			case FOR:
			case INTERVAL:
			case CASE:
			case WHEN:
			case THEN:
			case ELSE:
			case END:
			case JOIN:
			case CROSS:
			case OUTER:
			case INNER:
			case LEFT:
			case SEMI:
			case RIGHT:
			case FULL:
			case NATURAL:
			case ON:
			case PIVOT:
			case LATERAL:
			case WINDOW:
			case OVER:
			case PARTITION:
			case RANGE:
			case ROWS:
			case UNBOUNDED:
			case PRECEDING:
			case FOLLOWING:
			case CURRENT:
			case FIRST:
			case AFTER:
			case LAST:
			case ROW:
			case WITH:
			case VALUES:
			case CREATE:
			case TABLE:
			case DIRECTORY:
			case VIEW:
			case REPLACE:
			case INSERT:
			case UPDATE:
			case DELETE:
			case INTO:
			case DESCRIBE:
			case EXPLAIN:
			case FORMAT:
			case LOGICAL:
			case CODEGEN:
			case COST:
			case CAST:
			case SHOW:
			case TABLES:
			case COLUMNS:
			case COLUMN:
			case USE:
			case PARTITIONS:
			case FUNCTIONS:
			case DROP:
			case UNION:
			case EXCEPT:
			case SETMINUS:
			case INTERSECT:
			case TO:
			case TABLESAMPLE:
			case STRATIFY:
			case ALTER:
			case RENAME:
			case ARRAY:
			case MAP:
			case STRUCT:
			case COMMENT:
			case SET:
			case RESET:
			case DATA:
			case START:
			case TRANSACTION:
			case COMMIT:
			case ROLLBACK:
			case MACRO:
			case IGNORE:
			case BOTH:
			case LEADING:
			case TRAILING:
			case IF:
			case POSITION:
			case EXTRACT:
			case DIV:
			case PERCENTLIT:
			case BUCKET:
			case OUT:
			case OF:
			case SORT:
			case CLUSTER:
			case DISTRIBUTE:
			case OVERWRITE:
			case TRANSFORM:
			case REDUCE:
			case SERDE:
			case SERDEPROPERTIES:
			case RECORDREADER:
			case RECORDWRITER:
			case DELIMITED:
			case FIELDS:
			case TERMINATED:
			case COLLECTION:
			case ITEMS:
			case KEYS:
			case ESCAPED:
			case LINES:
			case SEPARATED:
			case FUNCTION:
			case EXTENDED:
			case REFRESH:
			case CLEAR:
			case CACHE:
			case UNCACHE:
			case LAZY:
			case FORMATTED:
			case GLOBAL:
			case TEMPORARY:
			case OPTIONS:
			case UNSET:
			case TBLPROPERTIES:
			case DBPROPERTIES:
			case BUCKETS:
			case SKEWED:
			case STORED:
			case DIRECTORIES:
			case LOCATION:
			case EXCHANGE:
			case ARCHIVE:
			case UNARCHIVE:
			case FILEFORMAT:
			case TOUCH:
			case COMPACT:
			case CONCATENATE:
			case CHANGE:
			case CASCADE:
			case RESTRICT:
			case CLUSTERED:
			case SORTED:
			case PURGE:
			case INPUTFORMAT:
			case OUTPUTFORMAT:
			case DATABASE:
			case DATABASES:
			case DFS:
			case TRUNCATE:
			case ANALYZE:
			case COMPUTE:
			case LIST:
			case STATISTICS:
			case PARTITIONED:
			case EXTERNAL:
			case DEFINED:
			case REVOKE:
			case GRANT:
			case LOCK:
			case UNLOCK:
			case MSCK:
			case REPAIR:
			case RECOVER:
			case EXPORT:
			case IMPORT:
			case LOAD:
			case ROLE:
			case ROLES:
			case COMPACTIONS:
			case PRINCIPALS:
			case TRANSACTIONS:
			case INDEX:
			case INDEXES:
			case LOCKS:
			case OPTION:
			case ANTI:
			case LOCAL:
			case INPATH:
			case IDENTIFIER:
			case BACKQUOTED_IDENTIFIER:
				_localctx = new WindowRefContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(2347);
				((WindowRefContext)_localctx).name = identifier();
				}
				break;
			case T__0:
				_localctx = new WindowDefContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(2348);
				match(T__0);
				setState(2383);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case CLUSTER:
					{
					setState(2349);
					match(CLUSTER);
					setState(2350);
					match(BY);
					setState(2351);
					((WindowDefContext)_localctx).expression = expression();
					((WindowDefContext)_localctx).partition.add(((WindowDefContext)_localctx).expression);
					setState(2356);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__2) {
						{
						{
						setState(2352);
						match(T__2);
						setState(2353);
						((WindowDefContext)_localctx).expression = expression();
						((WindowDefContext)_localctx).partition.add(((WindowDefContext)_localctx).expression);
						}
						}
						setState(2358);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
					break;
				case T__1:
				case ORDER:
				case PARTITION:
				case RANGE:
				case ROWS:
				case SORT:
				case DISTRIBUTE:
					{
					setState(2369);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==PARTITION || _la==DISTRIBUTE) {
						{
						setState(2359);
						_la = _input.LA(1);
						if ( !(_la==PARTITION || _la==DISTRIBUTE) ) {
						_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						setState(2360);
						match(BY);
						setState(2361);
						((WindowDefContext)_localctx).expression = expression();
						((WindowDefContext)_localctx).partition.add(((WindowDefContext)_localctx).expression);
						setState(2366);
						_errHandler.sync(this);
						_la = _input.LA(1);
						while (_la==T__2) {
							{
							{
							setState(2362);
							match(T__2);
							setState(2363);
							((WindowDefContext)_localctx).expression = expression();
							((WindowDefContext)_localctx).partition.add(((WindowDefContext)_localctx).expression);
							}
							}
							setState(2368);
							_errHandler.sync(this);
							_la = _input.LA(1);
						}
						}
					}

					setState(2381);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==ORDER || _la==SORT) {
						{
						setState(2371);
						_la = _input.LA(1);
						if ( !(_la==ORDER || _la==SORT) ) {
						_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						setState(2372);
						match(BY);
						setState(2373);
						sortItem();
						setState(2378);
						_errHandler.sync(this);
						_la = _input.LA(1);
						while (_la==T__2) {
							{
							{
							setState(2374);
							match(T__2);
							setState(2375);
							sortItem();
							}
							}
							setState(2380);
							_errHandler.sync(this);
							_la = _input.LA(1);
						}
						}
					}

					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				setState(2386);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==RANGE || _la==ROWS) {
					{
					setState(2385);
					windowFrame();
					}
				}

				setState(2388);
				match(T__1);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class WindowFrameContext extends ParserRuleContext {
		public Token frameType;
		public FrameBoundContext start;
		public FrameBoundContext end;
		public TerminalNode RANGE() { return getToken(SqlHiveParser.RANGE, 0); }
		public List<FrameBoundContext> frameBound() {
			return getRuleContexts(FrameBoundContext.class);
		}
		public FrameBoundContext frameBound(int i) {
			return getRuleContext(FrameBoundContext.class,i);
		}
		public TerminalNode ROWS() { return getToken(SqlHiveParser.ROWS, 0); }
		public TerminalNode BETWEEN() { return getToken(SqlHiveParser.BETWEEN, 0); }
		public TerminalNode AND() { return getToken(SqlHiveParser.AND, 0); }
		public WindowFrameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_windowFrame; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterWindowFrame(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitWindowFrame(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitWindowFrame(this);
			else return visitor.visitChildren(this);
		}
	}

	public final WindowFrameContext windowFrame() throws RecognitionException {
		WindowFrameContext _localctx = new WindowFrameContext(_ctx, getState());
		enterRule(_localctx, 194, RULE_windowFrame);
		try {
			setState(2407);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,316,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(2391);
				((WindowFrameContext)_localctx).frameType = match(RANGE);
				setState(2392);
				((WindowFrameContext)_localctx).start = frameBound();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(2393);
				((WindowFrameContext)_localctx).frameType = match(ROWS);
				setState(2394);
				((WindowFrameContext)_localctx).start = frameBound();
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(2395);
				((WindowFrameContext)_localctx).frameType = match(RANGE);
				setState(2396);
				match(BETWEEN);
				setState(2397);
				((WindowFrameContext)_localctx).start = frameBound();
				setState(2398);
				match(AND);
				setState(2399);
				((WindowFrameContext)_localctx).end = frameBound();
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(2401);
				((WindowFrameContext)_localctx).frameType = match(ROWS);
				setState(2402);
				match(BETWEEN);
				setState(2403);
				((WindowFrameContext)_localctx).start = frameBound();
				setState(2404);
				match(AND);
				setState(2405);
				((WindowFrameContext)_localctx).end = frameBound();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class FrameBoundContext extends ParserRuleContext {
		public Token boundType;
		public TerminalNode UNBOUNDED() { return getToken(SqlHiveParser.UNBOUNDED, 0); }
		public TerminalNode PRECEDING() { return getToken(SqlHiveParser.PRECEDING, 0); }
		public TerminalNode FOLLOWING() { return getToken(SqlHiveParser.FOLLOWING, 0); }
		public TerminalNode ROW() { return getToken(SqlHiveParser.ROW, 0); }
		public TerminalNode CURRENT() { return getToken(SqlHiveParser.CURRENT, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public FrameBoundContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_frameBound; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterFrameBound(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitFrameBound(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitFrameBound(this);
			else return visitor.visitChildren(this);
		}
	}

	public final FrameBoundContext frameBound() throws RecognitionException {
		FrameBoundContext _localctx = new FrameBoundContext(_ctx, getState());
		enterRule(_localctx, 196, RULE_frameBound);
		int _la;
		try {
			setState(2416);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,317,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(2409);
				match(UNBOUNDED);
				setState(2410);
				((FrameBoundContext)_localctx).boundType = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==PRECEDING || _la==FOLLOWING) ) {
					((FrameBoundContext)_localctx).boundType = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(2411);
				((FrameBoundContext)_localctx).boundType = match(CURRENT);
				setState(2412);
				match(ROW);
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(2413);
				expression();
				setState(2414);
				((FrameBoundContext)_localctx).boundType = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==PRECEDING || _la==FOLLOWING) ) {
					((FrameBoundContext)_localctx).boundType = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class QualifiedNameContext extends ParserRuleContext {
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public QualifiedNameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_qualifiedName; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterQualifiedName(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitQualifiedName(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitQualifiedName(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QualifiedNameContext qualifiedName() throws RecognitionException {
		QualifiedNameContext _localctx = new QualifiedNameContext(_ctx, getState());
		enterRule(_localctx, 198, RULE_qualifiedName);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(2418);
			identifier();
			setState(2423);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,318,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(2419);
					match(T__3);
					setState(2420);
					identifier();
					}
					} 
				}
				setState(2425);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,318,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class IdentifierContext extends ParserRuleContext {
		public StrictIdentifierContext strictIdentifier() {
			return getRuleContext(StrictIdentifierContext.class,0);
		}
		public TerminalNode ANTI() { return getToken(SqlHiveParser.ANTI, 0); }
		public TerminalNode FULL() { return getToken(SqlHiveParser.FULL, 0); }
		public TerminalNode INNER() { return getToken(SqlHiveParser.INNER, 0); }
		public TerminalNode LEFT() { return getToken(SqlHiveParser.LEFT, 0); }
		public TerminalNode SEMI() { return getToken(SqlHiveParser.SEMI, 0); }
		public TerminalNode RIGHT() { return getToken(SqlHiveParser.RIGHT, 0); }
		public TerminalNode NATURAL() { return getToken(SqlHiveParser.NATURAL, 0); }
		public TerminalNode JOIN() { return getToken(SqlHiveParser.JOIN, 0); }
		public TerminalNode CROSS() { return getToken(SqlHiveParser.CROSS, 0); }
		public TerminalNode ON() { return getToken(SqlHiveParser.ON, 0); }
		public TerminalNode UNION() { return getToken(SqlHiveParser.UNION, 0); }
		public TerminalNode INTERSECT() { return getToken(SqlHiveParser.INTERSECT, 0); }
		public TerminalNode EXCEPT() { return getToken(SqlHiveParser.EXCEPT, 0); }
		public TerminalNode SETMINUS() { return getToken(SqlHiveParser.SETMINUS, 0); }
		public IdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_identifier; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitIdentifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitIdentifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final IdentifierContext identifier() throws RecognitionException {
		IdentifierContext _localctx = new IdentifierContext(_ctx, getState());
		enterRule(_localctx, 200, RULE_identifier);
		try {
			setState(2441);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case SELECT:
			case FROM:
			case ADD:
			case AS:
			case ALL:
			case ANY:
			case DISTINCT:
			case WHERE:
			case GROUP:
			case BY:
			case GROUPING:
			case SETS:
			case CUBE:
			case ROLLUP:
			case ORDER:
			case HAVING:
			case LIMIT:
			case AT:
			case OR:
			case AND:
			case IN:
			case NOT:
			case NO:
			case EXISTS:
			case BETWEEN:
			case LIKE:
			case RLIKE:
			case IS:
			case NULL:
			case TRUE:
			case FALSE:
			case NULLS:
			case ASC:
			case DESC:
			case FOR:
			case INTERVAL:
			case CASE:
			case WHEN:
			case THEN:
			case ELSE:
			case END:
			case OUTER:
			case PIVOT:
			case LATERAL:
			case WINDOW:
			case OVER:
			case PARTITION:
			case RANGE:
			case ROWS:
			case UNBOUNDED:
			case PRECEDING:
			case FOLLOWING:
			case CURRENT:
			case FIRST:
			case AFTER:
			case LAST:
			case ROW:
			case WITH:
			case VALUES:
			case CREATE:
			case TABLE:
			case DIRECTORY:
			case VIEW:
			case REPLACE:
			case INSERT:
			case UPDATE:
			case DELETE:
			case INTO:
			case DESCRIBE:
			case EXPLAIN:
			case FORMAT:
			case LOGICAL:
			case CODEGEN:
			case COST:
			case CAST:
			case SHOW:
			case TABLES:
			case COLUMNS:
			case COLUMN:
			case USE:
			case PARTITIONS:
			case FUNCTIONS:
			case DROP:
			case TO:
			case TABLESAMPLE:
			case STRATIFY:
			case ALTER:
			case RENAME:
			case ARRAY:
			case MAP:
			case STRUCT:
			case COMMENT:
			case SET:
			case RESET:
			case DATA:
			case START:
			case TRANSACTION:
			case COMMIT:
			case ROLLBACK:
			case MACRO:
			case IGNORE:
			case BOTH:
			case LEADING:
			case TRAILING:
			case IF:
			case POSITION:
			case EXTRACT:
			case DIV:
			case PERCENTLIT:
			case BUCKET:
			case OUT:
			case OF:
			case SORT:
			case CLUSTER:
			case DISTRIBUTE:
			case OVERWRITE:
			case TRANSFORM:
			case REDUCE:
			case SERDE:
			case SERDEPROPERTIES:
			case RECORDREADER:
			case RECORDWRITER:
			case DELIMITED:
			case FIELDS:
			case TERMINATED:
			case COLLECTION:
			case ITEMS:
			case KEYS:
			case ESCAPED:
			case LINES:
			case SEPARATED:
			case FUNCTION:
			case EXTENDED:
			case REFRESH:
			case CLEAR:
			case CACHE:
			case UNCACHE:
			case LAZY:
			case FORMATTED:
			case GLOBAL:
			case TEMPORARY:
			case OPTIONS:
			case UNSET:
			case TBLPROPERTIES:
			case DBPROPERTIES:
			case BUCKETS:
			case SKEWED:
			case STORED:
			case DIRECTORIES:
			case LOCATION:
			case EXCHANGE:
			case ARCHIVE:
			case UNARCHIVE:
			case FILEFORMAT:
			case TOUCH:
			case COMPACT:
			case CONCATENATE:
			case CHANGE:
			case CASCADE:
			case RESTRICT:
			case CLUSTERED:
			case SORTED:
			case PURGE:
			case INPUTFORMAT:
			case OUTPUTFORMAT:
			case DATABASE:
			case DATABASES:
			case DFS:
			case TRUNCATE:
			case ANALYZE:
			case COMPUTE:
			case LIST:
			case STATISTICS:
			case PARTITIONED:
			case EXTERNAL:
			case DEFINED:
			case REVOKE:
			case GRANT:
			case LOCK:
			case UNLOCK:
			case MSCK:
			case REPAIR:
			case RECOVER:
			case EXPORT:
			case IMPORT:
			case LOAD:
			case ROLE:
			case ROLES:
			case COMPACTIONS:
			case PRINCIPALS:
			case TRANSACTIONS:
			case INDEX:
			case INDEXES:
			case LOCKS:
			case OPTION:
			case LOCAL:
			case INPATH:
			case IDENTIFIER:
			case BACKQUOTED_IDENTIFIER:
				enterOuterAlt(_localctx, 1);
				{
				setState(2426);
				strictIdentifier();
				}
				break;
			case ANTI:
				enterOuterAlt(_localctx, 2);
				{
				setState(2427);
				match(ANTI);
				}
				break;
			case FULL:
				enterOuterAlt(_localctx, 3);
				{
				setState(2428);
				match(FULL);
				}
				break;
			case INNER:
				enterOuterAlt(_localctx, 4);
				{
				setState(2429);
				match(INNER);
				}
				break;
			case LEFT:
				enterOuterAlt(_localctx, 5);
				{
				setState(2430);
				match(LEFT);
				}
				break;
			case SEMI:
				enterOuterAlt(_localctx, 6);
				{
				setState(2431);
				match(SEMI);
				}
				break;
			case RIGHT:
				enterOuterAlt(_localctx, 7);
				{
				setState(2432);
				match(RIGHT);
				}
				break;
			case NATURAL:
				enterOuterAlt(_localctx, 8);
				{
				setState(2433);
				match(NATURAL);
				}
				break;
			case JOIN:
				enterOuterAlt(_localctx, 9);
				{
				setState(2434);
				match(JOIN);
				}
				break;
			case CROSS:
				enterOuterAlt(_localctx, 10);
				{
				setState(2435);
				match(CROSS);
				}
				break;
			case ON:
				enterOuterAlt(_localctx, 11);
				{
				setState(2436);
				match(ON);
				}
				break;
			case UNION:
				enterOuterAlt(_localctx, 12);
				{
				setState(2437);
				match(UNION);
				}
				break;
			case INTERSECT:
				enterOuterAlt(_localctx, 13);
				{
				setState(2438);
				match(INTERSECT);
				}
				break;
			case EXCEPT:
				enterOuterAlt(_localctx, 14);
				{
				setState(2439);
				match(EXCEPT);
				}
				break;
			case SETMINUS:
				enterOuterAlt(_localctx, 15);
				{
				setState(2440);
				match(SETMINUS);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class StrictIdentifierContext extends ParserRuleContext {
		public StrictIdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_strictIdentifier; }
	 
		public StrictIdentifierContext() { }
		public void copyFrom(StrictIdentifierContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class QuotedIdentifierAlternativeContext extends StrictIdentifierContext {
		public QuotedIdentifierContext quotedIdentifier() {
			return getRuleContext(QuotedIdentifierContext.class,0);
		}
		public QuotedIdentifierAlternativeContext(StrictIdentifierContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterQuotedIdentifierAlternative(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitQuotedIdentifierAlternative(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitQuotedIdentifierAlternative(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class UnquotedIdentifierContext extends StrictIdentifierContext {
		public TerminalNode IDENTIFIER() { return getToken(SqlHiveParser.IDENTIFIER, 0); }
		public NonReservedContext nonReserved() {
			return getRuleContext(NonReservedContext.class,0);
		}
		public UnquotedIdentifierContext(StrictIdentifierContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterUnquotedIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitUnquotedIdentifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitUnquotedIdentifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final StrictIdentifierContext strictIdentifier() throws RecognitionException {
		StrictIdentifierContext _localctx = new StrictIdentifierContext(_ctx, getState());
		enterRule(_localctx, 202, RULE_strictIdentifier);
		try {
			setState(2446);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case IDENTIFIER:
				_localctx = new UnquotedIdentifierContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(2443);
				match(IDENTIFIER);
				}
				break;
			case BACKQUOTED_IDENTIFIER:
				_localctx = new QuotedIdentifierAlternativeContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(2444);
				quotedIdentifier();
				}
				break;
			case SELECT:
			case FROM:
			case ADD:
			case AS:
			case ALL:
			case ANY:
			case DISTINCT:
			case WHERE:
			case GROUP:
			case BY:
			case GROUPING:
			case SETS:
			case CUBE:
			case ROLLUP:
			case ORDER:
			case HAVING:
			case LIMIT:
			case AT:
			case OR:
			case AND:
			case IN:
			case NOT:
			case NO:
			case EXISTS:
			case BETWEEN:
			case LIKE:
			case RLIKE:
			case IS:
			case NULL:
			case TRUE:
			case FALSE:
			case NULLS:
			case ASC:
			case DESC:
			case FOR:
			case INTERVAL:
			case CASE:
			case WHEN:
			case THEN:
			case ELSE:
			case END:
			case OUTER:
			case PIVOT:
			case LATERAL:
			case WINDOW:
			case OVER:
			case PARTITION:
			case RANGE:
			case ROWS:
			case UNBOUNDED:
			case PRECEDING:
			case FOLLOWING:
			case CURRENT:
			case FIRST:
			case AFTER:
			case LAST:
			case ROW:
			case WITH:
			case VALUES:
			case CREATE:
			case TABLE:
			case DIRECTORY:
			case VIEW:
			case REPLACE:
			case INSERT:
			case UPDATE:
			case DELETE:
			case INTO:
			case DESCRIBE:
			case EXPLAIN:
			case FORMAT:
			case LOGICAL:
			case CODEGEN:
			case COST:
			case CAST:
			case SHOW:
			case TABLES:
			case COLUMNS:
			case COLUMN:
			case USE:
			case PARTITIONS:
			case FUNCTIONS:
			case DROP:
			case TO:
			case TABLESAMPLE:
			case STRATIFY:
			case ALTER:
			case RENAME:
			case ARRAY:
			case MAP:
			case STRUCT:
			case COMMENT:
			case SET:
			case RESET:
			case DATA:
			case START:
			case TRANSACTION:
			case COMMIT:
			case ROLLBACK:
			case MACRO:
			case IGNORE:
			case BOTH:
			case LEADING:
			case TRAILING:
			case IF:
			case POSITION:
			case EXTRACT:
			case DIV:
			case PERCENTLIT:
			case BUCKET:
			case OUT:
			case OF:
			case SORT:
			case CLUSTER:
			case DISTRIBUTE:
			case OVERWRITE:
			case TRANSFORM:
			case REDUCE:
			case SERDE:
			case SERDEPROPERTIES:
			case RECORDREADER:
			case RECORDWRITER:
			case DELIMITED:
			case FIELDS:
			case TERMINATED:
			case COLLECTION:
			case ITEMS:
			case KEYS:
			case ESCAPED:
			case LINES:
			case SEPARATED:
			case FUNCTION:
			case EXTENDED:
			case REFRESH:
			case CLEAR:
			case CACHE:
			case UNCACHE:
			case LAZY:
			case FORMATTED:
			case GLOBAL:
			case TEMPORARY:
			case OPTIONS:
			case UNSET:
			case TBLPROPERTIES:
			case DBPROPERTIES:
			case BUCKETS:
			case SKEWED:
			case STORED:
			case DIRECTORIES:
			case LOCATION:
			case EXCHANGE:
			case ARCHIVE:
			case UNARCHIVE:
			case FILEFORMAT:
			case TOUCH:
			case COMPACT:
			case CONCATENATE:
			case CHANGE:
			case CASCADE:
			case RESTRICT:
			case CLUSTERED:
			case SORTED:
			case PURGE:
			case INPUTFORMAT:
			case OUTPUTFORMAT:
			case DATABASE:
			case DATABASES:
			case DFS:
			case TRUNCATE:
			case ANALYZE:
			case COMPUTE:
			case LIST:
			case STATISTICS:
			case PARTITIONED:
			case EXTERNAL:
			case DEFINED:
			case REVOKE:
			case GRANT:
			case LOCK:
			case UNLOCK:
			case MSCK:
			case REPAIR:
			case RECOVER:
			case EXPORT:
			case IMPORT:
			case LOAD:
			case ROLE:
			case ROLES:
			case COMPACTIONS:
			case PRINCIPALS:
			case TRANSACTIONS:
			case INDEX:
			case INDEXES:
			case LOCKS:
			case OPTION:
			case LOCAL:
			case INPATH:
				_localctx = new UnquotedIdentifierContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(2445);
				nonReserved();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class QuotedIdentifierContext extends ParserRuleContext {
		public TerminalNode BACKQUOTED_IDENTIFIER() { return getToken(SqlHiveParser.BACKQUOTED_IDENTIFIER, 0); }
		public QuotedIdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_quotedIdentifier; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterQuotedIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitQuotedIdentifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitQuotedIdentifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QuotedIdentifierContext quotedIdentifier() throws RecognitionException {
		QuotedIdentifierContext _localctx = new QuotedIdentifierContext(_ctx, getState());
		enterRule(_localctx, 204, RULE_quotedIdentifier);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2448);
			match(BACKQUOTED_IDENTIFIER);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class NumberContext extends ParserRuleContext {
		public NumberContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_number; }
	 
		public NumberContext() { }
		public void copyFrom(NumberContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class DecimalLiteralContext extends NumberContext {
		public TerminalNode DECIMAL_VALUE() { return getToken(SqlHiveParser.DECIMAL_VALUE, 0); }
		public TerminalNode MINUS() { return getToken(SqlHiveParser.MINUS, 0); }
		public DecimalLiteralContext(NumberContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterDecimalLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitDecimalLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitDecimalLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class BigIntLiteralContext extends NumberContext {
		public TerminalNode BIGINT_LITERAL() { return getToken(SqlHiveParser.BIGINT_LITERAL, 0); }
		public TerminalNode MINUS() { return getToken(SqlHiveParser.MINUS, 0); }
		public BigIntLiteralContext(NumberContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterBigIntLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitBigIntLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitBigIntLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class TinyIntLiteralContext extends NumberContext {
		public TerminalNode TINYINT_LITERAL() { return getToken(SqlHiveParser.TINYINT_LITERAL, 0); }
		public TerminalNode MINUS() { return getToken(SqlHiveParser.MINUS, 0); }
		public TinyIntLiteralContext(NumberContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterTinyIntLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitTinyIntLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitTinyIntLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class BigDecimalLiteralContext extends NumberContext {
		public TerminalNode BIGDECIMAL_LITERAL() { return getToken(SqlHiveParser.BIGDECIMAL_LITERAL, 0); }
		public TerminalNode MINUS() { return getToken(SqlHiveParser.MINUS, 0); }
		public BigDecimalLiteralContext(NumberContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterBigDecimalLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitBigDecimalLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitBigDecimalLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class DoubleLiteralContext extends NumberContext {
		public TerminalNode DOUBLE_LITERAL() { return getToken(SqlHiveParser.DOUBLE_LITERAL, 0); }
		public TerminalNode MINUS() { return getToken(SqlHiveParser.MINUS, 0); }
		public DoubleLiteralContext(NumberContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterDoubleLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitDoubleLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitDoubleLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class IntegerLiteralContext extends NumberContext {
		public TerminalNode INTEGER_VALUE() { return getToken(SqlHiveParser.INTEGER_VALUE, 0); }
		public TerminalNode MINUS() { return getToken(SqlHiveParser.MINUS, 0); }
		public IntegerLiteralContext(NumberContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterIntegerLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitIntegerLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitIntegerLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class SmallIntLiteralContext extends NumberContext {
		public TerminalNode SMALLINT_LITERAL() { return getToken(SqlHiveParser.SMALLINT_LITERAL, 0); }
		public TerminalNode MINUS() { return getToken(SqlHiveParser.MINUS, 0); }
		public SmallIntLiteralContext(NumberContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterSmallIntLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitSmallIntLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitSmallIntLiteral(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NumberContext number() throws RecognitionException {
		NumberContext _localctx = new NumberContext(_ctx, getState());
		enterRule(_localctx, 206, RULE_number);
		int _la;
		try {
			setState(2478);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,328,_ctx) ) {
			case 1:
				_localctx = new DecimalLiteralContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(2451);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(2450);
					match(MINUS);
					}
				}

				setState(2453);
				match(DECIMAL_VALUE);
				}
				break;
			case 2:
				_localctx = new IntegerLiteralContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(2455);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(2454);
					match(MINUS);
					}
				}

				setState(2457);
				match(INTEGER_VALUE);
				}
				break;
			case 3:
				_localctx = new BigIntLiteralContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(2459);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(2458);
					match(MINUS);
					}
				}

				setState(2461);
				match(BIGINT_LITERAL);
				}
				break;
			case 4:
				_localctx = new SmallIntLiteralContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(2463);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(2462);
					match(MINUS);
					}
				}

				setState(2465);
				match(SMALLINT_LITERAL);
				}
				break;
			case 5:
				_localctx = new TinyIntLiteralContext(_localctx);
				enterOuterAlt(_localctx, 5);
				{
				setState(2467);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(2466);
					match(MINUS);
					}
				}

				setState(2469);
				match(TINYINT_LITERAL);
				}
				break;
			case 6:
				_localctx = new DoubleLiteralContext(_localctx);
				enterOuterAlt(_localctx, 6);
				{
				setState(2471);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(2470);
					match(MINUS);
					}
				}

				setState(2473);
				match(DOUBLE_LITERAL);
				}
				break;
			case 7:
				_localctx = new BigDecimalLiteralContext(_localctx);
				enterOuterAlt(_localctx, 7);
				{
				setState(2475);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(2474);
					match(MINUS);
					}
				}

				setState(2477);
				match(BIGDECIMAL_LITERAL);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class NonReservedContext extends ParserRuleContext {
		public TerminalNode SHOW() { return getToken(SqlHiveParser.SHOW, 0); }
		public TerminalNode TABLES() { return getToken(SqlHiveParser.TABLES, 0); }
		public TerminalNode COLUMNS() { return getToken(SqlHiveParser.COLUMNS, 0); }
		public TerminalNode COLUMN() { return getToken(SqlHiveParser.COLUMN, 0); }
		public TerminalNode PARTITIONS() { return getToken(SqlHiveParser.PARTITIONS, 0); }
		public TerminalNode FUNCTIONS() { return getToken(SqlHiveParser.FUNCTIONS, 0); }
		public TerminalNode DATABASES() { return getToken(SqlHiveParser.DATABASES, 0); }
		public TerminalNode ADD() { return getToken(SqlHiveParser.ADD, 0); }
		public TerminalNode OVER() { return getToken(SqlHiveParser.OVER, 0); }
		public TerminalNode PARTITION() { return getToken(SqlHiveParser.PARTITION, 0); }
		public TerminalNode RANGE() { return getToken(SqlHiveParser.RANGE, 0); }
		public TerminalNode ROWS() { return getToken(SqlHiveParser.ROWS, 0); }
		public TerminalNode PRECEDING() { return getToken(SqlHiveParser.PRECEDING, 0); }
		public TerminalNode FOLLOWING() { return getToken(SqlHiveParser.FOLLOWING, 0); }
		public TerminalNode CURRENT() { return getToken(SqlHiveParser.CURRENT, 0); }
		public TerminalNode ROW() { return getToken(SqlHiveParser.ROW, 0); }
		public TerminalNode LAST() { return getToken(SqlHiveParser.LAST, 0); }
		public TerminalNode FIRST() { return getToken(SqlHiveParser.FIRST, 0); }
		public TerminalNode AFTER() { return getToken(SqlHiveParser.AFTER, 0); }
		public TerminalNode MAP() { return getToken(SqlHiveParser.MAP, 0); }
		public TerminalNode ARRAY() { return getToken(SqlHiveParser.ARRAY, 0); }
		public TerminalNode STRUCT() { return getToken(SqlHiveParser.STRUCT, 0); }
		public TerminalNode PIVOT() { return getToken(SqlHiveParser.PIVOT, 0); }
		public TerminalNode LATERAL() { return getToken(SqlHiveParser.LATERAL, 0); }
		public TerminalNode WINDOW() { return getToken(SqlHiveParser.WINDOW, 0); }
		public TerminalNode REDUCE() { return getToken(SqlHiveParser.REDUCE, 0); }
		public TerminalNode TRANSFORM() { return getToken(SqlHiveParser.TRANSFORM, 0); }
		public TerminalNode SERDE() { return getToken(SqlHiveParser.SERDE, 0); }
		public TerminalNode SERDEPROPERTIES() { return getToken(SqlHiveParser.SERDEPROPERTIES, 0); }
		public TerminalNode RECORDREADER() { return getToken(SqlHiveParser.RECORDREADER, 0); }
		public TerminalNode DELIMITED() { return getToken(SqlHiveParser.DELIMITED, 0); }
		public TerminalNode FIELDS() { return getToken(SqlHiveParser.FIELDS, 0); }
		public TerminalNode TERMINATED() { return getToken(SqlHiveParser.TERMINATED, 0); }
		public TerminalNode COLLECTION() { return getToken(SqlHiveParser.COLLECTION, 0); }
		public TerminalNode ITEMS() { return getToken(SqlHiveParser.ITEMS, 0); }
		public TerminalNode KEYS() { return getToken(SqlHiveParser.KEYS, 0); }
		public TerminalNode ESCAPED() { return getToken(SqlHiveParser.ESCAPED, 0); }
		public TerminalNode LINES() { return getToken(SqlHiveParser.LINES, 0); }
		public TerminalNode SEPARATED() { return getToken(SqlHiveParser.SEPARATED, 0); }
		public TerminalNode EXTENDED() { return getToken(SqlHiveParser.EXTENDED, 0); }
		public TerminalNode REFRESH() { return getToken(SqlHiveParser.REFRESH, 0); }
		public TerminalNode CLEAR() { return getToken(SqlHiveParser.CLEAR, 0); }
		public TerminalNode CACHE() { return getToken(SqlHiveParser.CACHE, 0); }
		public TerminalNode UNCACHE() { return getToken(SqlHiveParser.UNCACHE, 0); }
		public TerminalNode LAZY() { return getToken(SqlHiveParser.LAZY, 0); }
		public TerminalNode GLOBAL() { return getToken(SqlHiveParser.GLOBAL, 0); }
		public TerminalNode TEMPORARY() { return getToken(SqlHiveParser.TEMPORARY, 0); }
		public TerminalNode OPTIONS() { return getToken(SqlHiveParser.OPTIONS, 0); }
		public TerminalNode GROUPING() { return getToken(SqlHiveParser.GROUPING, 0); }
		public TerminalNode CUBE() { return getToken(SqlHiveParser.CUBE, 0); }
		public TerminalNode ROLLUP() { return getToken(SqlHiveParser.ROLLUP, 0); }
		public TerminalNode EXPLAIN() { return getToken(SqlHiveParser.EXPLAIN, 0); }
		public TerminalNode FORMAT() { return getToken(SqlHiveParser.FORMAT, 0); }
		public TerminalNode LOGICAL() { return getToken(SqlHiveParser.LOGICAL, 0); }
		public TerminalNode FORMATTED() { return getToken(SqlHiveParser.FORMATTED, 0); }
		public TerminalNode CODEGEN() { return getToken(SqlHiveParser.CODEGEN, 0); }
		public TerminalNode COST() { return getToken(SqlHiveParser.COST, 0); }
		public TerminalNode TABLESAMPLE() { return getToken(SqlHiveParser.TABLESAMPLE, 0); }
		public TerminalNode USE() { return getToken(SqlHiveParser.USE, 0); }
		public TerminalNode TO() { return getToken(SqlHiveParser.TO, 0); }
		public TerminalNode BUCKET() { return getToken(SqlHiveParser.BUCKET, 0); }
		public TerminalNode PERCENTLIT() { return getToken(SqlHiveParser.PERCENTLIT, 0); }
		public TerminalNode OUT() { return getToken(SqlHiveParser.OUT, 0); }
		public TerminalNode OF() { return getToken(SqlHiveParser.OF, 0); }
		public TerminalNode SET() { return getToken(SqlHiveParser.SET, 0); }
		public TerminalNode RESET() { return getToken(SqlHiveParser.RESET, 0); }
		public TerminalNode VIEW() { return getToken(SqlHiveParser.VIEW, 0); }
		public TerminalNode REPLACE() { return getToken(SqlHiveParser.REPLACE, 0); }
		public TerminalNode IF() { return getToken(SqlHiveParser.IF, 0); }
		public TerminalNode POSITION() { return getToken(SqlHiveParser.POSITION, 0); }
		public TerminalNode EXTRACT() { return getToken(SqlHiveParser.EXTRACT, 0); }
		public TerminalNode NO() { return getToken(SqlHiveParser.NO, 0); }
		public TerminalNode DATA() { return getToken(SqlHiveParser.DATA, 0); }
		public TerminalNode START() { return getToken(SqlHiveParser.START, 0); }
		public TerminalNode TRANSACTION() { return getToken(SqlHiveParser.TRANSACTION, 0); }
		public TerminalNode COMMIT() { return getToken(SqlHiveParser.COMMIT, 0); }
		public TerminalNode ROLLBACK() { return getToken(SqlHiveParser.ROLLBACK, 0); }
		public TerminalNode IGNORE() { return getToken(SqlHiveParser.IGNORE, 0); }
		public TerminalNode SORT() { return getToken(SqlHiveParser.SORT, 0); }
		public TerminalNode CLUSTER() { return getToken(SqlHiveParser.CLUSTER, 0); }
		public TerminalNode DISTRIBUTE() { return getToken(SqlHiveParser.DISTRIBUTE, 0); }
		public TerminalNode UNSET() { return getToken(SqlHiveParser.UNSET, 0); }
		public TerminalNode TBLPROPERTIES() { return getToken(SqlHiveParser.TBLPROPERTIES, 0); }
		public TerminalNode SKEWED() { return getToken(SqlHiveParser.SKEWED, 0); }
		public TerminalNode STORED() { return getToken(SqlHiveParser.STORED, 0); }
		public TerminalNode DIRECTORIES() { return getToken(SqlHiveParser.DIRECTORIES, 0); }
		public TerminalNode LOCATION() { return getToken(SqlHiveParser.LOCATION, 0); }
		public TerminalNode EXCHANGE() { return getToken(SqlHiveParser.EXCHANGE, 0); }
		public TerminalNode ARCHIVE() { return getToken(SqlHiveParser.ARCHIVE, 0); }
		public TerminalNode UNARCHIVE() { return getToken(SqlHiveParser.UNARCHIVE, 0); }
		public TerminalNode FILEFORMAT() { return getToken(SqlHiveParser.FILEFORMAT, 0); }
		public TerminalNode TOUCH() { return getToken(SqlHiveParser.TOUCH, 0); }
		public TerminalNode COMPACT() { return getToken(SqlHiveParser.COMPACT, 0); }
		public TerminalNode CONCATENATE() { return getToken(SqlHiveParser.CONCATENATE, 0); }
		public TerminalNode CHANGE() { return getToken(SqlHiveParser.CHANGE, 0); }
		public TerminalNode CASCADE() { return getToken(SqlHiveParser.CASCADE, 0); }
		public TerminalNode RESTRICT() { return getToken(SqlHiveParser.RESTRICT, 0); }
		public TerminalNode BUCKETS() { return getToken(SqlHiveParser.BUCKETS, 0); }
		public TerminalNode CLUSTERED() { return getToken(SqlHiveParser.CLUSTERED, 0); }
		public TerminalNode SORTED() { return getToken(SqlHiveParser.SORTED, 0); }
		public TerminalNode PURGE() { return getToken(SqlHiveParser.PURGE, 0); }
		public TerminalNode INPUTFORMAT() { return getToken(SqlHiveParser.INPUTFORMAT, 0); }
		public TerminalNode OUTPUTFORMAT() { return getToken(SqlHiveParser.OUTPUTFORMAT, 0); }
		public TerminalNode DBPROPERTIES() { return getToken(SqlHiveParser.DBPROPERTIES, 0); }
		public TerminalNode DFS() { return getToken(SqlHiveParser.DFS, 0); }
		public TerminalNode TRUNCATE() { return getToken(SqlHiveParser.TRUNCATE, 0); }
		public TerminalNode COMPUTE() { return getToken(SqlHiveParser.COMPUTE, 0); }
		public TerminalNode LIST() { return getToken(SqlHiveParser.LIST, 0); }
		public TerminalNode STATISTICS() { return getToken(SqlHiveParser.STATISTICS, 0); }
		public TerminalNode ANALYZE() { return getToken(SqlHiveParser.ANALYZE, 0); }
		public TerminalNode PARTITIONED() { return getToken(SqlHiveParser.PARTITIONED, 0); }
		public TerminalNode EXTERNAL() { return getToken(SqlHiveParser.EXTERNAL, 0); }
		public TerminalNode DEFINED() { return getToken(SqlHiveParser.DEFINED, 0); }
		public TerminalNode RECORDWRITER() { return getToken(SqlHiveParser.RECORDWRITER, 0); }
		public TerminalNode REVOKE() { return getToken(SqlHiveParser.REVOKE, 0); }
		public TerminalNode GRANT() { return getToken(SqlHiveParser.GRANT, 0); }
		public TerminalNode LOCK() { return getToken(SqlHiveParser.LOCK, 0); }
		public TerminalNode UNLOCK() { return getToken(SqlHiveParser.UNLOCK, 0); }
		public TerminalNode MSCK() { return getToken(SqlHiveParser.MSCK, 0); }
		public TerminalNode REPAIR() { return getToken(SqlHiveParser.REPAIR, 0); }
		public TerminalNode RECOVER() { return getToken(SqlHiveParser.RECOVER, 0); }
		public TerminalNode EXPORT() { return getToken(SqlHiveParser.EXPORT, 0); }
		public TerminalNode IMPORT() { return getToken(SqlHiveParser.IMPORT, 0); }
		public TerminalNode LOAD() { return getToken(SqlHiveParser.LOAD, 0); }
		public TerminalNode VALUES() { return getToken(SqlHiveParser.VALUES, 0); }
		public TerminalNode COMMENT() { return getToken(SqlHiveParser.COMMENT, 0); }
		public TerminalNode ROLE() { return getToken(SqlHiveParser.ROLE, 0); }
		public TerminalNode ROLES() { return getToken(SqlHiveParser.ROLES, 0); }
		public TerminalNode COMPACTIONS() { return getToken(SqlHiveParser.COMPACTIONS, 0); }
		public TerminalNode PRINCIPALS() { return getToken(SqlHiveParser.PRINCIPALS, 0); }
		public TerminalNode TRANSACTIONS() { return getToken(SqlHiveParser.TRANSACTIONS, 0); }
		public TerminalNode INDEX() { return getToken(SqlHiveParser.INDEX, 0); }
		public TerminalNode INDEXES() { return getToken(SqlHiveParser.INDEXES, 0); }
		public TerminalNode LOCKS() { return getToken(SqlHiveParser.LOCKS, 0); }
		public TerminalNode OPTION() { return getToken(SqlHiveParser.OPTION, 0); }
		public TerminalNode LOCAL() { return getToken(SqlHiveParser.LOCAL, 0); }
		public TerminalNode INPATH() { return getToken(SqlHiveParser.INPATH, 0); }
		public TerminalNode ASC() { return getToken(SqlHiveParser.ASC, 0); }
		public TerminalNode DESC() { return getToken(SqlHiveParser.DESC, 0); }
		public TerminalNode LIMIT() { return getToken(SqlHiveParser.LIMIT, 0); }
		public TerminalNode RENAME() { return getToken(SqlHiveParser.RENAME, 0); }
		public TerminalNode SETS() { return getToken(SqlHiveParser.SETS, 0); }
		public TerminalNode AT() { return getToken(SqlHiveParser.AT, 0); }
		public TerminalNode NULLS() { return getToken(SqlHiveParser.NULLS, 0); }
		public TerminalNode OVERWRITE() { return getToken(SqlHiveParser.OVERWRITE, 0); }
		public TerminalNode ALL() { return getToken(SqlHiveParser.ALL, 0); }
		public TerminalNode ANY() { return getToken(SqlHiveParser.ANY, 0); }
		public TerminalNode ALTER() { return getToken(SqlHiveParser.ALTER, 0); }
		public TerminalNode AS() { return getToken(SqlHiveParser.AS, 0); }
		public TerminalNode BETWEEN() { return getToken(SqlHiveParser.BETWEEN, 0); }
		public TerminalNode BY() { return getToken(SqlHiveParser.BY, 0); }
		public TerminalNode CREATE() { return getToken(SqlHiveParser.CREATE, 0); }
		public TerminalNode DELETE() { return getToken(SqlHiveParser.DELETE, 0); }
		public TerminalNode UPDATE() { return getToken(SqlHiveParser.UPDATE, 0); }
		public TerminalNode DESCRIBE() { return getToken(SqlHiveParser.DESCRIBE, 0); }
		public TerminalNode DROP() { return getToken(SqlHiveParser.DROP, 0); }
		public TerminalNode EXISTS() { return getToken(SqlHiveParser.EXISTS, 0); }
		public TerminalNode FALSE() { return getToken(SqlHiveParser.FALSE, 0); }
		public TerminalNode FOR() { return getToken(SqlHiveParser.FOR, 0); }
		public TerminalNode GROUP() { return getToken(SqlHiveParser.GROUP, 0); }
		public TerminalNode IN() { return getToken(SqlHiveParser.IN, 0); }
		public TerminalNode INSERT() { return getToken(SqlHiveParser.INSERT, 0); }
		public TerminalNode INTO() { return getToken(SqlHiveParser.INTO, 0); }
		public TerminalNode IS() { return getToken(SqlHiveParser.IS, 0); }
		public TerminalNode LIKE() { return getToken(SqlHiveParser.LIKE, 0); }
		public TerminalNode NULL() { return getToken(SqlHiveParser.NULL, 0); }
		public TerminalNode ORDER() { return getToken(SqlHiveParser.ORDER, 0); }
		public TerminalNode OUTER() { return getToken(SqlHiveParser.OUTER, 0); }
		public TerminalNode TABLE() { return getToken(SqlHiveParser.TABLE, 0); }
		public TerminalNode TRUE() { return getToken(SqlHiveParser.TRUE, 0); }
		public TerminalNode WITH() { return getToken(SqlHiveParser.WITH, 0); }
		public TerminalNode RLIKE() { return getToken(SqlHiveParser.RLIKE, 0); }
		public TerminalNode AND() { return getToken(SqlHiveParser.AND, 0); }
		public TerminalNode CASE() { return getToken(SqlHiveParser.CASE, 0); }
		public TerminalNode CAST() { return getToken(SqlHiveParser.CAST, 0); }
		public TerminalNode DISTINCT() { return getToken(SqlHiveParser.DISTINCT, 0); }
		public TerminalNode DIV() { return getToken(SqlHiveParser.DIV, 0); }
		public TerminalNode ELSE() { return getToken(SqlHiveParser.ELSE, 0); }
		public TerminalNode END() { return getToken(SqlHiveParser.END, 0); }
		public TerminalNode FUNCTION() { return getToken(SqlHiveParser.FUNCTION, 0); }
		public TerminalNode INTERVAL() { return getToken(SqlHiveParser.INTERVAL, 0); }
		public TerminalNode MACRO() { return getToken(SqlHiveParser.MACRO, 0); }
		public TerminalNode OR() { return getToken(SqlHiveParser.OR, 0); }
		public TerminalNode STRATIFY() { return getToken(SqlHiveParser.STRATIFY, 0); }
		public TerminalNode THEN() { return getToken(SqlHiveParser.THEN, 0); }
		public TerminalNode UNBOUNDED() { return getToken(SqlHiveParser.UNBOUNDED, 0); }
		public TerminalNode WHEN() { return getToken(SqlHiveParser.WHEN, 0); }
		public TerminalNode DATABASE() { return getToken(SqlHiveParser.DATABASE, 0); }
		public TerminalNode SELECT() { return getToken(SqlHiveParser.SELECT, 0); }
		public TerminalNode FROM() { return getToken(SqlHiveParser.FROM, 0); }
		public TerminalNode WHERE() { return getToken(SqlHiveParser.WHERE, 0); }
		public TerminalNode HAVING() { return getToken(SqlHiveParser.HAVING, 0); }
		public TerminalNode NOT() { return getToken(SqlHiveParser.NOT, 0); }
		public TerminalNode DIRECTORY() { return getToken(SqlHiveParser.DIRECTORY, 0); }
		public TerminalNode BOTH() { return getToken(SqlHiveParser.BOTH, 0); }
		public TerminalNode LEADING() { return getToken(SqlHiveParser.LEADING, 0); }
		public TerminalNode TRAILING() { return getToken(SqlHiveParser.TRAILING, 0); }
		public NonReservedContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_nonReserved; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).enterNonReserved(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlHiveListener ) ((SqlHiveListener)listener).exitNonReserved(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlHiveVisitor ) return ((SqlHiveVisitor<? extends T>)visitor).visitNonReserved(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NonReservedContext nonReserved() throws RecognitionException {
		NonReservedContext _localctx = new NonReservedContext(_ctx, getState());
		enterRule(_localctx, 208, RULE_nonReserved);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2480);
			_la = _input.LA(1);
			if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << SELECT) | (1L << FROM) | (1L << ADD) | (1L << AS) | (1L << ALL) | (1L << ANY) | (1L << DISTINCT) | (1L << WHERE) | (1L << GROUP) | (1L << BY) | (1L << GROUPING) | (1L << SETS) | (1L << CUBE) | (1L << ROLLUP) | (1L << ORDER) | (1L << HAVING) | (1L << LIMIT) | (1L << AT) | (1L << OR) | (1L << AND) | (1L << IN) | (1L << NOT) | (1L << NO) | (1L << EXISTS) | (1L << BETWEEN) | (1L << LIKE) | (1L << RLIKE) | (1L << IS) | (1L << NULL) | (1L << TRUE) | (1L << FALSE) | (1L << NULLS) | (1L << ASC) | (1L << DESC) | (1L << FOR) | (1L << INTERVAL) | (1L << CASE) | (1L << WHEN) | (1L << THEN) | (1L << ELSE) | (1L << END) | (1L << OUTER) | (1L << PIVOT) | (1L << LATERAL))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (WINDOW - 64)) | (1L << (OVER - 64)) | (1L << (PARTITION - 64)) | (1L << (RANGE - 64)) | (1L << (ROWS - 64)) | (1L << (UNBOUNDED - 64)) | (1L << (PRECEDING - 64)) | (1L << (FOLLOWING - 64)) | (1L << (CURRENT - 64)) | (1L << (FIRST - 64)) | (1L << (AFTER - 64)) | (1L << (LAST - 64)) | (1L << (ROW - 64)) | (1L << (WITH - 64)) | (1L << (VALUES - 64)) | (1L << (CREATE - 64)) | (1L << (TABLE - 64)) | (1L << (DIRECTORY - 64)) | (1L << (VIEW - 64)) | (1L << (REPLACE - 64)) | (1L << (INSERT - 64)) | (1L << (UPDATE - 64)) | (1L << (DELETE - 64)) | (1L << (INTO - 64)) | (1L << (DESCRIBE - 64)) | (1L << (EXPLAIN - 64)) | (1L << (FORMAT - 64)) | (1L << (LOGICAL - 64)) | (1L << (CODEGEN - 64)) | (1L << (COST - 64)) | (1L << (CAST - 64)) | (1L << (SHOW - 64)) | (1L << (TABLES - 64)) | (1L << (COLUMNS - 64)) | (1L << (COLUMN - 64)) | (1L << (USE - 64)) | (1L << (PARTITIONS - 64)) | (1L << (FUNCTIONS - 64)) | (1L << (DROP - 64)) | (1L << (TO - 64)) | (1L << (TABLESAMPLE - 64)) | (1L << (STRATIFY - 64)) | (1L << (ALTER - 64)) | (1L << (RENAME - 64)) | (1L << (ARRAY - 64)) | (1L << (MAP - 64)) | (1L << (STRUCT - 64)) | (1L << (COMMENT - 64)) | (1L << (SET - 64)) | (1L << (RESET - 64)) | (1L << (DATA - 64)) | (1L << (START - 64)) | (1L << (TRANSACTION - 64)) | (1L << (COMMIT - 64)) | (1L << (ROLLBACK - 64)) | (1L << (MACRO - 64)) | (1L << (IGNORE - 64)) | (1L << (BOTH - 64)) | (1L << (LEADING - 64)) | (1L << (TRAILING - 64)))) != 0) || ((((_la - 128)) & ~0x3f) == 0 && ((1L << (_la - 128)) & ((1L << (IF - 128)) | (1L << (POSITION - 128)) | (1L << (EXTRACT - 128)) | (1L << (DIV - 128)) | (1L << (PERCENTLIT - 128)) | (1L << (BUCKET - 128)) | (1L << (OUT - 128)) | (1L << (OF - 128)) | (1L << (SORT - 128)) | (1L << (CLUSTER - 128)) | (1L << (DISTRIBUTE - 128)) | (1L << (OVERWRITE - 128)) | (1L << (TRANSFORM - 128)) | (1L << (REDUCE - 128)) | (1L << (SERDE - 128)) | (1L << (SERDEPROPERTIES - 128)) | (1L << (RECORDREADER - 128)) | (1L << (RECORDWRITER - 128)) | (1L << (DELIMITED - 128)) | (1L << (FIELDS - 128)) | (1L << (TERMINATED - 128)) | (1L << (COLLECTION - 128)) | (1L << (ITEMS - 128)) | (1L << (KEYS - 128)) | (1L << (ESCAPED - 128)) | (1L << (LINES - 128)) | (1L << (SEPARATED - 128)) | (1L << (FUNCTION - 128)) | (1L << (EXTENDED - 128)) | (1L << (REFRESH - 128)) | (1L << (CLEAR - 128)) | (1L << (CACHE - 128)) | (1L << (UNCACHE - 128)) | (1L << (LAZY - 128)) | (1L << (FORMATTED - 128)) | (1L << (GLOBAL - 128)) | (1L << (TEMPORARY - 128)) | (1L << (OPTIONS - 128)) | (1L << (UNSET - 128)) | (1L << (TBLPROPERTIES - 128)) | (1L << (DBPROPERTIES - 128)) | (1L << (BUCKETS - 128)) | (1L << (SKEWED - 128)) | (1L << (STORED - 128)) | (1L << (DIRECTORIES - 128)))) != 0) || ((((_la - 192)) & ~0x3f) == 0 && ((1L << (_la - 192)) & ((1L << (LOCATION - 192)) | (1L << (EXCHANGE - 192)) | (1L << (ARCHIVE - 192)) | (1L << (UNARCHIVE - 192)) | (1L << (FILEFORMAT - 192)) | (1L << (TOUCH - 192)) | (1L << (COMPACT - 192)) | (1L << (CONCATENATE - 192)) | (1L << (CHANGE - 192)) | (1L << (CASCADE - 192)) | (1L << (RESTRICT - 192)) | (1L << (CLUSTERED - 192)) | (1L << (SORTED - 192)) | (1L << (PURGE - 192)) | (1L << (INPUTFORMAT - 192)) | (1L << (OUTPUTFORMAT - 192)) | (1L << (DATABASE - 192)) | (1L << (DATABASES - 192)) | (1L << (DFS - 192)) | (1L << (TRUNCATE - 192)) | (1L << (ANALYZE - 192)) | (1L << (COMPUTE - 192)) | (1L << (LIST - 192)) | (1L << (STATISTICS - 192)) | (1L << (PARTITIONED - 192)) | (1L << (EXTERNAL - 192)) | (1L << (DEFINED - 192)) | (1L << (REVOKE - 192)) | (1L << (GRANT - 192)) | (1L << (LOCK - 192)) | (1L << (UNLOCK - 192)) | (1L << (MSCK - 192)) | (1L << (REPAIR - 192)) | (1L << (RECOVER - 192)) | (1L << (EXPORT - 192)) | (1L << (IMPORT - 192)) | (1L << (LOAD - 192)) | (1L << (ROLE - 192)) | (1L << (ROLES - 192)) | (1L << (COMPACTIONS - 192)) | (1L << (PRINCIPALS - 192)) | (1L << (TRANSACTIONS - 192)) | (1L << (INDEX - 192)) | (1L << (INDEXES - 192)) | (1L << (LOCKS - 192)) | (1L << (OPTION - 192)) | (1L << (LOCAL - 192)) | (1L << (INPATH - 192)))) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public boolean sempred(RuleContext _localctx, int ruleIndex, int predIndex) {
		switch (ruleIndex) {
		case 39:
			return queryTerm_sempred((QueryTermContext)_localctx, predIndex);
		case 75:
			return booleanExpression_sempred((BooleanExpressionContext)_localctx, predIndex);
		case 77:
			return valueExpression_sempred((ValueExpressionContext)_localctx, predIndex);
		case 78:
			return primaryExpression_sempred((PrimaryExpressionContext)_localctx, predIndex);
		}
		return true;
	}
	private boolean queryTerm_sempred(QueryTermContext _localctx, int predIndex) {
		switch (predIndex) {
		case 0:
			return precpred(_ctx, 3);
		case 1:
			return legacy_setops_precedence_enbled;
		case 2:
			return precpred(_ctx, 2);
		case 3:
			return !legacy_setops_precedence_enbled;
		case 4:
			return precpred(_ctx, 1);
		case 5:
			return !legacy_setops_precedence_enbled;
		}
		return true;
	}
	private boolean booleanExpression_sempred(BooleanExpressionContext _localctx, int predIndex) {
		switch (predIndex) {
		case 6:
			return precpred(_ctx, 2);
		case 7:
			return precpred(_ctx, 1);
		}
		return true;
	}
	private boolean valueExpression_sempred(ValueExpressionContext _localctx, int predIndex) {
		switch (predIndex) {
		case 8:
			return precpred(_ctx, 6);
		case 9:
			return precpred(_ctx, 5);
		case 10:
			return precpred(_ctx, 4);
		case 11:
			return precpred(_ctx, 3);
		case 12:
			return precpred(_ctx, 2);
		case 13:
			return precpred(_ctx, 1);
		}
		return true;
	}
	private boolean primaryExpression_sempred(PrimaryExpressionContext _localctx, int predIndex) {
		switch (predIndex) {
		case 14:
			return precpred(_ctx, 5);
		case 15:
			return precpred(_ctx, 3);
		}
		return true;
	}

	private static final int _serializedATNSegments = 2;
	private static final String _serializedATNSegment0 =
		"\3\u608b\ua72a\u8133\ub9ed\u417c\u3be7\u7786\u5964\3\u0101\u09b5\4\2\t"+
		"\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13"+
		"\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
		"\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31\t\31"+
		"\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t \4!"+
		"\t!\4\"\t\"\4#\t#\4$\t$\4%\t%\4&\t&\4\'\t\'\4(\t(\4)\t)\4*\t*\4+\t+\4"+
		",\t,\4-\t-\4.\t.\4/\t/\4\60\t\60\4\61\t\61\4\62\t\62\4\63\t\63\4\64\t"+
		"\64\4\65\t\65\4\66\t\66\4\67\t\67\48\t8\49\t9\4:\t:\4;\t;\4<\t<\4=\t="+
		"\4>\t>\4?\t?\4@\t@\4A\tA\4B\tB\4C\tC\4D\tD\4E\tE\4F\tF\4G\tG\4H\tH\4I"+
		"\tI\4J\tJ\4K\tK\4L\tL\4M\tM\4N\tN\4O\tO\4P\tP\4Q\tQ\4R\tR\4S\tS\4T\tT"+
		"\4U\tU\4V\tV\4W\tW\4X\tX\4Y\tY\4Z\tZ\4[\t[\4\\\t\\\4]\t]\4^\t^\4_\t_\4"+
		"`\t`\4a\ta\4b\tb\4c\tc\4d\td\4e\te\4f\tf\4g\tg\4h\th\4i\ti\4j\tj\3\2\3"+
		"\2\3\2\3\3\3\3\3\3\3\4\3\4\3\4\3\5\3\5\3\5\3\6\3\6\3\6\3\7\3\7\3\7\3\b"+
		"\3\b\3\b\3\b\3\b\3\b\3\b\3\b\3\b\3\b\5\b\u00f1\n\b\3\b\3\b\3\b\5\b\u00f6"+
		"\n\b\3\b\5\b\u00f9\n\b\3\b\3\b\3\b\5\b\u00fe\n\b\3\b\3\b\3\b\3\b\3\b\3"+
		"\b\3\b\3\b\3\b\3\b\3\b\5\b\u010b\n\b\3\b\3\b\5\b\u010f\n\b\3\b\3\b\3\b"+
		"\3\b\3\b\5\b\u0116\n\b\3\b\3\b\3\b\3\b\3\b\3\b\3\b\3\b\3\b\3\b\3\b\3\b"+
		"\7\b\u0124\n\b\f\b\16\b\u0127\13\b\3\b\5\b\u012a\n\b\3\b\5\b\u012d\n\b"+
		"\3\b\3\b\3\b\3\b\3\b\5\b\u0134\n\b\3\b\3\b\3\b\3\b\3\b\3\b\3\b\3\b\3\b"+
		"\3\b\3\b\3\b\3\b\3\b\3\b\7\b\u0145\n\b\f\b\16\b\u0148\13\b\3\b\5\b\u014b"+
		"\n\b\3\b\5\b\u014e\n\b\3\b\3\b\3\b\3\b\3\b\5\b\u0155\n\b\3\b\3\b\3\b\3"+
		"\b\5\b\u015b\n\b\3\b\3\b\3\b\3\b\5\b\u0161\n\b\3\b\3\b\3\b\3\b\3\b\3\b"+
		"\5\b\u0169\n\b\3\b\3\b\3\b\3\b\3\b\3\b\3\b\3\b\3\b\3\b\3\b\3\b\3\b\3\b"+
		"\3\b\3\b\3\b\3\b\3\b\3\b\3\b\3\b\3\b\3\b\3\b\3\b\3\b\3\b\3\b\3\b\5\b\u0189"+
		"\n\b\3\b\3\b\3\b\3\b\3\b\3\b\5\b\u0191\n\b\3\b\3\b\5\b\u0195\n\b\3\b\3"+
		"\b\3\b\5\b\u019a\n\b\3\b\3\b\3\b\3\b\5\b\u01a0\n\b\3\b\3\b\3\b\3\b\3\b"+
		"\3\b\5\b\u01a8\n\b\3\b\3\b\3\b\3\b\5\b\u01ae\n\b\3\b\3\b\3\b\3\b\3\b\3"+
		"\b\3\b\3\b\3\b\3\b\3\b\5\b\u01bb\n\b\3\b\6\b\u01be\n\b\r\b\16\b\u01bf"+
		"\3\b\3\b\3\b\3\b\3\b\3\b\3\b\5\b\u01c9\n\b\3\b\6\b\u01cc\n\b\r\b\16\b"+
		"\u01cd\3\b\3\b\3\b\3\b\3\b\3\b\3\b\3\b\3\b\3\b\3\b\3\b\3\b\3\b\5\b\u01de"+
		"\n\b\3\b\3\b\3\b\7\b\u01e3\n\b\f\b\16\b\u01e6\13\b\3\b\5\b\u01e9\n\b\3"+
		"\b\3\b\3\b\3\b\3\b\3\b\5\b\u01f1\n\b\3\b\3\b\3\b\7\b\u01f6\n\b\f\b\16"+
		"\b\u01f9\13\b\3\b\3\b\3\b\3\b\5\b\u01ff\n\b\3\b\3\b\3\b\3\b\3\b\3\b\3"+
		"\b\3\b\3\b\3\b\3\b\3\b\3\b\5\b\u020e\n\b\3\b\3\b\5\b\u0212\n\b\3\b\3\b"+
		"\3\b\3\b\5\b\u0218\n\b\3\b\3\b\3\b\3\b\5\b\u021e\n\b\3\b\5\b\u0221\n\b"+
		"\3\b\5\b\u0224\n\b\3\b\3\b\3\b\3\b\5\b\u022a\n\b\3\b\3\b\5\b\u022e\n\b"+
		"\3\b\3\b\5\b\u0232\n\b\3\b\3\b\3\b\5\b\u0237\n\b\3\b\3\b\5\b\u023b\n\b"+
		"\3\b\3\b\3\b\3\b\3\b\3\b\5\b\u0243\n\b\3\b\5\b\u0246\n\b\3\b\3\b\3\b\3"+
		"\b\3\b\3\b\3\b\5\b\u024f\n\b\3\b\3\b\3\b\5\b\u0254\n\b\3\b\3\b\3\b\3\b"+
		"\5\b\u025a\n\b\3\b\3\b\3\b\3\b\3\b\5\b\u0261\n\b\3\b\5\b\u0264\n\b\3\b"+
		"\3\b\3\b\3\b\5\b\u026a\n\b\3\b\3\b\3\b\3\b\3\b\3\b\3\b\7\b\u0273\n\b\f"+
		"\b\16\b\u0276\13\b\5\b\u0278\n\b\3\b\3\b\5\b\u027c\n\b\3\b\3\b\3\b\5\b"+
		"\u0281\n\b\3\b\3\b\3\b\5\b\u0286\n\b\3\b\3\b\3\b\3\b\3\b\5\b\u028d\n\b"+
		"\3\b\5\b\u0290\n\b\3\b\5\b\u0293\n\b\3\b\3\b\3\b\3\b\3\b\5\b\u029a\n\b"+
		"\3\b\3\b\3\b\5\b\u029f\n\b\3\b\3\b\3\b\5\b\u02a4\n\b\3\b\5\b\u02a7\n\b"+
		"\3\b\3\b\3\b\3\b\3\b\3\b\3\b\5\b\u02b0\n\b\3\b\3\b\3\b\3\b\3\b\3\b\5\b"+
		"\u02b8\n\b\3\b\3\b\3\b\3\b\5\b\u02be\n\b\3\b\3\b\5\b\u02c2\n\b\3\b\3\b"+
		"\5\b\u02c6\n\b\3\b\3\b\5\b\u02ca\n\b\5\b\u02cc\n\b\3\b\3\b\3\b\3\b\3\b"+
		"\3\b\3\b\5\b\u02d5\n\b\3\b\3\b\3\b\3\b\5\b\u02db\n\b\3\b\3\b\3\b\5\b\u02e0"+
		"\n\b\3\b\5\b\u02e3\n\b\3\b\3\b\5\b\u02e7\n\b\3\b\5\b\u02ea\n\b\3\b\3\b"+
		"\3\b\3\b\3\b\3\b\7\b\u02f2\n\b\f\b\16\b\u02f5\13\b\5\b\u02f7\n\b\3\b\3"+
		"\b\5\b\u02fb\n\b\3\b\3\b\3\b\5\b\u0300\n\b\3\b\5\b\u0303\n\b\3\b\3\b\3"+
		"\b\3\b\5\b\u0309\n\b\3\b\3\b\3\b\3\b\3\b\3\b\5\b\u0311\n\b\3\b\3\b\3\b"+
		"\5\b\u0316\n\b\3\b\3\b\3\b\3\b\5\b\u031c\n\b\3\b\3\b\3\b\3\b\5\b\u0322"+
		"\n\b\3\b\3\b\3\b\3\b\3\b\3\b\3\b\7\b\u032b\n\b\f\b\16\b\u032e\13\b\3\b"+
		"\3\b\3\b\7\b\u0333\n\b\f\b\16\b\u0336\13\b\3\b\3\b\7\b\u033a\n\b\f\b\16"+
		"\b\u033d\13\b\3\b\3\b\3\b\7\b\u0342\n\b\f\b\16\b\u0345\13\b\5\b\u0347"+
		"\n\b\3\t\3\t\3\t\3\t\3\t\3\t\5\t\u034f\n\t\3\t\3\t\5\t\u0353\n\t\3\t\3"+
		"\t\3\t\3\t\3\t\5\t\u035a\n\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3"+
		"\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t"+
		"\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3"+
		"\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t"+
		"\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3"+
		"\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t"+
		"\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\5\t\u03ce"+
		"\n\t\3\t\3\t\3\t\3\t\3\t\3\t\5\t\u03d6\n\t\3\t\3\t\3\t\3\t\3\t\3\t\5\t"+
		"\u03de\n\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\5\t\u03e7\n\t\3\t\3\t\3\t\3\t\3"+
		"\t\3\t\3\t\3\t\5\t\u03f1\n\t\3\n\3\n\3\n\3\n\3\n\5\n\u03f8\n\n\3\13\3"+
		"\13\3\13\3\13\3\13\3\13\5\13\u0400\n\13\3\f\3\f\3\f\7\f\u0405\n\f\f\f"+
		"\16\f\u0408\13\f\3\r\3\r\3\r\3\r\3\16\3\16\5\16\u0410\n\16\3\16\5\16\u0413"+
		"\n\16\3\16\3\16\3\16\3\16\5\16\u0419\n\16\3\16\3\16\3\17\3\17\3\17\3\17"+
		"\3\17\3\17\5\17\u0423\n\17\3\17\3\17\3\17\3\17\3\20\3\20\3\20\3\20\3\20"+
		"\3\20\5\20\u042f\n\20\3\20\3\20\3\20\5\20\u0434\n\20\3\21\3\21\3\21\3"+
		"\22\5\22\u043a\n\22\3\22\3\22\3\23\3\23\3\23\3\23\3\23\3\23\3\23\3\23"+
		"\5\23\u0446\n\23\5\23\u0448\n\23\3\23\3\23\3\23\5\23\u044d\n\23\3\23\3"+
		"\23\5\23\u0451\n\23\3\23\3\23\3\23\5\23\u0456\n\23\3\23\3\23\3\23\5\23"+
		"\u045b\n\23\3\23\5\23\u045e\n\23\3\23\3\23\3\23\5\23\u0463\n\23\3\23\3"+
		"\23\5\23\u0467\n\23\3\23\3\23\3\23\5\23\u046c\n\23\5\23\u046e\n\23\3\24"+
		"\3\24\5\24\u0472\n\24\3\25\3\25\3\25\3\25\3\25\7\25\u0479\n\25\f\25\16"+
		"\25\u047c\13\25\3\25\3\25\3\26\3\26\3\26\5\26\u0483\n\26\3\27\3\27\3\27"+
		"\3\27\3\27\5\27\u048a\n\27\3\30\3\30\3\30\7\30\u048f\n\30\f\30\16\30\u0492"+
		"\13\30\3\31\3\31\3\31\3\31\7\31\u0498\n\31\f\31\16\31\u049b\13\31\3\32"+
		"\3\32\5\32\u049f\n\32\3\32\3\32\3\32\3\32\3\33\3\33\3\33\3\34\3\34\3\34"+
		"\3\34\7\34\u04ac\n\34\f\34\16\34\u04af\13\34\3\34\3\34\3\35\3\35\5\35"+
		"\u04b5\n\35\3\35\5\35\u04b8\n\35\3\36\3\36\3\36\7\36\u04bd\n\36\f\36\16"+
		"\36\u04c0\13\36\3\36\5\36\u04c3\n\36\3\37\3\37\3\37\3\37\5\37\u04c9\n"+
		"\37\3 \3 \3 \3 \7 \u04cf\n \f \16 \u04d2\13 \3 \3 \3!\3!\3!\3!\7!\u04da"+
		"\n!\f!\16!\u04dd\13!\3!\3!\3\"\3\"\3\"\3\"\3\"\3\"\5\"\u04e7\n\"\3#\3"+
		"#\3#\3#\3#\5#\u04ee\n#\3$\3$\3$\3$\5$\u04f4\n$\3%\3%\3%\3&\5&\u04fa\n"+
		"&\3&\3&\3&\3&\3&\6&\u0501\n&\r&\16&\u0502\5&\u0505\n&\3\'\3\'\3\'\3\'"+
		"\3\'\7\'\u050c\n\'\f\'\16\'\u050f\13\'\5\'\u0511\n\'\3\'\3\'\3\'\3\'\3"+
		"\'\7\'\u0518\n\'\f\'\16\'\u051b\13\'\5\'\u051d\n\'\3\'\3\'\3\'\3\'\3\'"+
		"\7\'\u0524\n\'\f\'\16\'\u0527\13\'\5\'\u0529\n\'\3\'\3\'\3\'\3\'\3\'\7"+
		"\'\u0530\n\'\f\'\16\'\u0533\13\'\5\'\u0535\n\'\3\'\5\'\u0538\n\'\3\'\3"+
		"\'\3\'\5\'\u053d\n\'\5\'\u053f\n\'\3(\5(\u0542\n(\3(\3(\3(\3)\3)\3)\3"+
		")\3)\3)\3)\5)\u054e\n)\3)\3)\3)\3)\3)\5)\u0555\n)\3)\3)\3)\3)\3)\5)\u055c"+
		"\n)\3)\7)\u055f\n)\f)\16)\u0562\13)\3*\3*\3*\3*\3*\3*\3*\3*\5*\u056c\n"+
		"*\3+\3+\5+\u0570\n+\3+\3+\5+\u0574\n+\3,\3,\3,\3,\3,\3,\3,\3,\3,\3,\5"+
		",\u0580\n,\3,\5,\u0583\n,\3,\3,\5,\u0587\n,\3,\3,\3,\3,\3,\3,\3,\3,\5"+
		",\u0591\n,\3,\3,\5,\u0595\n,\5,\u0597\n,\3,\5,\u059a\n,\3,\3,\5,\u059e"+
		"\n,\3,\5,\u05a1\n,\3,\3,\5,\u05a5\n,\3,\3,\7,\u05a9\n,\f,\16,\u05ac\13"+
		",\3,\5,\u05af\n,\3,\3,\5,\u05b3\n,\3,\3,\3,\5,\u05b8\n,\3,\5,\u05bb\n"+
		",\5,\u05bd\n,\3,\7,\u05c0\n,\f,\16,\u05c3\13,\3,\3,\5,\u05c7\n,\3,\5,"+
		"\u05ca\n,\3,\3,\5,\u05ce\n,\3,\5,\u05d1\n,\5,\u05d3\n,\3-\3-\3-\5-\u05d8"+
		"\n-\3-\7-\u05db\n-\f-\16-\u05de\13-\3-\3-\3.\3.\3.\3.\3.\3.\7.\u05e8\n"+
		".\f.\16.\u05eb\13.\3.\3.\5.\u05ef\n.\3/\3/\3/\3/\7/\u05f5\n/\f/\16/\u05f8"+
		"\13/\3/\7/\u05fb\n/\f/\16/\u05fe\13/\3/\5/\u0601\n/\3\60\3\60\3\60\3\60"+
		"\3\60\7\60\u0608\n\60\f\60\16\60\u060b\13\60\3\60\3\60\3\60\3\60\3\60"+
		"\3\60\3\60\3\60\3\60\3\60\7\60\u0617\n\60\f\60\16\60\u061a\13\60\3\60"+
		"\3\60\5\60\u061e\n\60\3\60\3\60\3\60\3\60\3\60\3\60\3\60\3\60\7\60\u0628"+
		"\n\60\f\60\16\60\u062b\13\60\3\60\3\60\5\60\u062f\n\60\3\61\3\61\3\61"+
		"\3\61\7\61\u0635\n\61\f\61\16\61\u0638\13\61\5\61\u063a\n\61\3\61\3\61"+
		"\5\61\u063e\n\61\3\62\3\62\3\62\3\62\3\62\3\62\3\62\3\62\3\62\3\62\7\62"+
		"\u064a\n\62\f\62\16\62\u064d\13\62\3\62\3\62\3\62\3\63\3\63\3\63\3\63"+
		"\3\63\7\63\u0657\n\63\f\63\16\63\u065a\13\63\3\63\3\63\5\63\u065e\n\63"+
		"\3\64\3\64\5\64\u0662\n\64\3\64\5\64\u0665\n\64\3\65\3\65\3\65\5\65\u066a"+
		"\n\65\3\65\3\65\3\65\3\65\3\65\7\65\u0671\n\65\f\65\16\65\u0674\13\65"+
		"\5\65\u0676\n\65\3\65\3\65\3\65\5\65\u067b\n\65\3\65\3\65\3\65\7\65\u0680"+
		"\n\65\f\65\16\65\u0683\13\65\5\65\u0685\n\65\3\66\3\66\3\67\3\67\7\67"+
		"\u068b\n\67\f\67\16\67\u068e\13\67\38\38\38\38\58\u0694\n8\38\38\38\3"+
		"8\38\58\u069b\n8\39\59\u069e\n9\39\39\39\59\u06a3\n9\39\39\39\39\59\u06a9"+
		"\n9\39\39\59\u06ad\n9\39\59\u06b0\n9\39\59\u06b3\n9\3:\3:\3:\3:\3:\3:"+
		"\3:\7:\u06bc\n:\f:\16:\u06bf\13:\3:\3:\5:\u06c3\n:\3;\3;\3;\5;\u06c8\n"+
		";\3;\3;\3<\5<\u06cd\n<\3<\3<\3<\3<\3<\3<\3<\3<\3<\3<\3<\3<\3<\3<\3<\3"+
		"<\5<\u06df\n<\5<\u06e1\n<\3<\5<\u06e4\n<\3=\3=\3=\3=\3>\3>\3>\7>\u06ed"+
		"\n>\f>\16>\u06f0\13>\3?\3?\3?\3?\7?\u06f6\n?\f?\16?\u06f9\13?\3?\3?\3"+
		"@\3@\5@\u06ff\n@\3A\3A\3A\3A\7A\u0705\nA\fA\16A\u0708\13A\3A\3A\3B\3B"+
		"\3B\5B\u070f\nB\3C\3C\5C\u0713\nC\3C\3C\3C\3C\3C\3C\5C\u071b\nC\3C\3C"+
		"\3C\3C\3C\3C\5C\u0723\nC\3C\3C\3C\3C\5C\u0729\nC\3D\3D\3D\3D\7D\u072f"+
		"\nD\fD\16D\u0732\13D\3D\3D\3E\3E\3E\3E\3E\7E\u073b\nE\fE\16E\u073e\13"+
		"E\5E\u0740\nE\3E\3E\3E\3F\5F\u0746\nF\3F\3F\5F\u074a\nF\5F\u074c\nF\3"+
		"G\3G\3G\3G\3G\3G\3G\5G\u0755\nG\3G\3G\3G\3G\3G\3G\3G\3G\3G\3G\5G\u0761"+
		"\nG\5G\u0763\nG\3G\3G\3G\3G\3G\5G\u076a\nG\3G\3G\3G\3G\3G\5G\u0771\nG"+
		"\3G\3G\3G\3G\5G\u0777\nG\3G\3G\3G\3G\5G\u077d\nG\5G\u077f\nG\3H\3H\3H"+
		"\5H\u0784\nH\3H\3H\3I\3I\3I\5I\u078b\nI\3I\3I\3J\3J\5J\u0791\nJ\3J\3J"+
		"\5J\u0795\nJ\5J\u0797\nJ\3K\3K\3K\7K\u079c\nK\fK\16K\u079f\13K\3L\3L\3"+
		"M\3M\3M\3M\3M\3M\3M\3M\3M\3M\5M\u07ad\nM\5M\u07af\nM\3M\3M\3M\3M\3M\3"+
		"M\7M\u07b7\nM\fM\16M\u07ba\13M\3N\5N\u07bd\nN\3N\3N\3N\3N\3N\3N\5N\u07c5"+
		"\nN\3N\3N\3N\3N\3N\7N\u07cc\nN\fN\16N\u07cf\13N\3N\3N\3N\5N\u07d4\nN\3"+
		"N\3N\3N\3N\3N\3N\5N\u07dc\nN\3N\3N\3N\3N\5N\u07e2\nN\3N\3N\3N\5N\u07e7"+
		"\nN\3N\3N\3N\5N\u07ec\nN\3O\3O\3O\3O\5O\u07f2\nO\3O\3O\3O\3O\3O\3O\3O"+
		"\3O\3O\3O\3O\3O\3O\3O\3O\3O\3O\3O\3O\7O\u0807\nO\fO\16O\u080a\13O\3P\3"+
		"P\3P\6P\u080f\nP\rP\16P\u0810\3P\3P\5P\u0815\nP\3P\3P\3P\3P\3P\6P\u081c"+
		"\nP\rP\16P\u081d\3P\3P\5P\u0822\nP\3P\3P\3P\3P\3P\3P\3P\3P\3P\3P\3P\3"+
		"P\3P\3P\7P\u0832\nP\fP\16P\u0835\13P\5P\u0837\nP\3P\3P\3P\3P\3P\3P\5P"+
		"\u083f\nP\3P\3P\3P\3P\3P\3P\3P\5P\u0848\nP\3P\3P\3P\3P\3P\3P\3P\3P\3P"+
		"\3P\3P\3P\3P\3P\3P\3P\3P\3P\3P\6P\u085d\nP\rP\16P\u085e\3P\3P\3P\3P\3"+
		"P\3P\3P\3P\3P\5P\u086a\nP\3P\3P\3P\7P\u086f\nP\fP\16P\u0872\13P\5P\u0874"+
		"\nP\3P\3P\3P\5P\u0879\nP\3P\3P\3P\3P\3P\3P\3P\3P\3P\3P\3P\3P\3P\3P\3P"+
		"\6P\u088a\nP\rP\16P\u088b\3P\3P\3P\3P\3P\3P\3P\3P\3P\3P\3P\3P\3P\3P\3"+
		"P\5P\u089d\nP\3P\3P\3P\3P\3P\3P\3P\3P\7P\u08a7\nP\fP\16P\u08aa\13P\3Q"+
		"\3Q\3Q\3Q\3Q\3Q\3Q\3Q\6Q\u08b4\nQ\rQ\16Q\u08b5\5Q\u08b8\nQ\3R\3R\3S\3"+
		"S\3T\3T\3U\3U\3V\3V\7V\u08c4\nV\fV\16V\u08c7\13V\3W\3W\3W\3W\5W\u08cd"+
		"\nW\3X\5X\u08d0\nX\3X\3X\5X\u08d4\nX\3Y\3Y\3Y\5Y\u08d9\nY\3Z\3Z\3Z\3Z"+
		"\3Z\3Z\3Z\3Z\3Z\3Z\3Z\3Z\3Z\3Z\3Z\5Z\u08ea\nZ\3Z\3Z\5Z\u08ee\nZ\3Z\3Z"+
		"\3Z\3Z\3Z\7Z\u08f5\nZ\fZ\16Z\u08f8\13Z\3Z\5Z\u08fb\nZ\5Z\u08fd\nZ\3[\3"+
		"[\3[\7[\u0902\n[\f[\16[\u0905\13[\3\\\3\\\3\\\3\\\5\\\u090b\n\\\3]\3]"+
		"\3]\7]\u0910\n]\f]\16]\u0913\13]\3^\3^\3^\3^\3^\5^\u091a\n^\3_\3_\3_\3"+
		"_\3_\3`\3`\3`\3`\7`\u0925\n`\f`\16`\u0928\13`\3a\3a\3a\3a\3b\3b\3b\3b"+
		"\3b\3b\3b\7b\u0935\nb\fb\16b\u0938\13b\3b\3b\3b\3b\3b\7b\u093f\nb\fb\16"+
		"b\u0942\13b\5b\u0944\nb\3b\3b\3b\3b\3b\7b\u094b\nb\fb\16b\u094e\13b\5"+
		"b\u0950\nb\5b\u0952\nb\3b\5b\u0955\nb\3b\5b\u0958\nb\3c\3c\3c\3c\3c\3"+
		"c\3c\3c\3c\3c\3c\3c\3c\3c\3c\3c\5c\u096a\nc\3d\3d\3d\3d\3d\3d\3d\5d\u0973"+
		"\nd\3e\3e\3e\7e\u0978\ne\fe\16e\u097b\13e\3f\3f\3f\3f\3f\3f\3f\3f\3f\3"+
		"f\3f\3f\3f\3f\3f\5f\u098c\nf\3g\3g\3g\5g\u0991\ng\3h\3h\3i\5i\u0996\n"+
		"i\3i\3i\5i\u099a\ni\3i\3i\5i\u099e\ni\3i\3i\5i\u09a2\ni\3i\3i\5i\u09a6"+
		"\ni\3i\3i\5i\u09aa\ni\3i\3i\5i\u09ae\ni\3i\5i\u09b1\ni\3j\3j\3j\7\u02f3"+
		"\u032c\u0334\u033b\u0343\6P\u0098\u009c\u009ek\2\4\6\b\n\f\16\20\22\24"+
		"\26\30\32\34\36 \"$&(*,.\60\62\64\668:<>@BDFHJLNPRTVXZ\\^`bdfhjlnprtv"+
		"xz|~\u0080\u0082\u0084\u0086\u0088\u008a\u008c\u008e\u0090\u0092\u0094"+
		"\u0096\u0098\u009a\u009c\u009e\u00a0\u00a2\u00a4\u00a6\u00a8\u00aa\u00ac"+
		"\u00ae\u00b0\u00b2\u00b4\u00b6\u00b8\u00ba\u00bc\u00be\u00c0\u00c2\u00c4"+
		"\u00c6\u00c8\u00ca\u00cc\u00ce\u00d0\u00d2\2\35\3\2\u00cb\u00cc\4\2RR"+
		"TT\5\2]_\u00b1\u00b1\u00b7\u00b7\4\2\16\16!!\4\2..ZZ\4\2\u00b1\u00b1\u00b7"+
		"\u00b7\4\2\17\17\u00d8\u00d8\3\2il\3\2ik\3\2-.\4\2KKMM\4\2\21\21\23\23"+
		"\3\2\u00f7\u00f8\3\2&\'\4\2\u008d\u008e\u0093\u0093\3\2\u008f\u0092\4"+
		"\2\u008d\u008e\u0096\u0096\3\2\177\u0081\3\2\u0085\u008c\3\2\u008d\u0097"+
		"\3\2\37\"\3\2*+\3\2\u008d\u008e\4\2DD\u009e\u009e\4\2\33\33\u009c\u009c"+
		"\3\2HI\n\2\r\6588@hm\u0084\u0092\u0092\u0098\u00a1\u00a3\u00ef\u00f1\u00f2"+
		"\2\u0b4e\2\u00d4\3\2\2\2\4\u00d7\3\2\2\2\6\u00da\3\2\2\2\b\u00dd\3\2\2"+
		"\2\n\u00e0\3\2\2\2\f\u00e3\3\2\2\2\16\u0346\3\2\2\2\20\u03f0\3\2\2\2\22"+
		"\u03f2\3\2\2\2\24\u03f9\3\2\2\2\26\u0401\3\2\2\2\30\u0409\3\2\2\2\32\u040d"+
		"\3\2\2\2\34\u041c\3\2\2\2\36\u0428\3\2\2\2 \u0435\3\2\2\2\"\u0439\3\2"+
		"\2\2$\u046d\3\2\2\2&\u046f\3\2\2\2(\u0473\3\2\2\2*\u047f\3\2\2\2,\u0489"+
		"\3\2\2\2.\u048b\3\2\2\2\60\u0493\3\2\2\2\62\u049c\3\2\2\2\64\u04a4\3\2"+
		"\2\2\66\u04a7\3\2\2\28\u04b2\3\2\2\2:\u04c2\3\2\2\2<\u04c8\3\2\2\2>\u04ca"+
		"\3\2\2\2@\u04d5\3\2\2\2B\u04e6\3\2\2\2D\u04ed\3\2\2\2F\u04ef\3\2\2\2H"+
		"\u04f5\3\2\2\2J\u0504\3\2\2\2L\u0510\3\2\2\2N\u0541\3\2\2\2P\u0546\3\2"+
		"\2\2R\u056b\3\2\2\2T\u056d\3\2\2\2V\u05d2\3\2\2\2X\u05d4\3\2\2\2Z\u05ee"+
		"\3\2\2\2\\\u05f0\3\2\2\2^\u062e\3\2\2\2`\u063d\3\2\2\2b\u063f\3\2\2\2"+
		"d\u065d\3\2\2\2f\u065f\3\2\2\2h\u0666\3\2\2\2j\u0686\3\2\2\2l\u0688\3"+
		"\2\2\2n\u069a\3\2\2\2p\u06b2\3\2\2\2r\u06c2\3\2\2\2t\u06c4\3\2\2\2v\u06e3"+
		"\3\2\2\2x\u06e5\3\2\2\2z\u06e9\3\2\2\2|\u06f1\3\2\2\2~\u06fc\3\2\2\2\u0080"+
		"\u0700\3\2\2\2\u0082\u070b\3\2\2\2\u0084\u0728\3\2\2\2\u0086\u072a\3\2"+
		"\2\2\u0088\u0735\3\2\2\2\u008a\u074b\3\2\2\2\u008c\u077e\3\2\2\2\u008e"+
		"\u0783\3\2\2\2\u0090\u078a\3\2\2\2\u0092\u078e\3\2\2\2\u0094\u0798\3\2"+
		"\2\2\u0096\u07a0\3\2\2\2\u0098\u07ae\3\2\2\2\u009a\u07eb\3\2\2\2\u009c"+
		"\u07f1\3\2\2\2\u009e\u089c\3\2\2\2\u00a0\u08b7\3\2\2\2\u00a2\u08b9\3\2"+
		"\2\2\u00a4\u08bb\3\2\2\2\u00a6\u08bd\3\2\2\2\u00a8\u08bf\3\2\2\2\u00aa"+
		"\u08c1\3\2\2\2\u00ac\u08c8\3\2\2\2\u00ae\u08d3\3\2\2\2\u00b0\u08d8\3\2"+
		"\2\2\u00b2\u08fc\3\2\2\2\u00b4\u08fe\3\2\2\2\u00b6\u0906\3\2\2\2\u00b8"+
		"\u090c\3\2\2\2\u00ba\u0914\3\2\2\2\u00bc\u091b\3\2\2\2\u00be\u0920\3\2"+
		"\2\2\u00c0\u0929\3\2\2\2\u00c2\u0957\3\2\2\2\u00c4\u0969\3\2\2\2\u00c6"+
		"\u0972\3\2\2\2\u00c8\u0974\3\2\2\2\u00ca\u098b\3\2\2\2\u00cc\u0990\3\2"+
		"\2\2\u00ce\u0992\3\2\2\2\u00d0\u09b0\3\2\2\2\u00d2\u09b2\3\2\2\2\u00d4"+
		"\u00d5\5\16\b\2\u00d5\u00d6\7\2\2\3\u00d6\3\3\2\2\2\u00d7\u00d8\5\u0092"+
		"J\2\u00d8\u00d9\7\2\2\3\u00d9\5\3\2\2\2\u00da\u00db\5\u008eH\2\u00db\u00dc"+
		"\7\2\2\3\u00dc\7\3\2\2\2\u00dd\u00de\5\u0090I\2\u00de\u00df\7\2\2\3\u00df"+
		"\t\3\2\2\2\u00e0\u00e1\5\u00b2Z\2\u00e1\u00e2\7\2\2\3\u00e2\13\3\2\2\2"+
		"\u00e3\u00e4\5\u00b4[\2\u00e4\u00e5\7\2\2\3\u00e5\r\3\2\2\2\u00e6\u0347"+
		"\5\"\22\2\u00e7\u0347\5\22\n\2\u00e8\u0347\5\24\13\2\u00e9\u00ea\7e\2"+
		"\2\u00ea\u0347\5\u00caf\2\u00eb\u00ec\7Q\2\2\u00ec\u00f0\7\u00d2\2\2\u00ed"+
		"\u00ee\7\u0082\2\2\u00ee\u00ef\7\"\2\2\u00ef\u00f1\7$\2\2\u00f0\u00ed"+
		"\3\2\2\2\u00f0\u00f1\3\2\2\2\u00f1\u00f2\3\2\2\2\u00f2\u00f5\5\u00caf"+
		"\2\u00f3\u00f4\7u\2\2\u00f4\u00f6\7\u00f3\2\2\u00f5\u00f3\3\2\2\2\u00f5"+
		"\u00f6\3\2\2\2\u00f6\u00f8\3\2\2\2\u00f7\u00f9\5 \21\2\u00f8\u00f7\3\2"+
		"\2\2\u00f8\u00f9\3\2\2\2\u00f9\u00fd\3\2\2\2\u00fa\u00fb\7O\2\2\u00fb"+
		"\u00fc\7\u00bd\2\2\u00fc\u00fe\5\66\34\2\u00fd\u00fa\3\2\2\2\u00fd\u00fe"+
		"\3\2\2\2\u00fe\u0347\3\2\2\2\u00ff\u0100\7p\2\2\u0100\u0101\7\u00d2\2"+
		"\2\u0101\u0102\5\u00caf\2\u0102\u0103\7v\2\2\u0103\u0104\7\u00bd\2\2\u0104"+
		"\u0105\5\66\34\2\u0105\u0347\3\2\2\2\u0106\u0107\7h\2\2\u0107\u010a\7"+
		"\u00d2\2\2\u0108\u0109\7\u0082\2\2\u0109\u010b\7$\2\2\u010a\u0108\3\2"+
		"\2\2\u010a\u010b\3\2\2\2\u010b\u010c\3\2\2\2\u010c\u010e\5\u00caf\2\u010d"+
		"\u010f\t\2\2\2\u010e\u010d\3\2\2\2\u010e\u010f\3\2\2\2\u010f\u0347\3\2"+
		"\2\2\u0110\u0115\5\32\16\2\u0111\u0112\7\3\2\2\u0112\u0113\5\u00b4[\2"+
		"\u0113\u0114\7\4\2\2\u0114\u0116\3\2\2\2\u0115\u0111\3\2\2\2\u0115\u0116"+
		"\3\2\2\2\u0116\u0117\3\2\2\2\u0117\u0125\5\64\33\2\u0118\u0119\7\u00ba"+
		"\2\2\u0119\u0124\5\66\34\2\u011a\u011b\7\u00da\2\2\u011b\u011c\7\26\2"+
		"\2\u011c\u0124\5x=\2\u011d\u0124\5\34\17\2\u011e\u0124\5 \21\2\u011f\u0120"+
		"\7u\2\2\u0120\u0124\7\u00f3\2\2\u0121\u0122\7\u00bc\2\2\u0122\u0124\5"+
		"\66\34\2\u0123\u0118\3\2\2\2\u0123\u011a\3\2\2\2\u0123\u011d\3\2\2\2\u0123"+
		"\u011e\3\2\2\2\u0123\u011f\3\2\2\2\u0123\u0121\3\2\2\2\u0124\u0127\3\2"+
		"\2\2\u0125\u0123\3\2\2\2\u0125\u0126\3\2\2\2\u0126\u012c\3\2\2\2\u0127"+
		"\u0125\3\2\2\2\u0128\u012a\7\20\2\2\u0129\u0128\3\2\2\2\u0129\u012a\3"+
		"\2\2\2\u012a\u012b\3\2\2\2\u012b\u012d\5\"\22\2\u012c\u0129\3\2\2\2\u012c"+
		"\u012d\3\2\2\2\u012d\u0347\3\2\2\2\u012e\u0133\5\32\16\2\u012f\u0130\7"+
		"\3\2\2\u0130\u0131\5\u00b4[\2\u0131\u0132\7\4\2\2\u0132\u0134\3\2\2\2"+
		"\u0133\u012f\3\2\2\2\u0133\u0134\3\2\2\2\u0134\u0146\3\2\2\2\u0135\u0136"+
		"\7u\2\2\u0136\u0145\7\u00f3\2\2\u0137\u0138\7\u00da\2\2\u0138\u0139\7"+
		"\26\2\2\u0139\u013a\7\3\2\2\u013a\u013b\5\u00b4[\2\u013b\u013c\7\4\2\2"+
		"\u013c\u0145\3\2\2\2\u013d\u0145\5\34\17\2\u013e\u0145\5\36\20\2\u013f"+
		"\u0145\5\u008cG\2\u0140\u0145\5B\"\2\u0141\u0145\5 \21\2\u0142\u0143\7"+
		"\u00bc\2\2\u0143\u0145\5\66\34\2\u0144\u0135\3\2\2\2\u0144\u0137\3\2\2"+
		"\2\u0144\u013d\3\2\2\2\u0144\u013e\3\2\2\2\u0144\u013f\3\2\2\2\u0144\u0140"+
		"\3\2\2\2\u0144\u0141\3\2\2\2\u0144\u0142\3\2\2\2\u0145\u0148\3\2\2\2\u0146"+
		"\u0144\3\2\2\2\u0146\u0147\3\2\2\2\u0147\u014d\3\2\2\2\u0148\u0146\3\2"+
		"\2\2\u0149\u014b\7\20\2\2\u014a\u0149\3\2\2\2\u014a\u014b\3\2\2\2\u014b"+
		"\u014c\3\2\2\2\u014c\u014e\5\"\22\2\u014d\u014a\3\2\2\2\u014d\u014e\3"+
		"\2\2\2\u014e\u0347\3\2\2\2\u014f\u0150\7Q\2\2\u0150\u0154\7R\2\2\u0151"+
		"\u0152\7\u0082\2\2\u0152\u0153\7\"\2\2\u0153\u0155\7$\2\2\u0154\u0151"+
		"\3\2\2\2\u0154\u0155\3\2\2\2\u0155\u0156\3\2\2\2\u0156\u0157\5\u008eH"+
		"\2\u0157\u0158\7&\2\2\u0158\u015a\5\u008eH\2\u0159\u015b\5 \21\2\u015a"+
		"\u0159\3\2\2\2\u015a\u015b\3\2\2\2\u015b\u0347\3\2\2\2\u015c\u015d\7\u00d6"+
		"\2\2\u015d\u015e\7R\2\2\u015e\u0160\5\u008eH\2\u015f\u0161\5(\25\2\u0160"+
		"\u015f\3\2\2\2\u0160\u0161\3\2\2\2\u0161\u0162\3\2\2\2\u0162\u0163\7\u00d7"+
		"\2\2\u0163\u0168\7\u00d9\2\2\u0164\u0169\5\u00caf\2\u0165\u0166\7/\2\2"+
		"\u0166\u0167\7c\2\2\u0167\u0169\5z>\2\u0168\u0164\3\2\2\2\u0168\u0165"+
		"\3\2\2\2\u0168\u0169\3\2\2\2\u0169\u0347\3\2\2\2\u016a\u016b\7p\2\2\u016b"+
		"\u016c\7R\2\2\u016c\u016d\5\u008eH\2\u016d\u016e\7\17\2\2\u016e\u016f"+
		"\7c\2\2\u016f\u0170\7\3\2\2\u0170\u0171\5\u00b4[\2\u0171\u0172\7\4\2\2"+
		"\u0172\u0347\3\2\2\2\u0173\u0174\7p\2\2\u0174\u0175\t\3\2\2\u0175\u0176"+
		"\5\u008eH\2\u0176\u0177\7q\2\2\u0177\u0178\7m\2\2\u0178\u0179\5\u008e"+
		"H\2\u0179\u0347\3\2\2\2\u017a\u017b\7p\2\2\u017b\u017c\t\3\2\2\u017c\u017d"+
		"\5\u008eH\2\u017d\u017e\7v\2\2\u017e\u017f\7\u00bc\2\2\u017f\u0180\5\66"+
		"\34\2\u0180\u0347\3\2\2\2\u0181\u0182\7p\2\2\u0182\u0183\t\3\2\2\u0183"+
		"\u0184\5\u008eH\2\u0184\u0185\7\u00bb\2\2\u0185\u0188\7\u00bc\2\2\u0186"+
		"\u0187\7\u0082\2\2\u0187\u0189\7$\2\2\u0188\u0186\3\2\2\2\u0188\u0189"+
		"\3\2\2\2\u0189\u018a\3\2\2\2\u018a\u018b\5\66\34\2\u018b\u0347\3\2\2\2"+
		"\u018c\u018d\7p\2\2\u018d\u018e\7R\2\2\u018e\u0190\5\u008eH\2\u018f\u0191"+
		"\5(\25\2\u0190\u018f\3\2\2\2\u0190\u0191\3\2\2\2\u0191\u0192\3\2\2\2\u0192"+
		"\u0194\7\u00ca\2\2\u0193\u0195\7d\2\2\u0194\u0193\3\2\2\2\u0194\u0195"+
		"\3\2\2\2\u0195\u0196\3\2\2\2\u0196\u0197\5\u00caf\2\u0197\u0199\5\u00b6"+
		"\\\2\u0198\u019a\5\u00b0Y\2\u0199\u0198\3\2\2\2\u0199\u019a\3\2\2\2\u019a"+
		"\u0347\3\2\2\2\u019b\u019c\7p\2\2\u019c\u019d\7R\2\2\u019d\u019f\5\u008e"+
		"H\2\u019e\u01a0\5(\25\2\u019f\u019e\3\2\2\2\u019f\u01a0\3\2\2\2\u01a0"+
		"\u01a1\3\2\2\2\u01a1\u01a2\7v\2\2\u01a2\u01a3\7\u00a3\2\2\u01a3\u01a7"+
		"\7\u00f3\2\2\u01a4\u01a5\7O\2\2\u01a5\u01a6\7\u00a4\2\2\u01a6\u01a8\5"+
		"\66\34\2\u01a7\u01a4\3\2\2\2\u01a7\u01a8\3\2\2\2\u01a8\u0347\3\2\2\2\u01a9"+
		"\u01aa\7p\2\2\u01aa\u01ab\7R\2\2\u01ab\u01ad\5\u008eH\2\u01ac\u01ae\5"+
		"(\25\2\u01ad\u01ac\3\2\2\2\u01ad\u01ae\3\2\2\2\u01ae\u01af\3\2\2\2\u01af"+
		"\u01b0\7v\2\2\u01b0\u01b1\7\u00a4\2\2\u01b1\u01b2\5\66\34\2\u01b2\u0347"+
		"\3\2\2\2\u01b3\u01b4\7p\2\2\u01b4\u01b5\7R\2\2\u01b5\u01b6\5\u008eH\2"+
		"\u01b6\u01ba\7\17\2\2\u01b7\u01b8\7\u0082\2\2\u01b8\u01b9\7\"\2\2\u01b9"+
		"\u01bb\7$\2\2\u01ba\u01b7\3\2\2\2\u01ba\u01bb\3\2\2\2\u01bb\u01bd\3\2"+
		"\2\2\u01bc\u01be\5&\24\2\u01bd\u01bc\3\2\2\2\u01be\u01bf\3\2\2\2\u01bf"+
		"\u01bd\3\2\2\2\u01bf\u01c0\3\2\2\2\u01c0\u0347\3\2\2\2\u01c1\u01c2\7p"+
		"\2\2\u01c2\u01c3\7T\2\2\u01c3\u01c4\5\u008eH\2\u01c4\u01c8\7\17\2\2\u01c5"+
		"\u01c6\7\u0082\2\2\u01c6\u01c7\7\"\2\2\u01c7\u01c9\7$\2\2\u01c8\u01c5"+
		"\3\2\2\2\u01c8\u01c9\3\2\2\2\u01c9\u01cb\3\2\2\2\u01ca\u01cc\5(\25\2\u01cb"+
		"\u01ca\3\2\2\2\u01cc\u01cd\3\2\2\2\u01cd\u01cb\3\2\2\2\u01cd\u01ce\3\2"+
		"\2\2\u01ce\u0347\3\2\2\2\u01cf\u01d0\7p\2\2\u01d0\u01d1\7R\2\2\u01d1\u01d2"+
		"\5\u008eH\2\u01d2\u01d3\5(\25\2\u01d3\u01d4\7q\2\2\u01d4\u01d5\7m\2\2"+
		"\u01d5\u01d6\5(\25\2\u01d6\u0347\3\2\2\2\u01d7\u01d8\7p\2\2\u01d8\u01d9"+
		"\7R\2\2\u01d9\u01da\5\u008eH\2\u01da\u01dd\7h\2\2\u01db\u01dc\7\u0082"+
		"\2\2\u01dc\u01de\7$\2\2\u01dd\u01db\3\2\2\2\u01dd\u01de\3\2\2\2\u01de"+
		"\u01df\3\2\2\2\u01df\u01e4\5(\25\2\u01e0\u01e1\7\5\2\2\u01e1\u01e3\5("+
		"\25\2\u01e2\u01e0\3\2\2\2\u01e3\u01e6\3\2\2\2\u01e4\u01e2\3\2\2\2\u01e4"+
		"\u01e5\3\2\2\2\u01e5\u01e8\3\2\2\2\u01e6\u01e4\3\2\2\2\u01e7\u01e9\7\u00cf"+
		"\2\2\u01e8\u01e7\3\2\2\2\u01e8\u01e9\3\2\2\2\u01e9\u0347\3\2\2\2\u01ea"+
		"\u01eb\7p\2\2\u01eb\u01ec\7T\2\2\u01ec\u01ed\5\u008eH\2\u01ed\u01f0\7"+
		"h\2\2\u01ee\u01ef\7\u0082\2\2\u01ef\u01f1\7$\2\2\u01f0\u01ee\3\2\2\2\u01f0"+
		"\u01f1\3\2\2\2\u01f1\u01f2\3\2\2\2\u01f2\u01f7\5(\25\2\u01f3\u01f4\7\5"+
		"\2\2\u01f4\u01f6\5(\25\2\u01f5\u01f3\3\2\2\2\u01f6\u01f9\3\2\2\2\u01f7"+
		"\u01f5\3\2\2\2\u01f7\u01f8\3\2\2\2\u01f8\u0347\3\2\2\2\u01f9\u01f7\3\2"+
		"\2\2\u01fa\u01fb\7p\2\2\u01fb\u01fc\7R\2\2\u01fc\u01fe\5\u008eH\2\u01fd"+
		"\u01ff\5(\25\2\u01fe\u01fd\3\2\2\2\u01fe\u01ff\3\2\2\2\u01ff\u0200\3\2"+
		"\2\2\u0200\u0201\7v\2\2\u0201\u0202\5 \21\2\u0202\u0347\3\2\2\2\u0203"+
		"\u0204\7p\2\2\u0204\u0205\7R\2\2\u0205\u0206\5\u008eH\2\u0206\u0207\7"+
		"\u00e3\2\2\u0207\u0208\7f\2\2\u0208\u0347\3\2\2\2\u0209\u020a\7h\2\2\u020a"+
		"\u020d\7R\2\2\u020b\u020c\7\u0082\2\2\u020c\u020e\7$\2\2\u020d\u020b\3"+
		"\2\2\2\u020d\u020e\3\2\2\2\u020e\u020f\3\2\2\2\u020f\u0211\5\u008eH\2"+
		"\u0210\u0212\7\u00cf\2\2\u0211\u0210\3\2\2\2\u0211\u0212\3\2\2\2\u0212"+
		"\u0347\3\2\2\2\u0213\u0214\7h\2\2\u0214\u0217\7T\2\2\u0215\u0216\7\u0082"+
		"\2\2\u0216\u0218\7$\2\2\u0217\u0215\3\2\2\2\u0217\u0218\3\2\2\2\u0218"+
		"\u0219\3\2\2\2\u0219\u0347\5\u008eH\2\u021a\u021d\7Q\2\2\u021b\u021c\7"+
		"\37\2\2\u021c\u021e\7U\2\2\u021d\u021b\3\2\2\2\u021d\u021e\3\2\2\2\u021e"+
		"\u0223\3\2\2\2\u021f\u0221\7\u00b8\2\2\u0220\u021f\3\2\2\2\u0220\u0221"+
		"\3\2\2\2\u0221\u0222\3\2\2\2\u0222\u0224\7\u00b9\2\2\u0223\u0220\3\2\2"+
		"\2\u0223\u0224\3\2\2\2\u0224\u0225\3\2\2\2\u0225\u0229\7T\2\2\u0226\u0227"+
		"\7\u0082\2\2\u0227\u0228\7\"\2\2\u0228\u022a\7$\2\2\u0229\u0226\3\2\2"+
		"\2\u0229\u022a\3\2\2\2\u022a\u022b\3\2\2\2\u022b\u022d\5\u008eH\2\u022c"+
		"\u022e\5\u0080A\2\u022d\u022c\3\2\2\2\u022d\u022e\3\2\2\2\u022e\u0231"+
		"\3\2\2\2\u022f\u0230\7u\2\2\u0230\u0232\7\u00f3\2\2\u0231\u022f\3\2\2"+
		"\2\u0231\u0232\3\2\2\2\u0232\u0236\3\2\2\2\u0233\u0234\7\u00da\2\2\u0234"+
		"\u0235\7?\2\2\u0235\u0237\5x=\2\u0236\u0233\3\2\2\2\u0236\u0237\3\2\2"+
		"\2\u0237\u023a\3\2\2\2\u0238\u0239\7\u00bc\2\2\u0239\u023b\5\66\34\2\u023a"+
		"\u0238\3\2\2\2\u023a\u023b\3\2\2\2\u023b\u023c\3\2\2\2\u023c\u023d\7\20"+
		"\2\2\u023d\u023e\5\"\22\2\u023e\u0347\3\2\2\2\u023f\u0242\7Q\2\2\u0240"+
		"\u0241\7\37\2\2\u0241\u0243\7U\2\2\u0242\u0240\3\2\2\2\u0242\u0243\3\2"+
		"\2\2\u0243\u0245\3\2\2\2\u0244\u0246\7\u00b8\2\2\u0245\u0244\3\2\2\2\u0245"+
		"\u0246\3\2\2\2\u0246\u0247\3\2\2\2\u0247\u0248\7\u00b9\2\2\u0248\u0249"+
		"\7T\2\2\u0249\u024e\5\u008eH\2\u024a\u024b\7\3\2\2\u024b\u024c\5\u00b4"+
		"[\2\u024c\u024d\7\4\2\2\u024d\u024f\3\2\2\2\u024e\u024a\3\2\2\2\u024e"+
		"\u024f\3\2\2\2\u024f\u0250\3\2\2\2\u0250\u0253\5\64\33\2\u0251\u0252\7"+
		"\u00ba\2\2\u0252\u0254\5\66\34\2\u0253\u0251\3\2\2\2\u0253\u0254\3\2\2"+
		"\2\u0254\u0347\3\2\2\2\u0255\u0256\7p\2\2\u0256\u0257\7T\2\2\u0257\u0259"+
		"\5\u008eH\2\u0258\u025a\7\20\2\2\u0259\u0258\3\2\2\2\u0259\u025a\3\2\2"+
		"\2\u025a\u025b\3\2\2\2\u025b\u025c\5\"\22\2\u025c\u0347\3\2\2\2\u025d"+
		"\u0260\7Q\2\2\u025e\u025f\7\37\2\2\u025f\u0261\7U\2\2\u0260\u025e\3\2"+
		"\2\2\u0260\u0261\3\2\2\2\u0261\u0263\3\2\2\2\u0262\u0264\7\u00b9\2\2\u0263"+
		"\u0262\3\2\2\2\u0263\u0264\3\2\2\2\u0264\u0265\3\2\2\2\u0265\u0269\7\u00b0"+
		"\2\2\u0266\u0267\7\u0082\2\2\u0267\u0268\7\"\2\2\u0268\u026a\7$\2\2\u0269"+
		"\u0266\3\2\2\2\u0269\u026a\3\2\2\2\u026a\u026b\3\2\2\2\u026b\u026c\5\u00c8"+
		"e\2\u026c\u026d\7\20\2\2\u026d\u0277\7\u00f3\2\2\u026e\u026f\7\u00a2\2"+
		"\2\u026f\u0274\5H%\2\u0270\u0271\7\5\2\2\u0271\u0273\5H%\2\u0272\u0270"+
		"\3\2\2\2\u0273\u0276\3\2\2\2\u0274\u0272\3\2\2\2\u0274\u0275\3\2\2\2\u0275"+
		"\u0278\3\2\2\2\u0276\u0274\3\2\2\2\u0277\u026e\3\2\2\2\u0277\u0278\3\2"+
		"\2\2\u0278\u0347\3\2\2\2\u0279\u027b\7h\2\2\u027a\u027c\7\u00b9\2\2\u027b"+
		"\u027a\3\2\2\2\u027b\u027c\3\2\2\2\u027c\u027d\3\2\2\2\u027d\u0280\7\u00b0"+
		"\2\2\u027e\u027f\7\u0082\2\2\u027f\u0281\7$\2\2\u0280\u027e\3\2\2\2\u0280"+
		"\u0281\3\2\2\2\u0281\u0282\3\2\2\2\u0282\u0347\5\u00c8e\2\u0283\u0285"+
		"\7[\2\2\u0284\u0286\t\4\2\2\u0285\u0284\3\2\2\2\u0285\u0286\3\2\2\2\u0286"+
		"\u0287\3\2\2\2\u0287\u0347\5\16\b\2\u0288\u0289\7a\2\2\u0289\u028c\7b"+
		"\2\2\u028a\u028b\t\5\2\2\u028b\u028d\5\u00caf\2\u028c\u028a\3\2\2\2\u028c"+
		"\u028d\3\2\2\2\u028d\u0292\3\2\2\2\u028e\u0290\7&\2\2\u028f\u028e\3\2"+
		"\2\2\u028f\u0290\3\2\2\2\u0290\u0291\3\2\2\2\u0291\u0293\7\u00f3\2\2\u0292"+
		"\u028f\3\2\2\2\u0292\u0293\3\2\2\2\u0293\u0347\3\2\2\2\u0294\u0295\7a"+
		"\2\2\u0295\u0296\7R\2\2\u0296\u0299\7\u00b1\2\2\u0297\u0298\t\5\2\2\u0298"+
		"\u029a\5\u00caf\2\u0299\u0297\3\2\2\2\u0299\u029a\3\2\2\2\u029a\u029b"+
		"\3\2\2\2\u029b\u029c\7&\2\2\u029c\u029e\7\u00f3\2\2\u029d\u029f\5(\25"+
		"\2\u029e\u029d\3\2\2\2\u029e\u029f\3\2\2\2\u029f\u0347\3\2\2\2\u02a0\u02a1"+
		"\7a\2\2\u02a1\u02a6\7\u00d3\2\2\u02a2\u02a4\7&\2\2\u02a3\u02a2\3\2\2\2"+
		"\u02a3\u02a4\3\2\2\2\u02a4\u02a5\3\2\2\2\u02a5\u02a7\7\u00f3\2\2\u02a6"+
		"\u02a3\3\2\2\2\u02a6\u02a7\3\2\2\2\u02a7\u0347\3\2\2\2\u02a8\u02a9\7a"+
		"\2\2\u02a9\u02aa\7\u00bc\2\2\u02aa\u02af\5\u008eH\2\u02ab\u02ac\7\3\2"+
		"\2\u02ac\u02ad\5:\36\2\u02ad\u02ae\7\4\2\2\u02ae\u02b0\3\2\2\2\u02af\u02ab"+
		"\3\2\2\2\u02af\u02b0\3\2\2\2\u02b0\u0347\3\2\2\2\u02b1\u02b2\7a\2\2\u02b2"+
		"\u02b3\7c\2\2\u02b3\u02b4\t\5\2\2\u02b4\u02b7\5\u008eH\2\u02b5\u02b6\t"+
		"\5\2\2\u02b6\u02b8\5\u00caf\2\u02b7\u02b5\3\2\2\2\u02b7\u02b8\3\2\2\2"+
		"\u02b8\u0347\3\2\2\2\u02b9\u02ba\7a\2\2\u02ba\u02bb\7f\2\2\u02bb\u02bd"+
		"\5\u008eH\2\u02bc\u02be\5(\25\2\u02bd\u02bc\3\2\2\2\u02bd\u02be\3\2\2"+
		"\2\u02be\u0347\3\2\2\2\u02bf\u02c1\7a\2\2\u02c0\u02c2\5\u00caf\2\u02c1"+
		"\u02c0\3\2\2\2\u02c1\u02c2\3\2\2\2\u02c2\u02c3\3\2\2\2\u02c3\u02cb\7g"+
		"\2\2\u02c4\u02c6\7&\2\2\u02c5\u02c4\3\2\2\2\u02c5\u02c6\3\2\2\2\u02c6"+
		"\u02c9\3\2\2\2\u02c7\u02ca\5\u00c8e\2\u02c8\u02ca\7\u00f3\2\2\u02c9\u02c7"+
		"\3\2\2\2\u02c9\u02c8\3\2\2\2\u02ca\u02cc\3\2\2\2\u02cb\u02c5\3\2\2\2\u02cb"+
		"\u02cc\3\2\2\2\u02cc\u0347\3\2\2\2\u02cd\u02ce\7a\2\2\u02ce\u02cf\7Q\2"+
		"\2\u02cf\u02d0\7R\2\2\u02d0\u0347\5\u008eH\2\u02d1\u02d2\t\6\2\2\u02d2"+
		"\u02d4\7\u00b0\2\2\u02d3\u02d5\7\u00b1\2\2\u02d4\u02d3\3\2\2\2\u02d4\u02d5"+
		"\3\2\2\2\u02d5\u02d6\3\2\2\2\u02d6\u0347\5,\27\2\u02d7\u02d8\t\6\2\2\u02d8"+
		"\u02da\7\u00d2\2\2\u02d9\u02db\7\u00b1\2\2\u02da\u02d9\3\2\2\2\u02da\u02db"+
		"\3\2\2\2\u02db\u02dc\3\2\2\2\u02dc\u0347\5\u00caf\2\u02dd\u02df\t\6\2"+
		"\2\u02de\u02e0\7R\2\2\u02df\u02de\3\2\2\2\u02df\u02e0\3\2\2\2\u02e0\u02e2"+
		"\3\2\2\2\u02e1\u02e3\t\7\2\2\u02e2\u02e1\3\2\2\2\u02e2\u02e3\3\2\2\2\u02e3"+
		"\u02e4\3\2\2\2\u02e4\u02e6\5\u008eH\2\u02e5\u02e7\5(\25\2\u02e6\u02e5"+
		"\3\2\2\2\u02e6\u02e7\3\2\2\2\u02e7\u02e9\3\2\2\2\u02e8\u02ea\5.\30\2\u02e9"+
		"\u02e8\3\2\2\2\u02e9\u02ea\3\2\2\2\u02ea\u0347\3\2\2\2\u02eb\u02ec\7\u00b2"+
		"\2\2\u02ec\u02ed\7R\2\2\u02ed\u0347\5\u008eH\2\u02ee\u02f6\7\u00b2\2\2"+
		"\u02ef\u02f7\7\u00f3\2\2\u02f0\u02f2\13\2\2\2\u02f1\u02f0\3\2\2\2\u02f2"+
		"\u02f5\3\2\2\2\u02f3\u02f4\3\2\2\2\u02f3\u02f1\3\2\2\2\u02f4\u02f7\3\2"+
		"\2\2\u02f5\u02f3\3\2\2\2\u02f6\u02ef\3\2\2\2\u02f6\u02f3\3\2\2\2\u02f7"+
		"\u0347\3\2\2\2\u02f8\u02fa\7\u00b4\2\2\u02f9\u02fb\7\u00b6\2\2\u02fa\u02f9"+
		"\3\2\2\2\u02fa\u02fb\3\2\2\2\u02fb\u02fc\3\2\2\2\u02fc\u02fd\7R\2\2\u02fd"+
		"\u0302\5\u008eH\2\u02fe\u0300\7\20\2\2\u02ff\u02fe\3\2\2\2\u02ff\u0300"+
		"\3\2\2\2\u0300\u0301\3\2\2\2\u0301\u0303\5\"\22\2\u0302\u02ff\3\2\2\2"+
		"\u0302\u0303\3\2\2\2\u0303\u0347\3\2\2\2\u0304\u0305\7\u00b5\2\2\u0305"+
		"\u0308\7R\2\2\u0306\u0307\7\u0082\2\2\u0307\u0309\7$\2\2\u0308\u0306\3"+
		"\2\2\2\u0308\u0309\3\2\2\2\u0309\u030a\3\2\2\2\u030a\u0347\5\u008eH\2"+
		"\u030b\u030c\7\u00b3\2\2\u030c\u0347\7\u00b4\2\2\u030d\u030e\7\u00e6\2"+
		"\2\u030e\u0310\7x\2\2\u030f\u0311\7\u00f1\2\2\u0310\u030f\3\2\2\2\u0310"+
		"\u0311\3\2\2\2\u0311\u0312\3\2\2\2\u0312\u0313\7\u00f2\2\2\u0313\u0315"+
		"\7\u00f3\2\2\u0314\u0316\7\u009f\2\2\u0315\u0314\3\2\2\2\u0315\u0316\3"+
		"\2\2\2\u0316\u0317\3\2\2\2\u0317\u0318\7Y\2\2\u0318\u0319\7R\2\2\u0319"+
		"\u031b\5\u008eH\2\u031a\u031c\5(\25\2\u031b\u031a\3\2\2\2\u031b\u031c"+
		"\3\2\2\2\u031c\u0347\3\2\2\2\u031d\u031e\7\u00d5\2\2\u031e\u031f\7R\2"+
		"\2\u031f\u0321\5\u008eH\2\u0320\u0322\5(\25\2\u0321\u0320\3\2\2\2\u0321"+
		"\u0322\3\2\2\2\u0322\u0347\3\2\2\2\u0323\u0324\7\u00e1\2\2\u0324\u0325"+
		"\7\u00e2\2\2\u0325\u0326\7R\2\2\u0326\u0347\5\u008eH\2\u0327\u0328\t\b"+
		"\2\2\u0328\u032c\5\u00caf\2\u0329\u032b\13\2\2\2\u032a\u0329\3\2\2\2\u032b"+
		"\u032e\3\2\2\2\u032c\u032d\3\2\2\2\u032c\u032a\3\2\2\2\u032d\u0347\3\2"+
		"\2\2\u032e\u032c\3\2\2\2\u032f\u0330\7v\2\2\u0330\u0334\7\u00e7\2\2\u0331"+
		"\u0333\13\2\2\2\u0332\u0331\3\2\2\2\u0333\u0336\3\2\2\2\u0334\u0335\3"+
		"\2\2\2\u0334\u0332\3\2\2\2\u0335\u0347\3\2\2\2\u0336\u0334\3\2\2\2\u0337"+
		"\u033b\7v\2\2\u0338\u033a\13\2\2\2\u0339\u0338\3\2\2\2\u033a\u033d\3\2"+
		"\2\2\u033b\u033c\3\2\2\2\u033b\u0339\3\2\2\2\u033c\u0347\3\2\2\2\u033d"+
		"\u033b\3\2\2\2\u033e\u0347\7w\2\2\u033f\u0343\5\20\t\2\u0340\u0342\13"+
		"\2\2\2\u0341\u0340\3\2\2\2\u0342\u0345\3\2\2\2\u0343\u0344\3\2\2\2\u0343"+
		"\u0341\3\2\2\2\u0344\u0347\3\2\2\2\u0345\u0343\3\2\2\2\u0346\u00e6\3\2"+
		"\2\2\u0346\u00e7\3\2\2\2\u0346\u00e8\3\2\2\2\u0346\u00e9\3\2\2\2\u0346"+
		"\u00eb\3\2\2\2\u0346\u00ff\3\2\2\2\u0346\u0106\3\2\2\2\u0346\u0110\3\2"+
		"\2\2\u0346\u012e\3\2\2\2\u0346\u014f\3\2\2\2\u0346\u015c\3\2\2\2\u0346"+
		"\u016a\3\2\2\2\u0346\u0173\3\2\2\2\u0346\u017a\3\2\2\2\u0346\u0181\3\2"+
		"\2\2\u0346\u018c\3\2\2\2\u0346\u019b\3\2\2\2\u0346\u01a9\3\2\2\2\u0346"+
		"\u01b3\3\2\2\2\u0346\u01c1\3\2\2\2\u0346\u01cf\3\2\2\2\u0346\u01d7\3\2"+
		"\2\2\u0346\u01ea\3\2\2\2\u0346\u01fa\3\2\2\2\u0346\u0203\3\2\2\2\u0346"+
		"\u0209\3\2\2\2\u0346\u0213\3\2\2\2\u0346\u021a\3\2\2\2\u0346\u023f\3\2"+
		"\2\2\u0346\u0255\3\2\2\2\u0346\u025d\3\2\2\2\u0346\u0279\3\2\2\2\u0346"+
		"\u0283\3\2\2\2\u0346\u0288\3\2\2\2\u0346\u0294\3\2\2\2\u0346\u02a0\3\2"+
		"\2\2\u0346\u02a8\3\2\2\2\u0346\u02b1\3\2\2\2\u0346\u02b9\3\2\2\2\u0346"+
		"\u02bf\3\2\2\2\u0346\u02cd\3\2\2\2\u0346\u02d1\3\2\2\2\u0346\u02d7\3\2"+
		"\2\2\u0346\u02dd\3\2\2\2\u0346\u02eb\3\2\2\2\u0346\u02ee\3\2\2\2\u0346"+
		"\u02f8\3\2\2\2\u0346\u0304\3\2\2\2\u0346\u030b\3\2\2\2\u0346\u030d\3\2"+
		"\2\2\u0346\u031d\3\2\2\2\u0346\u0323\3\2\2\2\u0346\u0327\3\2\2\2\u0346"+
		"\u032f\3\2\2\2\u0346\u0337\3\2\2\2\u0346\u033e\3\2\2\2\u0346\u033f\3\2"+
		"\2\2\u0347\17\3\2\2\2\u0348\u0349\7Q\2\2\u0349\u03f1\7\u00e7\2\2\u034a"+
		"\u034b\7h\2\2\u034b\u03f1\7\u00e7\2\2\u034c\u034e\7\u00de\2\2\u034d\u034f"+
		"\7\u00e7\2\2\u034e\u034d\3\2\2\2\u034e\u034f\3\2\2\2\u034f\u03f1\3\2\2"+
		"\2\u0350\u0352\7\u00dd\2\2\u0351\u0353\7\u00e7\2\2\u0352\u0351\3\2\2\2"+
		"\u0352\u0353\3\2\2\2\u0353\u03f1\3\2\2\2\u0354\u0355\7a\2\2\u0355\u03f1"+
		"\7\u00de\2\2\u0356\u0357\7a\2\2\u0357\u0359\7\u00e7\2\2\u0358\u035a\7"+
		"\u00de\2\2\u0359\u0358\3\2\2\2\u0359\u035a\3\2\2\2\u035a\u03f1\3\2\2\2"+
		"\u035b\u035c\7a\2\2\u035c\u03f1\7\u00ea\2\2\u035d\u035e\7a\2\2\u035e\u03f1"+
		"\7\u00e8\2\2\u035f\u0360\7a\2\2\u0360\u0361\7J\2\2\u0361\u03f1\7\u00e8"+
		"\2\2\u0362\u0363\7\u00e4\2\2\u0363\u03f1\7R\2\2\u0364\u0365\7\u00e5\2"+
		"\2\u0365\u03f1\7R\2\2\u0366\u0367\7a\2\2\u0367\u03f1\7\u00e9\2\2\u0368"+
		"\u0369\7a\2\2\u0369\u036a\7Q\2\2\u036a\u03f1\7R\2\2\u036b\u036c\7a\2\2"+
		"\u036c\u03f1\7\u00eb\2\2\u036d\u036e\7a\2\2\u036e\u03f1\7\u00ed\2\2\u036f"+
		"\u0370\7a\2\2\u0370\u03f1\7\u00ee\2\2\u0371\u0372\7Q\2\2\u0372\u03f1\7"+
		"\u00ec\2\2\u0373\u0374\7h\2\2\u0374\u03f1\7\u00ec\2\2\u0375\u0376\7p\2"+
		"\2\u0376\u03f1\7\u00ec\2\2\u0377\u0378\7\u00df\2\2\u0378\u03f1\7R\2\2"+
		"\u0379\u037a\7\u00df\2\2\u037a\u03f1\7\u00d2\2\2\u037b\u037c\7\u00e0\2"+
		"\2\u037c\u03f1\7R\2\2\u037d\u037e\7\u00e0\2\2\u037e\u03f1\7\u00d2\2\2"+
		"\u037f\u0380\7Q\2\2\u0380\u0381\7\u00b9\2\2\u0381\u03f1\7}\2\2\u0382\u0383"+
		"\7h\2\2\u0383\u0384\7\u00b9\2\2\u0384\u03f1\7}\2\2\u0385\u0386\7p\2\2"+
		"\u0386\u0387\7R\2\2\u0387\u0388\5\u008eH\2\u0388\u0389\7\"\2\2\u0389\u038a"+
		"\7\u00cd\2\2\u038a\u03f1\3\2\2\2\u038b\u038c\7p\2\2\u038c\u038d\7R\2\2"+
		"\u038d\u038e\5\u008eH\2\u038e\u038f\7\u00cd\2\2\u038f\u0390\7\26\2\2\u0390"+
		"\u03f1\3\2\2\2\u0391\u0392\7p\2\2\u0392\u0393\7R\2\2\u0393\u0394\5\u008e"+
		"H\2\u0394\u0395\7\"\2\2\u0395\u0396\7\u00ce\2\2\u0396\u03f1\3\2\2\2\u0397"+
		"\u0398\7p\2\2\u0398\u0399\7R\2\2\u0399\u039a\5\u008eH\2\u039a\u039b\7"+
		"\u00bf\2\2\u039b\u039c\7\26\2\2\u039c\u03f1\3\2\2\2\u039d\u039e\7p\2\2"+
		"\u039e\u039f\7R\2\2\u039f\u03a0\5\u008eH\2\u03a0\u03a1\7\"\2\2\u03a1\u03a2"+
		"\7\u00bf\2\2\u03a2\u03f1\3\2\2\2\u03a3\u03a4\7p\2\2\u03a4\u03a5\7R\2\2"+
		"\u03a5\u03a6\5\u008eH\2\u03a6\u03a7\7\"\2\2\u03a7\u03a8\7\u00c0\2\2\u03a8"+
		"\u03a9\7\20\2\2\u03a9\u03aa\7\u00c1\2\2\u03aa\u03f1\3\2\2\2\u03ab\u03ac"+
		"\7p\2\2\u03ac\u03ad\7R\2\2\u03ad\u03ae\5\u008eH\2\u03ae\u03af\7v\2\2\u03af"+
		"\u03b0\7\u00bf\2\2\u03b0\u03b1\7\u00c2\2\2\u03b1\u03f1\3\2\2\2\u03b2\u03b3"+
		"\7p\2\2\u03b3\u03b4\7R\2\2\u03b4\u03b5\5\u008eH\2\u03b5\u03b6\7\u00c3"+
		"\2\2\u03b6\u03b7\7D\2\2\u03b7\u03f1\3\2\2\2\u03b8\u03b9\7p\2\2\u03b9\u03ba"+
		"\7R\2\2\u03ba\u03bb\5\u008eH\2\u03bb\u03bc\7\u00c4\2\2\u03bc\u03bd\7D"+
		"\2\2\u03bd\u03f1\3\2\2\2\u03be\u03bf\7p\2\2\u03bf\u03c0\7R\2\2\u03c0\u03c1"+
		"\5\u008eH\2\u03c1\u03c2\7\u00c5\2\2\u03c2\u03c3\7D\2\2\u03c3\u03f1\3\2"+
		"\2\2\u03c4\u03c5\7p\2\2\u03c5\u03c6\7R\2\2\u03c6\u03c7\5\u008eH\2\u03c7"+
		"\u03c8\7\u00c7\2\2\u03c8\u03f1\3\2\2\2\u03c9\u03ca\7p\2\2\u03ca\u03cb"+
		"\7R\2\2\u03cb\u03cd\5\u008eH\2\u03cc\u03ce\5(\25\2\u03cd\u03cc\3\2\2\2"+
		"\u03cd\u03ce\3\2\2\2\u03ce\u03cf\3\2\2\2\u03cf\u03d0\7\u00c8\2\2\u03d0"+
		"\u03f1\3\2\2\2\u03d1\u03d2\7p\2\2\u03d2\u03d3\7R\2\2\u03d3\u03d5\5\u008e"+
		"H\2\u03d4\u03d6\5(\25\2\u03d5\u03d4\3\2\2\2\u03d5\u03d6\3\2\2\2\u03d6"+
		"\u03d7\3\2\2\2\u03d7\u03d8\7\u00c9\2\2\u03d8\u03f1\3\2\2\2\u03d9\u03da"+
		"\7p\2\2\u03da\u03db\7R\2\2\u03db\u03dd\5\u008eH\2\u03dc\u03de\5(\25\2"+
		"\u03dd\u03dc\3\2\2\2\u03dd\u03de\3\2\2\2\u03de\u03df\3\2\2\2\u03df\u03e0"+
		"\7v\2\2\u03e0\u03e1\7\u00c6\2\2\u03e1\u03f1\3\2\2\2\u03e2\u03e3\7p\2\2"+
		"\u03e3\u03e4\7R\2\2\u03e4\u03e6\5\u008eH\2\u03e5\u03e7\5(\25\2\u03e6\u03e5"+
		"\3\2\2\2\u03e6\u03e7\3\2\2\2\u03e7\u03e8\3\2\2\2\u03e8\u03e9\7U\2\2\u03e9"+
		"\u03ea\7c\2\2\u03ea\u03f1\3\2\2\2\u03eb\u03ec\7y\2\2\u03ec\u03f1\7z\2"+
		"\2\u03ed\u03f1\7{\2\2\u03ee\u03f1\7|\2\2\u03ef\u03f1\7\u00d4\2\2\u03f0"+
		"\u0348\3\2\2\2\u03f0\u034a\3\2\2\2\u03f0\u034c\3\2\2\2\u03f0\u0350\3\2"+
		"\2\2\u03f0\u0354\3\2\2\2\u03f0\u0356\3\2\2\2\u03f0\u035b\3\2\2\2\u03f0"+
		"\u035d\3\2\2\2\u03f0\u035f\3\2\2\2\u03f0\u0362\3\2\2\2\u03f0\u0364\3\2"+
		"\2\2\u03f0\u0366\3\2\2\2\u03f0\u0368\3\2\2\2\u03f0\u036b\3\2\2\2\u03f0"+
		"\u036d\3\2\2\2\u03f0\u036f\3\2\2\2\u03f0\u0371\3\2\2\2\u03f0\u0373\3\2"+
		"\2\2\u03f0\u0375\3\2\2\2\u03f0\u0377\3\2\2\2\u03f0\u0379\3\2\2\2\u03f0"+
		"\u037b\3\2\2\2\u03f0\u037d\3\2\2\2\u03f0\u037f\3\2\2\2\u03f0\u0382\3\2"+
		"\2\2\u03f0\u0385\3\2\2\2\u03f0\u038b\3\2\2\2\u03f0\u0391\3\2\2\2\u03f0"+
		"\u0397\3\2\2\2\u03f0\u039d\3\2\2\2\u03f0\u03a3\3\2\2\2\u03f0\u03ab\3\2"+
		"\2\2\u03f0\u03b2\3\2\2\2\u03f0\u03b8\3\2\2\2\u03f0\u03be\3\2\2\2\u03f0"+
		"\u03c4\3\2\2\2\u03f0\u03c9\3\2\2\2\u03f0\u03d1\3\2\2\2\u03f0\u03d9\3\2"+
		"\2\2\u03f0\u03e2\3\2\2\2\u03f0\u03eb\3\2\2\2\u03f0\u03ed\3\2\2\2\u03f0"+
		"\u03ee\3\2\2\2\u03f0\u03ef\3\2\2\2\u03f1\21\3\2\2\2\u03f2\u03f3\7X\2\2"+
		"\u03f3\u03f4\7\16\2\2\u03f4\u03f7\5\u008eH\2\u03f5\u03f6\7\24\2\2\u03f6"+
		"\u03f8\5\u0098M\2\u03f7\u03f5\3\2\2\2\u03f7\u03f8\3\2\2\2\u03f8\23\3\2"+
		"\2\2\u03f9\u03fa\7W\2\2\u03fa\u03fb\5\u008eH\2\u03fb\u03fc\7v\2\2\u03fc"+
		"\u03ff\5\26\f\2\u03fd\u03fe\7\24\2\2\u03fe\u0400\5\u0098M\2\u03ff\u03fd"+
		"\3\2\2\2\u03ff\u0400\3\2\2\2\u0400\25\3\2\2\2\u0401\u0406\5\30\r\2\u0402"+
		"\u0403\7\5\2\2\u0403\u0405\5\30\r\2\u0404\u0402\3\2\2\2\u0405\u0408\3"+
		"\2\2\2\u0406\u0404\3\2\2\2\u0406\u0407\3\2\2\2\u0407\27\3\2\2\2\u0408"+
		"\u0406\3\2\2\2\u0409\u040a\5\u00caf\2\u040a\u040b\7\u0085\2\2\u040b\u040c"+
		"\5\u009cO\2\u040c\31\3\2\2\2\u040d\u040f\7Q\2\2\u040e\u0410\7\u00b9\2"+
		"\2\u040f\u040e\3\2\2\2\u040f\u0410\3\2\2\2\u0410\u0412\3\2\2\2\u0411\u0413"+
		"\7\u00db\2\2\u0412\u0411\3\2\2\2\u0412\u0413\3\2\2\2\u0413\u0414\3\2\2"+
		"\2\u0414\u0418\7R\2\2\u0415\u0416\7\u0082\2\2\u0416\u0417\7\"\2\2\u0417"+
		"\u0419\7$\2\2\u0418\u0415\3\2\2\2\u0418\u0419\3\2\2\2\u0419\u041a\3\2"+
		"\2\2\u041a\u041b\5\u008eH\2\u041b\33\3\2\2\2\u041c\u041d\7\u00cd\2\2\u041d"+
		"\u041e\7\26\2\2\u041e\u0422\5x=\2\u041f\u0420\7\u00ce\2\2\u0420\u0421"+
		"\7\26\2\2\u0421\u0423\5|?\2\u0422\u041f\3\2\2\2\u0422\u0423\3\2\2\2\u0423"+
		"\u0424\3\2\2\2\u0424\u0425\7Y\2\2\u0425\u0426\7\u00f7\2\2\u0426\u0427"+
		"\7\u00be\2\2\u0427\35\3\2\2\2\u0428\u0429\7\u00bf\2\2\u0429\u042a\7\26"+
		"\2\2\u042a\u042b\5x=\2\u042b\u042e\7?\2\2\u042c\u042f\5> \2\u042d\u042f"+
		"\5@!\2\u042e\u042c\3\2\2\2\u042e\u042d\3\2\2\2\u042f\u0433\3\2\2\2\u0430"+
		"\u0431\7\u00c0\2\2\u0431\u0432\7\20\2\2\u0432\u0434\7\u00c1\2\2\u0433"+
		"\u0430\3\2\2\2\u0433\u0434\3\2\2\2\u0434\37\3\2\2\2\u0435\u0436\7\u00c2"+
		"\2\2\u0436\u0437\7\u00f3\2\2\u0437!\3\2\2\2\u0438\u043a\5\60\31\2\u0439"+
		"\u0438\3\2\2\2\u0439\u043a\3\2\2\2\u043a\u043b\3\2\2\2\u043b\u043c\5J"+
		"&\2\u043c#\3\2\2\2\u043d\u043e\7V\2\2\u043e\u043f\7\u009f\2\2\u043f\u0440"+
		"\7R\2\2\u0440\u0447\5\u008eH\2\u0441\u0445\5(\25\2\u0442\u0443\7\u0082"+
		"\2\2\u0443\u0444\7\"\2\2\u0444\u0446\7$\2\2\u0445\u0442\3\2\2\2\u0445"+
		"\u0446\3\2\2\2\u0446\u0448\3\2\2\2\u0447\u0441\3\2\2\2\u0447\u0448\3\2"+
		"\2\2\u0448\u046e\3\2\2\2\u0449\u044a\7V\2\2\u044a\u044c\7Y\2\2\u044b\u044d"+
		"\7R\2\2\u044c\u044b\3\2\2\2\u044c\u044d\3\2\2\2\u044d\u044e\3\2\2\2\u044e"+
		"\u0450\5\u008eH\2\u044f\u0451\5(\25\2\u0450\u044f\3\2\2\2\u0450\u0451"+
		"\3\2\2\2\u0451\u046e\3\2\2\2\u0452\u0453\7V\2\2\u0453\u0455\7\u009f\2"+
		"\2\u0454\u0456\7\u00f1\2\2\u0455\u0454\3\2\2\2\u0455\u0456\3\2\2\2\u0456"+
		"\u0457\3\2\2\2\u0457\u0458\7S\2\2\u0458\u045a\7\u00f3\2\2\u0459\u045b"+
		"\5\u008cG\2\u045a\u0459\3\2\2\2\u045a\u045b\3\2\2\2\u045b\u045d\3\2\2"+
		"\2\u045c\u045e\5B\"\2\u045d\u045c\3\2\2\2\u045d\u045e\3\2\2\2\u045e\u046e"+
		"\3\2\2\2\u045f\u0460\7V\2\2\u0460\u0462\7\u009f\2\2\u0461\u0463\7\u00f1"+
		"\2\2\u0462\u0461\3\2\2\2\u0462\u0463\3\2\2\2\u0463\u0464\3\2\2\2\u0464"+
		"\u0466\7S\2\2\u0465\u0467\7\u00f3\2\2\u0466\u0465\3\2\2\2\u0466\u0467"+
		"\3\2\2\2\u0467\u0468\3\2\2\2\u0468\u046b\5\64\33\2\u0469\u046a\7\u00ba"+
		"\2\2\u046a\u046c\5\66\34\2\u046b\u0469\3\2\2\2\u046b\u046c\3\2\2\2\u046c"+
		"\u046e\3\2\2\2\u046d\u043d\3\2\2\2\u046d\u0449\3\2\2\2\u046d\u0452\3\2"+
		"\2\2\u046d\u045f\3\2\2\2\u046e%\3\2\2\2\u046f\u0471\5(\25\2\u0470\u0472"+
		"\5 \21\2\u0471\u0470\3\2\2\2\u0471\u0472\3\2\2\2\u0472\'\3\2\2\2\u0473"+
		"\u0474\7D\2\2\u0474\u0475\7\3\2\2\u0475\u047a\5*\26\2\u0476\u0477\7\5"+
		"\2\2\u0477\u0479\5*\26\2\u0478\u0476\3\2\2\2\u0479\u047c\3\2\2\2\u047a"+
		"\u0478\3\2\2\2\u047a\u047b\3\2\2\2\u047b\u047d\3\2\2\2\u047c\u047a\3\2"+
		"\2\2\u047d\u047e\7\4\2\2\u047e)\3\2\2\2\u047f\u0482\5\u00caf\2\u0480\u0481"+
		"\7\u0085\2\2\u0481\u0483\5\u00a0Q\2\u0482\u0480\3\2\2\2\u0482\u0483\3"+
		"\2\2\2\u0483+\3\2\2\2\u0484\u048a\5\u00c8e\2\u0485\u048a\7\u00f3\2\2\u0486"+
		"\u048a\5\u00a2R\2\u0487\u048a\5\u00a4S\2\u0488\u048a\5\u00a6T\2\u0489"+
		"\u0484\3\2\2\2\u0489\u0485\3\2\2\2\u0489\u0486\3\2\2\2\u0489\u0487\3\2"+
		"\2\2\u0489\u0488\3\2\2\2\u048a-\3\2\2\2\u048b\u0490\5\u00caf\2\u048c\u048d"+
		"\7\6\2\2\u048d\u048f\5\u00caf\2\u048e\u048c\3\2\2\2\u048f\u0492\3\2\2"+
		"\2\u0490\u048e\3\2\2\2\u0490\u0491\3\2\2\2\u0491/\3\2\2\2\u0492\u0490"+
		"\3\2\2\2\u0493\u0494\7O\2\2\u0494\u0499\5\62\32\2\u0495\u0496\7\5\2\2"+
		"\u0496\u0498\5\62\32\2\u0497\u0495\3\2\2\2\u0498\u049b\3\2\2\2\u0499\u0497"+
		"\3\2\2\2\u0499\u049a\3\2\2\2\u049a\61\3\2\2\2\u049b\u0499\3\2\2\2\u049c"+
		"\u049e\5\u00caf\2\u049d\u049f\7\20\2\2\u049e\u049d\3\2\2\2\u049e\u049f"+
		"\3\2\2\2\u049f\u04a0\3\2\2\2\u04a0\u04a1\7\3\2\2\u04a1\u04a2\5\"\22\2"+
		"\u04a2\u04a3\7\4\2\2\u04a3\63\3\2\2\2\u04a4\u04a5\7\u00a2\2\2\u04a5\u04a6"+
		"\5\u00c8e\2\u04a6\65\3\2\2\2\u04a7\u04a8\7\3\2\2\u04a8\u04ad\58\35\2\u04a9"+
		"\u04aa\7\5\2\2\u04aa\u04ac\58\35\2\u04ab\u04a9\3\2\2\2\u04ac\u04af\3\2"+
		"\2\2\u04ad\u04ab\3\2\2\2\u04ad\u04ae\3\2\2\2\u04ae\u04b0\3\2\2\2\u04af"+
		"\u04ad\3\2\2\2\u04b0\u04b1\7\4\2\2\u04b1\67\3\2\2\2\u04b2\u04b7\5:\36"+
		"\2\u04b3\u04b5\7\u0085\2\2\u04b4\u04b3\3\2\2\2\u04b4\u04b5\3\2\2\2\u04b5"+
		"\u04b6\3\2\2\2\u04b6\u04b8\5<\37\2\u04b7\u04b4\3\2\2\2\u04b7\u04b8\3\2"+
		"\2\2\u04b89\3\2\2\2\u04b9\u04be\5\u00caf\2\u04ba\u04bb\7\6\2\2\u04bb\u04bd"+
		"\5\u00caf\2\u04bc\u04ba\3\2\2\2\u04bd\u04c0\3\2\2\2\u04be\u04bc\3\2\2"+
		"\2\u04be\u04bf\3\2\2\2\u04bf\u04c3\3\2\2\2\u04c0\u04be\3\2\2\2\u04c1\u04c3"+
		"\7\u00f3\2\2\u04c2\u04b9\3\2\2\2\u04c2\u04c1\3\2\2\2\u04c3;\3\2\2\2\u04c4"+
		"\u04c9\7\u00f7\2\2\u04c5\u04c9\7\u00f8\2\2\u04c6\u04c9\5\u00a8U\2\u04c7"+
		"\u04c9\7\u00f3\2\2\u04c8\u04c4\3\2\2\2\u04c8\u04c5\3\2\2\2\u04c8\u04c6"+
		"\3\2\2\2\u04c8\u04c7\3\2\2\2\u04c9=\3\2\2\2\u04ca\u04cb\7\3\2\2\u04cb"+
		"\u04d0\5\u00a0Q\2\u04cc\u04cd\7\5\2\2\u04cd\u04cf\5\u00a0Q\2\u04ce\u04cc"+
		"\3\2\2\2\u04cf\u04d2\3\2\2\2\u04d0\u04ce\3\2\2\2\u04d0\u04d1\3\2\2\2\u04d1"+
		"\u04d3\3\2\2\2\u04d2\u04d0\3\2\2\2\u04d3\u04d4\7\4\2\2\u04d4?\3\2\2\2"+
		"\u04d5\u04d6\7\3\2\2\u04d6\u04db\5> \2\u04d7\u04d8\7\5\2\2\u04d8\u04da"+
		"\5> \2\u04d9\u04d7\3\2\2\2\u04da\u04dd\3\2\2\2\u04db\u04d9\3\2\2\2\u04db"+
		"\u04dc\3\2\2\2\u04dc\u04de\3\2\2\2\u04dd\u04db\3\2\2\2\u04de\u04df\7\4"+
		"\2\2\u04dfA\3\2\2\2\u04e0\u04e1\7\u00c0\2\2\u04e1\u04e2\7\20\2\2\u04e2"+
		"\u04e7\5D#\2\u04e3\u04e4\7\u00c0\2\2\u04e4\u04e5\7\26\2\2\u04e5\u04e7"+
		"\5F$\2\u04e6\u04e0\3\2\2\2\u04e6\u04e3\3\2\2\2\u04e7C\3\2\2\2\u04e8\u04e9"+
		"\7\u00d0\2\2\u04e9\u04ea\7\u00f3\2\2\u04ea\u04eb\7\u00d1\2\2\u04eb\u04ee"+
		"\7\u00f3\2\2\u04ec\u04ee\5\u00caf\2\u04ed\u04e8\3\2\2\2\u04ed\u04ec\3"+
		"\2\2\2\u04eeE\3\2\2\2\u04ef\u04f3\7\u00f3\2\2\u04f0\u04f1\7O\2\2\u04f1"+
		"\u04f2\7\u00a4\2\2\u04f2\u04f4\5\66\34\2\u04f3\u04f0\3\2\2\2\u04f3\u04f4"+
		"\3\2\2\2\u04f4G\3\2\2\2\u04f5\u04f6\5\u00caf\2\u04f6\u04f7\7\u00f3\2\2"+
		"\u04f7I\3\2\2\2\u04f8\u04fa\5$\23\2\u04f9\u04f8\3\2\2\2\u04f9\u04fa\3"+
		"\2\2\2\u04fa\u04fb\3\2\2\2\u04fb\u04fc\5P)\2\u04fc\u04fd\5L\'\2\u04fd"+
		"\u0505\3\2\2\2\u04fe\u0500\5\\/\2\u04ff\u0501\5N(\2\u0500\u04ff\3\2\2"+
		"\2\u0501\u0502\3\2\2\2\u0502\u0500\3\2\2\2\u0502\u0503\3\2\2\2\u0503\u0505"+
		"\3\2\2\2\u0504\u04f9\3\2\2\2\u0504\u04fe\3\2\2\2\u0505K\3\2\2\2\u0506"+
		"\u0507\7\33\2\2\u0507\u0508\7\26\2\2\u0508\u050d\5T+\2\u0509\u050a\7\5"+
		"\2\2\u050a\u050c\5T+\2\u050b\u0509\3\2\2\2\u050c\u050f\3\2\2\2\u050d\u050b"+
		"\3\2\2\2\u050d\u050e\3\2\2\2\u050e\u0511\3\2\2\2\u050f\u050d\3\2\2\2\u0510"+
		"\u0506\3\2\2\2\u0510\u0511\3\2\2\2\u0511\u051c\3\2\2\2\u0512\u0513\7\u009d"+
		"\2\2\u0513\u0514\7\26\2\2\u0514\u0519\5\u0096L\2\u0515\u0516\7\5\2\2\u0516"+
		"\u0518\5\u0096L\2\u0517\u0515\3\2\2\2\u0518\u051b\3\2\2\2\u0519\u0517"+
		"\3\2\2\2\u0519\u051a\3\2\2\2\u051a\u051d\3\2\2\2\u051b\u0519\3\2\2\2\u051c"+
		"\u0512\3\2\2\2\u051c\u051d\3\2\2\2\u051d\u0528\3\2\2\2\u051e\u051f\7\u009e"+
		"\2\2\u051f\u0520\7\26\2\2\u0520\u0525\5\u0096L\2\u0521\u0522\7\5\2\2\u0522"+
		"\u0524\5\u0096L\2\u0523\u0521\3\2\2\2\u0524\u0527\3\2\2\2\u0525\u0523"+
		"\3\2\2\2\u0525\u0526\3\2\2\2\u0526\u0529\3\2\2\2\u0527\u0525\3\2\2\2\u0528"+
		"\u051e\3\2\2\2\u0528\u0529\3\2\2\2\u0529\u0534\3\2\2\2\u052a\u052b\7\u009c"+
		"\2\2\u052b\u052c\7\26\2\2\u052c\u0531\5T+\2\u052d\u052e\7\5\2\2\u052e"+
		"\u0530\5T+\2\u052f\u052d\3\2\2\2\u0530\u0533\3\2\2\2\u0531\u052f\3\2\2"+
		"\2\u0531\u0532\3\2\2\2\u0532\u0535\3\2\2\2\u0533\u0531\3\2\2\2\u0534\u052a"+
		"\3\2\2\2\u0534\u0535\3\2\2\2\u0535\u0537\3\2\2\2\u0536\u0538\5\u00be`"+
		"\2\u0537\u0536\3\2\2\2\u0537\u0538\3\2\2\2\u0538\u053e\3\2\2\2\u0539\u053c"+
		"\7\35\2\2\u053a\u053d\7\21\2\2\u053b\u053d\5\u0096L\2\u053c\u053a\3\2"+
		"\2\2\u053c\u053b\3\2\2\2\u053d\u053f\3\2\2\2\u053e\u0539\3\2\2\2\u053e"+
		"\u053f\3\2\2\2\u053fM\3\2\2\2\u0540\u0542\5$\23\2\u0541\u0540\3\2\2\2"+
		"\u0541\u0542\3\2\2\2\u0542\u0543\3\2\2\2\u0543\u0544\5V,\2\u0544\u0545"+
		"\5L\'\2\u0545O\3\2\2\2\u0546\u0547\b)\1\2\u0547\u0548\5R*\2\u0548\u0560"+
		"\3\2\2\2\u0549\u054a\f\5\2\2\u054a\u054b\6)\3\2\u054b\u054d\t\t\2\2\u054c"+
		"\u054e\5j\66\2\u054d\u054c\3\2\2\2\u054d\u054e\3\2\2\2\u054e\u054f\3\2"+
		"\2\2\u054f\u055f\5P)\6\u0550\u0551\f\4\2\2\u0551\u0552\6)\5\2\u0552\u0554"+
		"\7l\2\2\u0553\u0555\5j\66\2\u0554\u0553\3\2\2\2\u0554\u0555\3\2\2\2\u0555"+
		"\u0556\3\2\2\2\u0556\u055f\5P)\5\u0557\u0558\f\3\2\2\u0558\u0559\6)\7"+
		"\2\u0559\u055b\t\n\2\2\u055a\u055c\5j\66\2\u055b\u055a\3\2\2\2\u055b\u055c"+
		"\3\2\2\2\u055c\u055d\3\2\2\2\u055d\u055f\5P)\4\u055e\u0549\3\2\2\2\u055e"+
		"\u0550\3\2\2\2\u055e\u0557\3\2\2\2\u055f\u0562\3\2\2\2\u0560\u055e\3\2"+
		"\2\2\u0560\u0561\3\2\2\2\u0561Q\3\2\2\2\u0562\u0560\3\2\2\2\u0563\u056c"+
		"\5V,\2\u0564\u0565\7R\2\2\u0565\u056c\5\u008eH\2\u0566\u056c\5\u0086D"+
		"\2\u0567\u0568\7\3\2\2\u0568\u0569\5J&\2\u0569\u056a\7\4\2\2\u056a\u056c"+
		"\3\2\2\2\u056b\u0563\3\2\2\2\u056b\u0564\3\2\2\2\u056b\u0566\3\2\2\2\u056b"+
		"\u0567\3\2\2\2\u056cS\3\2\2\2\u056d\u056f\5\u0096L\2\u056e\u0570\t\13"+
		"\2\2\u056f\u056e\3\2\2\2\u056f\u0570\3\2\2\2\u0570\u0573\3\2\2\2\u0571"+
		"\u0572\7,\2\2\u0572\u0574\t\f\2\2\u0573\u0571\3\2\2\2\u0573\u0574\3\2"+
		"\2\2\u0574U\3\2\2\2\u0575\u0576\7\r\2\2\u0576\u0577\7\u00a0\2\2\u0577"+
		"\u0578\7\3\2\2\u0578\u0579\5\u0094K\2\u0579\u057a\7\4\2\2\u057a\u0580"+
		"\3\2\2\2\u057b\u057c\7s\2\2\u057c\u0580\5\u0094K\2\u057d\u057e\7\u00a1"+
		"\2\2\u057e\u0580\5\u0094K\2\u057f\u0575\3\2\2\2\u057f\u057b\3\2\2\2\u057f"+
		"\u057d\3\2\2\2\u0580\u0582\3\2\2\2\u0581\u0583\5\u008cG\2\u0582\u0581"+
		"\3\2\2\2\u0582\u0583\3\2\2\2\u0583\u0586\3\2\2\2\u0584\u0585\7\u00a6\2"+
		"\2\u0585\u0587\7\u00f3\2\2\u0586\u0584\3\2\2\2\u0586\u0587\3\2\2\2\u0587"+
		"\u0588\3\2\2\2\u0588\u0589\7\u00a2\2\2\u0589\u0596\7\u00f3\2\2\u058a\u0594"+
		"\7\20\2\2\u058b\u0595\5z>\2\u058c\u0595\5\u00b4[\2\u058d\u0590\7\3\2\2"+
		"\u058e\u0591\5z>\2\u058f\u0591\5\u00b4[\2\u0590\u058e\3\2\2\2\u0590\u058f"+
		"\3\2\2\2\u0591\u0592\3\2\2\2\u0592\u0593\7\4\2\2\u0593\u0595\3\2\2\2\u0594"+
		"\u058b\3\2\2\2\u0594\u058c\3\2\2\2\u0594\u058d\3\2\2\2\u0595\u0597\3\2"+
		"\2\2\u0596\u058a\3\2\2\2\u0596\u0597\3\2\2\2\u0597\u0599\3\2\2\2\u0598"+
		"\u059a\5\u008cG\2\u0599\u0598\3\2\2\2\u0599\u059a\3\2\2\2\u059a\u059d"+
		"\3\2\2\2\u059b\u059c\7\u00a5\2\2\u059c\u059e\7\u00f3\2\2\u059d\u059b\3"+
		"\2\2\2\u059d\u059e\3\2\2\2\u059e\u05a0\3\2\2\2\u059f\u05a1\5\\/\2\u05a0"+
		"\u059f\3\2\2\2\u05a0\u05a1\3\2\2\2\u05a1\u05a4\3\2\2\2\u05a2\u05a3\7\24"+
		"\2\2\u05a3\u05a5\5\u0098M\2\u05a4\u05a2\3\2\2\2\u05a4\u05a5\3\2\2\2\u05a5"+
		"\u05d3\3\2\2\2\u05a6\u05aa\7\r\2\2\u05a7\u05a9\5X-\2\u05a8\u05a7\3\2\2"+
		"\2\u05a9\u05ac\3\2\2\2\u05aa\u05a8\3\2\2\2\u05aa\u05ab\3\2\2\2\u05ab\u05ae"+
		"\3\2\2\2\u05ac\u05aa\3\2\2\2\u05ad\u05af\5j\66\2\u05ae\u05ad\3\2\2\2\u05ae"+
		"\u05af\3\2\2\2\u05af\u05b0\3\2\2\2\u05b0\u05b2\5\u0094K\2\u05b1\u05b3"+
		"\5\\/\2\u05b2\u05b1\3\2\2\2\u05b2\u05b3\3\2\2\2\u05b3\u05bd\3\2\2\2\u05b4"+
		"\u05ba\5\\/\2\u05b5\u05b7\7\r\2\2\u05b6\u05b8\5j\66\2\u05b7\u05b6\3\2"+
		"\2\2\u05b7\u05b8\3\2\2\2\u05b8\u05b9\3\2\2\2\u05b9\u05bb\5\u0094K\2\u05ba"+
		"\u05b5\3\2\2\2\u05ba\u05bb\3\2\2\2\u05bb\u05bd\3\2\2\2\u05bc\u05a6\3\2"+
		"\2\2\u05bc\u05b4\3\2\2\2\u05bd\u05c1\3\2\2\2\u05be\u05c0\5h\65\2\u05bf"+
		"\u05be\3\2\2\2\u05c0\u05c3\3\2\2\2\u05c1\u05bf\3\2\2\2\u05c1\u05c2\3\2"+
		"\2\2\u05c2\u05c6\3\2\2\2\u05c3\u05c1\3\2\2\2\u05c4\u05c5\7\24\2\2\u05c5"+
		"\u05c7\5\u0098M\2\u05c6\u05c4\3\2\2\2\u05c6\u05c7\3\2\2\2\u05c7\u05c9"+
		"\3\2\2\2\u05c8\u05ca\5^\60\2\u05c9\u05c8\3\2\2\2\u05c9\u05ca\3\2\2\2\u05ca"+
		"\u05cd\3\2\2\2\u05cb\u05cc\7\34\2\2\u05cc\u05ce\5\u0098M\2\u05cd\u05cb"+
		"\3\2\2\2\u05cd\u05ce\3\2\2\2\u05ce\u05d0\3\2\2\2\u05cf\u05d1\5\u00be`"+
		"\2\u05d0\u05cf\3\2\2\2\u05d0\u05d1\3\2\2\2\u05d1\u05d3\3\2\2\2\u05d2\u057f"+
		"\3\2\2\2\u05d2\u05bc\3\2\2\2\u05d3W\3\2\2\2\u05d4\u05d5\7\7\2\2\u05d5"+
		"\u05dc\5Z.\2\u05d6\u05d8\7\5\2\2\u05d7\u05d6\3\2\2\2\u05d7\u05d8\3\2\2"+
		"\2\u05d8\u05d9\3\2\2\2\u05d9\u05db\5Z.\2\u05da\u05d7\3\2\2\2\u05db\u05de"+
		"\3\2\2\2\u05dc\u05da\3\2\2\2\u05dc\u05dd\3\2\2\2\u05dd\u05df\3\2\2\2\u05de"+
		"\u05dc\3\2\2\2\u05df\u05e0\7\b\2\2\u05e0Y\3\2\2\2\u05e1\u05ef\5\u00ca"+
		"f\2\u05e2\u05e3\5\u00caf\2\u05e3\u05e4\7\3\2\2\u05e4\u05e9\5\u009eP\2"+
		"\u05e5\u05e6\7\5\2\2\u05e6\u05e8\5\u009eP\2\u05e7\u05e5\3\2\2\2\u05e8"+
		"\u05eb\3\2\2\2\u05e9\u05e7\3\2\2\2\u05e9\u05ea\3\2\2\2\u05ea\u05ec\3\2"+
		"\2\2\u05eb\u05e9\3\2\2\2\u05ec\u05ed\7\4\2\2\u05ed\u05ef\3\2\2\2\u05ee"+
		"\u05e1\3\2\2\2\u05ee\u05e2\3\2\2\2\u05ef[\3\2\2\2\u05f0\u05f1\7\16\2\2"+
		"\u05f1\u05f6\5l\67\2\u05f2\u05f3\7\5\2\2\u05f3\u05f5\5l\67\2\u05f4\u05f2"+
		"\3\2\2\2\u05f5\u05f8\3\2\2\2\u05f6\u05f4\3\2\2\2\u05f6\u05f7\3\2\2\2\u05f7"+
		"\u05fc\3\2\2\2\u05f8\u05f6\3\2\2\2\u05f9\u05fb\5h\65\2\u05fa\u05f9\3\2"+
		"\2\2\u05fb\u05fe\3\2\2\2\u05fc\u05fa\3\2\2\2\u05fc\u05fd\3\2\2\2\u05fd"+
		"\u0600\3\2\2\2\u05fe\u05fc\3\2\2\2\u05ff\u0601\5b\62\2\u0600\u05ff\3\2"+
		"\2\2\u0600\u0601\3\2\2\2\u0601]\3\2\2\2\u0602\u0603\7\25\2\2\u0603\u0604"+
		"\7\26\2\2\u0604\u0609\5\u0096L\2\u0605\u0606\7\5\2\2\u0606\u0608\5\u0096"+
		"L\2\u0607\u0605\3\2\2\2\u0608\u060b\3\2\2\2\u0609\u0607\3\2\2\2\u0609"+
		"\u060a\3\2\2\2\u060a\u061d\3\2\2\2\u060b\u0609\3\2\2\2\u060c\u060d\7O"+
		"\2\2\u060d\u061e\7\32\2\2\u060e\u060f\7O\2\2\u060f\u061e\7\31\2\2\u0610"+
		"\u0611\7\27\2\2\u0611\u0612\7\30\2\2\u0612\u0613\7\3\2\2\u0613\u0618\5"+
		"`\61\2\u0614\u0615\7\5\2\2\u0615\u0617\5`\61\2\u0616\u0614\3\2\2\2\u0617"+
		"\u061a\3\2\2\2\u0618\u0616\3\2\2\2\u0618\u0619\3\2\2\2\u0619\u061b\3\2"+
		"\2\2\u061a\u0618\3\2\2\2\u061b\u061c\7\4\2\2\u061c\u061e\3\2\2\2\u061d"+
		"\u060c\3\2\2\2\u061d\u060e\3\2\2\2\u061d\u0610\3\2\2\2\u061d\u061e\3\2"+
		"\2\2\u061e\u062f\3\2\2\2\u061f\u0620\7\25\2\2\u0620\u0621\7\26\2\2\u0621"+
		"\u0622\7\27\2\2\u0622\u0623\7\30\2\2\u0623\u0624\7\3\2\2\u0624\u0629\5"+
		"`\61\2\u0625\u0626\7\5\2\2\u0626\u0628\5`\61\2\u0627\u0625\3\2\2\2\u0628"+
		"\u062b\3\2\2\2\u0629\u0627\3\2\2\2\u0629\u062a\3\2\2\2\u062a\u062c\3\2"+
		"\2\2\u062b\u0629\3\2\2\2\u062c\u062d\7\4\2\2\u062d\u062f\3\2\2\2\u062e"+
		"\u0602\3\2\2\2\u062e\u061f\3\2\2\2\u062f_\3\2\2\2\u0630\u0639\7\3\2\2"+
		"\u0631\u0636\5\u0096L\2\u0632\u0633\7\5\2\2\u0633\u0635\5\u0096L\2\u0634"+
		"\u0632\3\2\2\2\u0635\u0638\3\2\2\2\u0636\u0634\3\2\2\2\u0636\u0637\3\2"+
		"\2\2\u0637\u063a\3\2\2\2\u0638\u0636\3\2\2\2\u0639\u0631\3\2\2\2\u0639"+
		"\u063a\3\2\2\2\u063a\u063b\3\2\2\2\u063b\u063e\7\4\2\2\u063c\u063e\5\u0096"+
		"L\2\u063d\u0630\3\2\2\2\u063d\u063c\3\2\2\2\u063ea\3\2\2\2\u063f\u0640"+
		"\7@\2\2\u0640\u0641\7\3\2\2\u0641\u0642\5\u0094K\2\u0642\u0643\7/\2\2"+
		"\u0643\u0644\5d\63\2\u0644\u0645\7!\2\2\u0645\u0646\7\3\2\2\u0646\u064b"+
		"\5f\64\2\u0647\u0648\7\5\2\2\u0648\u064a\5f\64\2\u0649\u0647\3\2\2\2\u064a"+
		"\u064d\3\2\2\2\u064b\u0649\3\2\2\2\u064b\u064c\3\2\2\2\u064c\u064e\3\2"+
		"\2\2\u064d\u064b\3\2\2\2\u064e\u064f\7\4\2\2\u064f\u0650\7\4\2\2\u0650"+
		"c\3\2\2\2\u0651\u065e\5\u00caf\2\u0652\u0653\7\3\2\2\u0653\u0658\5\u00ca"+
		"f\2\u0654\u0655\7\5\2\2\u0655\u0657\5\u00caf\2\u0656\u0654\3\2\2\2\u0657"+
		"\u065a\3\2\2\2\u0658\u0656\3\2\2\2\u0658\u0659\3\2\2\2\u0659\u065b\3\2"+
		"\2\2\u065a\u0658\3\2\2\2\u065b\u065c\7\4\2\2\u065c\u065e\3\2\2\2\u065d"+
		"\u0651\3\2\2\2\u065d\u0652\3\2\2\2\u065ee\3\2\2\2\u065f\u0664\5\u0096"+
		"L\2\u0660\u0662\7\20\2\2\u0661\u0660\3\2\2\2\u0661\u0662\3\2\2\2\u0662"+
		"\u0663\3\2\2\2\u0663\u0665\5\u00caf\2\u0664\u0661\3\2\2\2\u0664\u0665"+
		"\3\2\2\2\u0665g\3\2\2\2\u0666\u0667\7A\2\2\u0667\u0669\7T\2\2\u0668\u066a"+
		"\78\2\2\u0669\u0668\3\2\2\2\u0669\u066a\3\2\2\2\u066a\u066b\3\2\2\2\u066b"+
		"\u066c\5\u00c8e\2\u066c\u0675\7\3\2\2\u066d\u0672\5\u0096L\2\u066e\u066f"+
		"\7\5\2\2\u066f\u0671\5\u0096L\2\u0670\u066e\3\2\2\2\u0671\u0674\3\2\2"+
		"\2\u0672\u0670\3\2\2\2\u0672\u0673\3\2\2\2\u0673\u0676\3\2\2\2\u0674\u0672"+
		"\3\2\2\2\u0675\u066d\3\2\2\2\u0675\u0676\3\2\2\2\u0676\u0677\3\2\2\2\u0677"+
		"\u0678\7\4\2\2\u0678\u0684\5\u00caf\2\u0679\u067b\7\20\2\2\u067a\u0679"+
		"\3\2\2\2\u067a\u067b\3\2\2\2\u067b\u067c\3\2\2\2\u067c\u0681\5\u00caf"+
		"\2\u067d\u067e\7\5\2\2\u067e\u0680\5\u00caf\2\u067f\u067d\3\2\2\2\u0680"+
		"\u0683\3\2\2\2\u0681\u067f\3\2\2\2\u0681\u0682\3\2\2\2\u0682\u0685\3\2"+
		"\2\2\u0683\u0681\3\2\2\2\u0684\u067a\3\2\2\2\u0684\u0685\3\2\2\2\u0685"+
		"i\3\2\2\2\u0686\u0687\t\r\2\2\u0687k\3\2\2\2\u0688\u068c\5\u0084C\2\u0689"+
		"\u068b\5n8\2\u068a\u0689\3\2\2\2\u068b\u068e\3\2\2\2\u068c\u068a\3\2\2"+
		"\2\u068c\u068d\3\2\2\2\u068dm\3\2\2\2\u068e\u068c\3\2\2\2\u068f\u0690"+
		"\5p9\2\u0690\u0691\7\66\2\2\u0691\u0693\5\u0084C\2\u0692\u0694\5r:\2\u0693"+
		"\u0692\3\2\2\2\u0693\u0694\3\2\2\2\u0694\u069b\3\2\2\2\u0695\u0696\7>"+
		"\2\2\u0696\u0697\5p9\2\u0697\u0698\7\66\2\2\u0698\u0699\5\u0084C\2\u0699"+
		"\u069b\3\2\2\2\u069a\u068f\3\2\2\2\u069a\u0695\3\2\2\2\u069bo\3\2\2\2"+
		"\u069c\u069e\79\2\2\u069d\u069c\3\2\2\2\u069d\u069e\3\2\2\2\u069e\u06b3"+
		"\3\2\2\2\u069f\u06b3\7\67\2\2\u06a0\u06a2\7:\2\2\u06a1\u06a3\78\2\2\u06a2"+
		"\u06a1\3\2\2\2\u06a2\u06a3\3\2\2\2\u06a3\u06b3\3\2\2\2\u06a4\u06a5\7:"+
		"\2\2\u06a5\u06b3\7;\2\2\u06a6\u06a8\7<\2\2\u06a7\u06a9\78\2\2\u06a8\u06a7"+
		"\3\2\2\2\u06a8\u06a9\3\2\2\2\u06a9\u06b3\3\2\2\2\u06aa\u06ac\7=\2\2\u06ab"+
		"\u06ad\78\2\2\u06ac\u06ab\3\2\2\2\u06ac\u06ad\3\2\2\2\u06ad\u06b3\3\2"+
		"\2\2\u06ae\u06b0\7:\2\2\u06af\u06ae\3\2\2\2\u06af\u06b0\3\2\2\2\u06b0"+
		"\u06b1\3\2\2\2\u06b1\u06b3\7\u00f0\2\2\u06b2\u069d\3\2\2\2\u06b2\u069f"+
		"\3\2\2\2\u06b2\u06a0\3\2\2\2\u06b2\u06a4\3\2\2\2\u06b2\u06a6\3\2\2\2\u06b2"+
		"\u06aa\3\2\2\2\u06b2\u06af\3\2\2\2\u06b3q\3\2\2\2\u06b4\u06b5\7?\2\2\u06b5"+
		"\u06c3\5\u0098M\2\u06b6\u06b7\7\u00a2\2\2\u06b7\u06b8\7\3\2\2\u06b8\u06bd"+
		"\5\u00caf\2\u06b9\u06ba\7\5\2\2\u06ba\u06bc\5\u00caf\2\u06bb\u06b9\3\2"+
		"\2\2\u06bc\u06bf\3\2\2\2\u06bd\u06bb\3\2\2\2\u06bd\u06be\3\2\2\2\u06be"+
		"\u06c0\3\2\2\2\u06bf\u06bd\3\2\2\2\u06c0\u06c1\7\4\2\2\u06c1\u06c3\3\2"+
		"\2\2\u06c2\u06b4\3\2\2\2\u06c2\u06b6\3\2\2\2\u06c3s\3\2\2\2\u06c4\u06c5"+
		"\7n\2\2\u06c5\u06c7\7\3\2\2\u06c6\u06c8\5v<\2\u06c7\u06c6\3\2\2\2\u06c7"+
		"\u06c8\3\2\2\2\u06c8\u06c9\3\2\2\2\u06c9\u06ca\7\4\2\2\u06cau\3\2\2\2"+
		"\u06cb\u06cd\7\u008e\2\2\u06cc\u06cb\3\2\2\2\u06cc\u06cd\3\2\2\2\u06cd"+
		"\u06ce\3\2\2\2\u06ce\u06cf\t\16\2\2\u06cf\u06e4\7\u0098\2\2\u06d0\u06d1"+
		"\5\u0096L\2\u06d1\u06d2\7F\2\2\u06d2\u06e4\3\2\2\2\u06d3\u06d4\7\u0099"+
		"\2\2\u06d4\u06d5\7\u00f7\2\2\u06d5\u06d6\7\u009a\2\2\u06d6\u06d7\7\u009b"+
		"\2\2\u06d7\u06e0\7\u00f7\2\2\u06d8\u06de\7?\2\2\u06d9\u06df\5\u00caf\2"+
		"\u06da\u06db\5\u00c8e\2\u06db\u06dc\7\3\2\2\u06dc\u06dd\7\4\2\2\u06dd"+
		"\u06df\3\2\2\2\u06de\u06d9\3\2\2\2\u06de\u06da\3\2\2\2\u06df\u06e1\3\2"+
		"\2\2\u06e0\u06d8\3\2\2\2\u06e0\u06e1\3\2\2\2\u06e1\u06e4\3\2\2\2\u06e2"+
		"\u06e4\5\u0096L\2\u06e3\u06cc\3\2\2\2\u06e3\u06d0\3\2\2\2\u06e3\u06d3"+
		"\3\2\2\2\u06e3\u06e2\3\2\2\2\u06e4w\3\2\2\2\u06e5\u06e6\7\3\2\2\u06e6"+
		"\u06e7\5z>\2\u06e7\u06e8\7\4\2\2\u06e8y\3\2\2\2\u06e9\u06ee\5\u00caf\2"+
		"\u06ea\u06eb\7\5\2\2\u06eb\u06ed\5\u00caf\2\u06ec\u06ea\3\2\2\2\u06ed"+
		"\u06f0\3\2\2\2\u06ee\u06ec\3\2\2\2\u06ee\u06ef\3\2\2\2\u06ef{\3\2\2\2"+
		"\u06f0\u06ee\3\2\2\2\u06f1\u06f2\7\3\2\2\u06f2\u06f7\5~@\2\u06f3\u06f4"+
		"\7\5\2\2\u06f4\u06f6\5~@\2\u06f5\u06f3\3\2\2\2\u06f6\u06f9\3\2\2\2\u06f7"+
		"\u06f5\3\2\2\2\u06f7\u06f8\3\2\2\2\u06f8\u06fa\3\2\2\2\u06f9\u06f7\3\2"+
		"\2\2\u06fa\u06fb\7\4\2\2\u06fb}\3\2\2\2\u06fc\u06fe\5\u00caf\2\u06fd\u06ff"+
		"\t\13\2\2\u06fe\u06fd\3\2\2\2\u06fe\u06ff\3\2\2\2\u06ff\177\3\2\2\2\u0700"+
		"\u0701\7\3\2\2\u0701\u0706\5\u0082B\2\u0702\u0703\7\5\2\2\u0703\u0705"+
		"\5\u0082B\2\u0704\u0702\3\2\2\2\u0705\u0708\3\2\2\2\u0706\u0704\3\2\2"+
		"\2\u0706\u0707\3\2\2\2\u0707\u0709\3\2\2\2\u0708\u0706\3\2\2\2\u0709\u070a"+
		"\7\4\2\2\u070a\u0081\3\2\2\2\u070b\u070e\5\u00caf\2\u070c\u070d\7u\2\2"+
		"\u070d\u070f\7\u00f3\2\2\u070e\u070c\3\2\2\2\u070e\u070f\3\2\2\2\u070f"+
		"\u0083\3\2\2\2\u0710\u0712\5\u008eH\2\u0711\u0713\5t;\2\u0712\u0711\3"+
		"\2\2\2\u0712\u0713\3\2\2\2\u0713\u0714\3\2\2\2\u0714\u0715\5\u008aF\2"+
		"\u0715\u0729\3\2\2\2\u0716\u0717\7\3\2\2\u0717\u0718\5J&\2\u0718\u071a"+
		"\7\4\2\2\u0719\u071b\5t;\2\u071a\u0719\3\2\2\2\u071a\u071b\3\2\2\2\u071b"+
		"\u071c\3\2\2\2\u071c\u071d\5\u008aF\2\u071d\u0729\3\2\2\2\u071e\u071f"+
		"\7\3\2\2\u071f\u0720\5l\67\2\u0720\u0722\7\4\2\2\u0721\u0723\5t;\2\u0722"+
		"\u0721\3\2\2\2\u0722\u0723\3\2\2\2\u0723\u0724\3\2\2\2\u0724\u0725\5\u008a"+
		"F\2\u0725\u0729\3\2\2\2\u0726\u0729\5\u0086D\2\u0727\u0729\5\u0088E\2"+
		"\u0728\u0710\3\2\2\2\u0728\u0716\3\2\2\2\u0728\u071e\3\2\2\2\u0728\u0726"+
		"\3\2\2\2\u0728\u0727\3\2\2\2\u0729\u0085\3\2\2\2\u072a\u072b\7P\2\2\u072b"+
		"\u0730\5\u0096L\2\u072c\u072d\7\5\2\2\u072d\u072f\5\u0096L\2\u072e\u072c"+
		"\3\2\2\2\u072f\u0732\3\2\2\2\u0730\u072e\3\2\2\2\u0730\u0731\3\2\2\2\u0731"+
		"\u0733\3\2\2\2\u0732\u0730\3\2\2\2\u0733\u0734\5\u008aF\2\u0734\u0087"+
		"\3\2\2\2\u0735\u0736\5\u00caf\2\u0736\u073f\7\3\2\2\u0737\u073c\5\u0096"+
		"L\2\u0738\u0739\7\5\2\2\u0739\u073b\5\u0096L\2\u073a\u0738\3\2\2\2\u073b"+
		"\u073e\3\2\2\2\u073c\u073a\3\2\2\2\u073c\u073d\3\2\2\2\u073d\u0740\3\2"+
		"\2\2\u073e\u073c\3\2\2\2\u073f\u0737\3\2\2\2\u073f\u0740\3\2\2\2\u0740"+
		"\u0741\3\2\2\2\u0741\u0742\7\4\2\2\u0742\u0743\5\u008aF\2\u0743\u0089"+
		"\3\2\2\2\u0744\u0746\7\20\2\2\u0745\u0744\3\2\2\2\u0745\u0746\3\2\2\2"+
		"\u0746\u0747\3\2\2\2\u0747\u0749\5\u00ccg\2\u0748\u074a\5x=\2\u0749\u0748"+
		"\3\2\2\2\u0749\u074a\3\2\2\2\u074a\u074c\3\2\2\2\u074b\u0745\3\2\2\2\u074b"+
		"\u074c\3\2\2\2\u074c\u008b\3\2\2\2\u074d\u074e\7N\2\2\u074e\u074f\7\\"+
		"\2\2\u074f\u0750\7\u00a3\2\2\u0750\u0754\7\u00f3\2\2\u0751\u0752\7O\2"+
		"\2\u0752\u0753\7\u00a4\2\2\u0753\u0755\5\66\34\2\u0754\u0751\3\2\2\2\u0754"+
		"\u0755\3\2\2\2\u0755\u077f\3\2\2\2\u0756\u0757\7N\2\2\u0757\u0758\7\\"+
		"\2\2\u0758\u0762\7\u00a7\2\2\u0759\u075a\7\u00a8\2\2\u075a\u075b\7\u00a9"+
		"\2\2\u075b\u075c\7\26\2\2\u075c\u0760\7\u00f3\2\2\u075d\u075e\7\u00ad"+
		"\2\2\u075e\u075f\7\26\2\2\u075f\u0761\7\u00f3\2\2\u0760\u075d\3\2\2\2"+
		"\u0760\u0761\3\2\2\2\u0761\u0763\3\2\2\2\u0762\u0759\3\2\2\2\u0762\u0763"+
		"\3\2\2\2\u0763\u0769\3\2\2\2\u0764\u0765\7\u00aa\2\2\u0765\u0766\7\u00ab"+
		"\2\2\u0766\u0767\7\u00a9\2\2\u0767\u0768\7\26\2\2\u0768\u076a\7\u00f3"+
		"\2\2\u0769\u0764\3\2\2\2\u0769\u076a\3\2\2\2\u076a\u0770\3\2\2\2\u076b"+
		"\u076c\7s\2\2\u076c\u076d\7\u00ac\2\2\u076d\u076e\7\u00a9\2\2\u076e\u076f"+
		"\7\26\2\2\u076f\u0771\7\u00f3\2\2\u0770\u076b\3\2\2\2\u0770\u0771\3\2"+
		"\2\2\u0771\u0776\3\2\2\2\u0772\u0773\7\u00ae\2\2\u0773\u0774\7\u00a9\2"+
		"\2\u0774\u0775\7\26\2\2\u0775\u0777\7\u00f3\2\2\u0776\u0772\3\2\2\2\u0776"+
		"\u0777\3\2\2\2\u0777\u077c\3\2\2\2\u0778\u0779\7)\2\2\u0779\u077a\7\u00dc"+
		"\2\2\u077a\u077b\7\20\2\2\u077b\u077d\7\u00f3\2\2\u077c\u0778\3\2\2\2"+
		"\u077c\u077d\3\2\2\2\u077d\u077f\3\2\2\2\u077e\u074d\3\2\2\2\u077e\u0756"+
		"\3\2\2\2\u077f\u008d\3\2\2\2\u0780\u0781\5\u00caf\2\u0781\u0782\7\6\2"+
		"\2\u0782\u0784\3\2\2\2\u0783\u0780\3\2\2\2\u0783\u0784\3\2\2\2\u0784\u0785"+
		"\3\2\2\2\u0785\u0786\5\u00caf\2\u0786\u008f\3\2\2\2\u0787\u0788\5\u00ca"+
		"f\2\u0788\u0789\7\6\2\2\u0789\u078b\3\2\2\2\u078a\u0787\3\2\2\2\u078a"+
		"\u078b\3\2\2\2\u078b\u078c\3\2\2\2\u078c\u078d\5\u00caf\2\u078d\u0091"+
		"\3\2\2\2\u078e\u0796\5\u0096L\2\u078f\u0791\7\20\2\2\u0790\u078f\3\2\2"+
		"\2\u0790\u0791\3\2\2\2\u0791\u0794\3\2\2\2\u0792\u0795\5\u00caf\2\u0793"+
		"\u0795\5x=\2\u0794\u0792\3\2\2\2\u0794\u0793\3\2\2\2\u0795\u0797\3\2\2"+
		"\2\u0796\u0790\3\2\2\2\u0796\u0797\3\2\2\2\u0797\u0093\3\2\2\2\u0798\u079d"+
		"\5\u0092J\2\u0799\u079a\7\5\2\2\u079a\u079c\5\u0092J\2\u079b\u0799\3\2"+
		"\2\2\u079c\u079f\3\2\2\2\u079d\u079b\3\2\2\2\u079d\u079e\3\2\2\2\u079e"+
		"\u0095\3\2\2\2\u079f\u079d\3\2\2\2\u07a0\u07a1\5\u0098M\2\u07a1\u0097"+
		"\3\2\2\2\u07a2\u07a3\bM\1\2\u07a3\u07a4\7\"\2\2\u07a4\u07af\5\u0098M\7"+
		"\u07a5\u07a6\7$\2\2\u07a6\u07a7\7\3\2\2\u07a7\u07a8\5\"\22\2\u07a8\u07a9"+
		"\7\4\2\2\u07a9\u07af\3\2\2\2\u07aa\u07ac\5\u009cO\2\u07ab\u07ad\5\u009a"+
		"N\2\u07ac\u07ab\3\2\2\2\u07ac\u07ad\3\2\2\2\u07ad\u07af\3\2\2\2\u07ae"+
		"\u07a2\3\2\2\2\u07ae\u07a5\3\2\2\2\u07ae\u07aa\3\2\2\2\u07af\u07b8\3\2"+
		"\2\2\u07b0\u07b1\f\4\2\2\u07b1\u07b2\7 \2\2\u07b2\u07b7\5\u0098M\5\u07b3"+
		"\u07b4\f\3\2\2\u07b4\u07b5\7\37\2\2\u07b5\u07b7\5\u0098M\4\u07b6\u07b0"+
		"\3\2\2\2\u07b6\u07b3\3\2\2\2\u07b7\u07ba\3\2\2\2\u07b8\u07b6\3\2\2\2\u07b8"+
		"\u07b9\3\2\2\2\u07b9\u0099\3\2\2\2\u07ba\u07b8\3\2\2\2\u07bb\u07bd\7\""+
		"\2\2\u07bc\u07bb\3\2\2\2\u07bc\u07bd\3\2\2\2\u07bd\u07be\3\2\2\2\u07be"+
		"\u07bf\7%\2\2\u07bf\u07c0\5\u009cO\2\u07c0\u07c1\7 \2\2\u07c1\u07c2\5"+
		"\u009cO\2\u07c2\u07ec\3\2\2\2\u07c3\u07c5\7\"\2\2\u07c4\u07c3\3\2\2\2"+
		"\u07c4\u07c5\3\2\2\2\u07c5\u07c6\3\2\2\2\u07c6\u07c7\7!\2\2\u07c7\u07c8"+
		"\7\3\2\2\u07c8\u07cd\5\u0096L\2\u07c9\u07ca\7\5\2\2\u07ca\u07cc\5\u0096"+
		"L\2\u07cb\u07c9\3\2\2\2\u07cc\u07cf\3\2\2\2\u07cd\u07cb\3\2\2\2\u07cd"+
		"\u07ce\3\2\2\2\u07ce\u07d0\3\2\2\2\u07cf\u07cd\3\2\2\2\u07d0\u07d1\7\4"+
		"\2\2\u07d1\u07ec\3\2\2\2\u07d2\u07d4\7\"\2\2\u07d3\u07d2\3\2\2\2\u07d3"+
		"\u07d4\3\2\2\2\u07d4\u07d5\3\2\2\2\u07d5\u07d6\7!\2\2\u07d6\u07d7\7\3"+
		"\2\2\u07d7\u07d8\5\"\22\2\u07d8\u07d9\7\4\2\2\u07d9\u07ec\3\2\2\2\u07da"+
		"\u07dc\7\"\2\2\u07db\u07da\3\2\2\2\u07db\u07dc\3\2\2\2\u07dc\u07dd\3\2"+
		"\2\2\u07dd\u07de\t\17\2\2\u07de\u07ec\5\u009cO\2\u07df\u07e1\7(\2\2\u07e0"+
		"\u07e2\7\"\2\2\u07e1\u07e0\3\2\2\2\u07e1\u07e2\3\2\2\2\u07e2\u07e3\3\2"+
		"\2\2\u07e3\u07ec\7)\2\2\u07e4\u07e6\7(\2\2\u07e5\u07e7\7\"\2\2\u07e6\u07e5"+
		"\3\2\2\2\u07e6\u07e7\3\2\2\2\u07e7\u07e8\3\2\2\2\u07e8\u07e9\7\23\2\2"+
		"\u07e9\u07ea\7\16\2\2\u07ea\u07ec\5\u009cO\2\u07eb\u07bc\3\2\2\2\u07eb"+
		"\u07c4\3\2\2\2\u07eb\u07d3\3\2\2\2\u07eb\u07db\3\2\2\2\u07eb\u07df\3\2"+
		"\2\2\u07eb\u07e4\3\2\2\2\u07ec\u009b\3\2\2\2\u07ed\u07ee\bO\1\2\u07ee"+
		"\u07f2\5\u009eP\2\u07ef\u07f0\t\20\2\2\u07f0\u07f2\5\u009cO\t\u07f1\u07ed"+
		"\3\2\2\2\u07f1\u07ef\3\2\2\2\u07f2\u0808\3\2\2\2\u07f3\u07f4\f\b\2\2\u07f4"+
		"\u07f5\t\21\2\2\u07f5\u0807\5\u009cO\t\u07f6\u07f7\f\7\2\2\u07f7\u07f8"+
		"\t\22\2\2\u07f8\u0807\5\u009cO\b\u07f9\u07fa\f\6\2\2\u07fa\u07fb\7\u0094"+
		"\2\2\u07fb\u0807\5\u009cO\7\u07fc\u07fd\f\5\2\2\u07fd\u07fe\7\u0097\2"+
		"\2\u07fe\u0807\5\u009cO\6\u07ff\u0800\f\4\2\2\u0800\u0801\7\u0095\2\2"+
		"\u0801\u0807\5\u009cO\5\u0802\u0803\f\3\2\2\u0803\u0804\5\u00a2R\2\u0804"+
		"\u0805\5\u009cO\4\u0805\u0807\3\2\2\2\u0806\u07f3\3\2\2\2\u0806\u07f6"+
		"\3\2\2\2\u0806\u07f9\3\2\2\2\u0806\u07fc\3\2\2\2\u0806\u07ff\3\2\2\2\u0806"+
		"\u0802\3\2\2\2\u0807\u080a\3\2\2\2\u0808\u0806\3\2\2\2\u0808\u0809\3\2"+
		"\2\2\u0809\u009d\3\2\2\2\u080a\u0808\3\2\2\2\u080b\u080c\bP\1\2\u080c"+
		"\u080e\7\61\2\2\u080d\u080f\5\u00bc_\2\u080e\u080d\3\2\2\2\u080f\u0810"+
		"\3\2\2\2\u0810\u080e\3\2\2\2\u0810\u0811\3\2\2\2\u0811\u0814\3\2\2\2\u0812"+
		"\u0813\7\64\2\2\u0813\u0815\5\u0096L\2\u0814\u0812\3\2\2\2\u0814\u0815"+
		"\3\2\2\2\u0815\u0816\3\2\2\2\u0816\u0817\7\65\2\2\u0817\u089d\3\2\2\2"+
		"\u0818\u0819\7\61\2\2\u0819\u081b\5\u0096L\2\u081a\u081c\5\u00bc_\2\u081b"+
		"\u081a\3\2\2\2\u081c\u081d\3\2\2\2\u081d\u081b\3\2\2\2\u081d\u081e\3\2"+
		"\2\2\u081e\u0821\3\2\2\2\u081f\u0820\7\64\2\2\u0820\u0822\5\u0096L\2\u0821"+
		"\u081f\3\2\2\2\u0821\u0822\3\2\2\2\u0822\u0823\3\2\2\2\u0823\u0824\7\65"+
		"\2\2\u0824\u089d\3\2\2\2\u0825\u0826\7`\2\2\u0826\u0827\7\3\2\2\u0827"+
		"\u0828\5\u0096L\2\u0828\u0829\7\20\2\2\u0829\u082a\5\u00b2Z\2\u082a\u082b"+
		"\7\4\2\2\u082b\u089d\3\2\2\2\u082c\u082d\7t\2\2\u082d\u0836\7\3\2\2\u082e"+
		"\u0833\5\u0092J\2\u082f\u0830\7\5\2\2\u0830\u0832\5\u0092J\2\u0831\u082f"+
		"\3\2\2\2\u0832\u0835\3\2\2\2\u0833\u0831\3\2\2\2\u0833\u0834\3\2\2\2\u0834"+
		"\u0837\3\2\2\2\u0835\u0833\3\2\2\2\u0836\u082e\3\2\2\2\u0836\u0837\3\2"+
		"\2\2\u0837\u0838\3\2\2\2\u0838\u089d\7\4\2\2\u0839\u083a\7K\2\2\u083a"+
		"\u083b\7\3\2\2\u083b\u083e\5\u0096L\2\u083c\u083d\7~\2\2\u083d\u083f\7"+
		",\2\2\u083e\u083c\3\2\2\2\u083e\u083f\3\2\2\2\u083f\u0840\3\2\2\2\u0840"+
		"\u0841\7\4\2\2\u0841\u089d\3\2\2\2\u0842\u0843\7M\2\2\u0843\u0844\7\3"+
		"\2\2\u0844\u0847\5\u0096L\2\u0845\u0846\7~\2\2\u0846\u0848\7,\2\2\u0847"+
		"\u0845\3\2\2\2\u0847\u0848\3\2\2\2\u0848\u0849\3\2\2\2\u0849\u084a\7\4"+
		"\2\2\u084a\u089d\3\2\2\2\u084b\u084c\7\u0083\2\2\u084c\u084d\7\3\2\2\u084d"+
		"\u084e\5\u009cO\2\u084e\u084f\7!\2\2\u084f\u0850\5\u009cO\2\u0850\u0851"+
		"\7\4\2\2\u0851\u089d\3\2\2\2\u0852\u089d\5\u00a0Q\2\u0853\u089d\7\u008f"+
		"\2\2\u0854\u0855\5\u00c8e\2\u0855\u0856\7\6\2\2\u0856\u0857\7\u008f\2"+
		"\2\u0857\u089d\3\2\2\2\u0858\u0859\7\3\2\2\u0859\u085c\5\u0092J\2\u085a"+
		"\u085b\7\5\2\2\u085b\u085d\5\u0092J\2\u085c\u085a\3\2\2\2\u085d\u085e"+
		"\3\2\2\2\u085e\u085c\3\2\2\2\u085e\u085f\3\2\2\2\u085f\u0860\3\2\2\2\u0860"+
		"\u0861\7\4\2\2\u0861\u089d\3\2\2\2\u0862\u0863\7\3\2\2\u0863\u0864\5\""+
		"\22\2\u0864\u0865\7\4\2\2\u0865\u089d\3\2\2\2\u0866\u0867\5\u00c8e\2\u0867"+
		"\u0873\7\3\2\2\u0868\u086a\5j\66\2\u0869\u0868\3\2\2\2\u0869\u086a\3\2"+
		"\2\2\u086a\u086b\3\2\2\2\u086b\u0870\5\u0096L\2\u086c\u086d\7\5\2\2\u086d"+
		"\u086f\5\u0096L\2\u086e\u086c\3\2\2\2\u086f\u0872\3\2\2\2\u0870\u086e"+
		"\3\2\2\2\u0870\u0871\3\2\2\2\u0871\u0874\3\2\2\2\u0872\u0870\3\2\2\2\u0873"+
		"\u0869\3\2\2\2\u0873\u0874\3\2\2\2\u0874\u0875\3\2\2\2\u0875\u0878\7\4"+
		"\2\2\u0876\u0877\7C\2\2\u0877\u0879\5\u00c2b\2\u0878\u0876\3\2\2\2\u0878"+
		"\u0879\3\2\2\2\u0879\u089d\3\2\2\2\u087a\u087b\5\u00c8e\2\u087b\u087c"+
		"\7\3\2\2\u087c\u087d\t\23\2\2\u087d\u087e\5\u0096L\2\u087e\u087f\7\16"+
		"\2\2\u087f\u0880\5\u0096L\2\u0880\u0881\7\4\2\2\u0881\u089d\3\2\2\2\u0882"+
		"\u0883\7\u00fb\2\2\u0883\u0884\7\t\2\2\u0884\u089d\5\u0096L\2\u0885\u0886"+
		"\7\3\2\2\u0886\u0889\7\u00fb\2\2\u0887\u0888\7\5\2\2\u0888\u088a\7\u00fb"+
		"\2\2\u0889\u0887\3\2\2\2\u088a\u088b\3\2\2\2\u088b\u0889\3\2\2\2\u088b"+
		"\u088c\3\2\2\2\u088c\u088d\3\2\2\2\u088d\u088e\7\4\2\2\u088e\u088f\7\t"+
		"\2\2\u088f\u089d\5\u0096L\2\u0890\u089d\5\u00caf\2\u0891\u0892\7\3\2\2"+
		"\u0892\u0893\5\u0096L\2\u0893\u0894\7\4\2\2\u0894\u089d\3\2\2\2\u0895"+
		"\u0896\7\u0084\2\2\u0896\u0897\7\3\2\2\u0897\u0898\5\u00caf\2\u0898\u0899"+
		"\7\16\2\2\u0899\u089a\5\u009cO\2\u089a\u089b\7\4\2\2\u089b\u089d\3\2\2"+
		"\2\u089c\u080b\3\2\2\2\u089c\u0818\3\2\2\2\u089c\u0825\3\2\2\2\u089c\u082c"+
		"\3\2\2\2\u089c\u0839\3\2\2\2\u089c\u0842\3\2\2\2\u089c\u084b\3\2\2\2\u089c"+
		"\u0852\3\2\2\2\u089c\u0853\3\2\2\2\u089c\u0854\3\2\2\2\u089c\u0858\3\2"+
		"\2\2\u089c\u0862\3\2\2\2\u089c\u0866\3\2\2\2\u089c\u087a\3\2\2\2\u089c"+
		"\u0882\3\2\2\2\u089c\u0885\3\2\2\2\u089c\u0890\3\2\2\2\u089c\u0891\3\2"+
		"\2\2\u089c\u0895\3\2\2\2\u089d\u08a8\3\2\2\2\u089e\u089f\f\7\2\2\u089f"+
		"\u08a0\7\n\2\2\u08a0\u08a1\5\u009cO\2\u08a1\u08a2\7\13\2\2\u08a2\u08a7"+
		"\3\2\2\2\u08a3\u08a4\f\5\2\2\u08a4\u08a5\7\6\2\2\u08a5\u08a7\5\u00caf"+
		"\2\u08a6\u089e\3\2\2\2\u08a6\u08a3\3\2\2\2\u08a7\u08aa\3\2\2\2\u08a8\u08a6"+
		"\3\2\2\2\u08a8\u08a9\3\2\2\2\u08a9\u009f\3\2\2\2\u08aa\u08a8\3\2\2\2\u08ab"+
		"\u08b8\7)\2\2\u08ac\u08b8\5\u00aaV\2\u08ad\u08ae\5\u00caf\2\u08ae\u08af"+
		"\7\u00f3\2\2\u08af\u08b8\3\2\2\2\u08b0\u08b8\5\u00d0i\2\u08b1\u08b8\5"+
		"\u00a8U\2\u08b2\u08b4\7\u00f3\2\2\u08b3\u08b2\3\2\2\2\u08b4\u08b5\3\2"+
		"\2\2\u08b5\u08b3\3\2\2\2\u08b5\u08b6\3\2\2\2\u08b6\u08b8\3\2\2\2\u08b7"+
		"\u08ab\3\2\2\2\u08b7\u08ac\3\2\2\2\u08b7\u08ad\3\2\2\2\u08b7\u08b0\3\2"+
		"\2\2\u08b7\u08b1\3\2\2\2\u08b7\u08b3\3\2\2\2\u08b8\u00a1\3\2\2\2\u08b9"+
		"\u08ba\t\24\2\2\u08ba\u00a3\3\2\2\2\u08bb\u08bc\t\25\2\2\u08bc\u00a5\3"+
		"\2\2\2\u08bd\u08be\t\26\2\2\u08be\u00a7\3\2\2\2\u08bf\u08c0\t\27\2\2\u08c0"+
		"\u00a9\3\2\2\2\u08c1\u08c5\7\60\2\2\u08c2\u08c4\5\u00acW\2\u08c3\u08c2"+
		"\3\2\2\2\u08c4\u08c7\3\2\2\2\u08c5\u08c3\3\2\2\2\u08c5\u08c6\3\2\2\2\u08c6"+
		"\u00ab\3\2\2\2\u08c7\u08c5\3\2\2\2\u08c8\u08c9\5\u00aeX\2\u08c9\u08cc"+
		"\5\u00caf\2\u08ca\u08cb\7m\2\2\u08cb\u08cd\5\u00caf\2\u08cc\u08ca\3\2"+
		"\2\2\u08cc\u08cd\3\2\2\2\u08cd\u00ad\3\2\2\2\u08ce\u08d0\t\30\2\2\u08cf"+
		"\u08ce\3\2\2\2\u08cf\u08d0\3\2\2\2\u08d0\u08d1\3\2\2\2\u08d1\u08d4\t\16"+
		"\2\2\u08d2\u08d4\7\u00f3\2\2\u08d3\u08cf\3\2\2\2\u08d3\u08d2\3\2\2\2\u08d4"+
		"\u00af\3\2\2\2\u08d5\u08d9\7K\2\2\u08d6\u08d7\7L\2\2\u08d7\u08d9\5\u00ca"+
		"f\2\u08d8\u08d5\3\2\2\2\u08d8\u08d6\3\2\2\2\u08d9\u00b1\3\2\2\2\u08da"+
		"\u08db\7r\2\2\u08db\u08dc\7\u0089\2\2\u08dc\u08dd\5\u00b2Z\2\u08dd\u08de"+
		"\7\u008b\2\2\u08de\u08fd\3\2\2\2\u08df\u08e0\7s\2\2\u08e0\u08e1\7\u0089"+
		"\2\2\u08e1\u08e2\5\u00b2Z\2\u08e2\u08e3\7\5\2\2\u08e3\u08e4\5\u00b2Z\2"+
		"\u08e4\u08e5\7\u008b\2\2\u08e5\u08fd\3\2\2\2\u08e6\u08ed\7t\2\2\u08e7"+
		"\u08e9\7\u0089\2\2\u08e8\u08ea\5\u00b8]\2\u08e9\u08e8\3\2\2\2\u08e9\u08ea"+
		"\3\2\2\2\u08ea\u08eb\3\2\2\2\u08eb\u08ee\7\u008b\2\2\u08ec\u08ee\7\u0087"+
		"\2\2\u08ed\u08e7\3\2\2\2\u08ed\u08ec\3\2\2\2\u08ee\u08fd\3\2\2\2\u08ef"+
		"\u08fa\5\u00caf\2\u08f0\u08f1\7\3\2\2\u08f1\u08f6\7\u00f7\2\2\u08f2\u08f3"+
		"\7\5\2\2\u08f3\u08f5\7\u00f7\2\2\u08f4\u08f2\3\2\2\2\u08f5\u08f8\3\2\2"+
		"\2\u08f6\u08f4\3\2\2\2\u08f6\u08f7\3\2\2\2\u08f7\u08f9\3\2\2\2\u08f8\u08f6"+
		"\3\2\2\2\u08f9\u08fb\7\4\2\2\u08fa\u08f0\3\2\2\2\u08fa\u08fb\3\2\2\2\u08fb"+
		"\u08fd\3\2\2\2\u08fc\u08da\3\2\2\2\u08fc\u08df\3\2\2\2\u08fc\u08e6\3\2"+
		"\2\2\u08fc\u08ef\3\2\2\2\u08fd\u00b3\3\2\2\2\u08fe\u0903\5\u00b6\\\2\u08ff"+
		"\u0900\7\5\2\2\u0900\u0902\5\u00b6\\\2\u0901\u08ff\3\2\2\2\u0902\u0905"+
		"\3\2\2\2\u0903\u0901\3\2\2\2\u0903\u0904\3\2\2\2\u0904\u00b5\3\2\2\2\u0905"+
		"\u0903\3\2\2\2\u0906\u0907\5\u00caf\2\u0907\u090a\5\u00b2Z\2\u0908\u0909"+
		"\7u\2\2\u0909\u090b\7\u00f3\2\2\u090a\u0908\3\2\2\2\u090a\u090b\3\2\2"+
		"\2\u090b\u00b7\3\2\2\2\u090c\u0911\5\u00ba^\2\u090d\u090e\7\5\2\2\u090e"+
		"\u0910\5\u00ba^\2\u090f\u090d\3\2\2\2\u0910\u0913\3\2\2\2\u0911\u090f"+
		"\3\2\2\2\u0911\u0912\3\2\2\2\u0912\u00b9\3\2\2\2\u0913\u0911\3\2\2\2\u0914"+
		"\u0915\5\u00caf\2\u0915\u0916\7\f\2\2\u0916\u0919\5\u00b2Z\2\u0917\u0918"+
		"\7u\2\2\u0918\u091a\7\u00f3\2\2\u0919\u0917\3\2\2\2\u0919\u091a\3\2\2"+
		"\2\u091a\u00bb\3\2\2\2\u091b\u091c\7\62\2\2\u091c\u091d\5\u0096L\2\u091d"+
		"\u091e\7\63\2\2\u091e\u091f\5\u0096L\2\u091f\u00bd\3\2\2\2\u0920\u0921"+
		"\7B\2\2\u0921";
	private static final String _serializedATNSegment1 =
		"\u0926\5\u00c0a\2\u0922\u0923\7\5\2\2\u0923\u0925\5\u00c0a\2\u0924\u0922"+
		"\3\2\2\2\u0925\u0928\3\2\2\2\u0926\u0924\3\2\2\2\u0926\u0927\3\2\2\2\u0927"+
		"\u00bf\3\2\2\2\u0928\u0926\3\2\2\2\u0929\u092a\5\u00caf\2\u092a\u092b"+
		"\7\20\2\2\u092b\u092c\5\u00c2b\2\u092c\u00c1\3\2\2\2\u092d\u0958\5\u00ca"+
		"f\2\u092e\u0951\7\3\2\2\u092f\u0930\7\u009d\2\2\u0930\u0931\7\26\2\2\u0931"+
		"\u0936\5\u0096L\2\u0932\u0933\7\5\2\2\u0933\u0935\5\u0096L\2\u0934\u0932"+
		"\3\2\2\2\u0935\u0938\3\2\2\2\u0936\u0934\3\2\2\2\u0936\u0937\3\2\2\2\u0937"+
		"\u0952\3\2\2\2\u0938\u0936\3\2\2\2\u0939\u093a\t\31\2\2\u093a\u093b\7"+
		"\26\2\2\u093b\u0940\5\u0096L\2\u093c\u093d\7\5\2\2\u093d\u093f\5\u0096"+
		"L\2\u093e\u093c\3\2\2\2\u093f\u0942\3\2\2\2\u0940\u093e\3\2\2\2\u0940"+
		"\u0941\3\2\2\2\u0941\u0944\3\2\2\2\u0942\u0940\3\2\2\2\u0943\u0939\3\2"+
		"\2\2\u0943\u0944\3\2\2\2\u0944\u094f\3\2\2\2\u0945\u0946\t\32\2\2\u0946"+
		"\u0947\7\26\2\2\u0947\u094c\5T+\2\u0948\u0949\7\5\2\2\u0949\u094b\5T+"+
		"\2\u094a\u0948\3\2\2\2\u094b\u094e\3\2\2\2\u094c\u094a\3\2\2\2\u094c\u094d"+
		"\3\2\2\2\u094d\u0950\3\2\2\2\u094e\u094c\3\2\2\2\u094f\u0945\3\2\2\2\u094f"+
		"\u0950\3\2\2\2\u0950\u0952\3\2\2\2\u0951\u092f\3\2\2\2\u0951\u0943\3\2"+
		"\2\2\u0952\u0954\3\2\2\2\u0953\u0955\5\u00c4c\2\u0954\u0953\3\2\2\2\u0954"+
		"\u0955\3\2\2\2\u0955\u0956\3\2\2\2\u0956\u0958\7\4\2\2\u0957\u092d\3\2"+
		"\2\2\u0957\u092e\3\2\2\2\u0958\u00c3\3\2\2\2\u0959\u095a\7E\2\2\u095a"+
		"\u096a\5\u00c6d\2\u095b\u095c\7F\2\2\u095c\u096a\5\u00c6d\2\u095d\u095e"+
		"\7E\2\2\u095e\u095f\7%\2\2\u095f\u0960\5\u00c6d\2\u0960\u0961\7 \2\2\u0961"+
		"\u0962\5\u00c6d\2\u0962\u096a\3\2\2\2\u0963\u0964\7F\2\2\u0964\u0965\7"+
		"%\2\2\u0965\u0966\5\u00c6d\2\u0966\u0967\7 \2\2\u0967\u0968\5\u00c6d\2"+
		"\u0968\u096a\3\2\2\2\u0969\u0959\3\2\2\2\u0969\u095b\3\2\2\2\u0969\u095d"+
		"\3\2\2\2\u0969\u0963\3\2\2\2\u096a\u00c5\3\2\2\2\u096b\u096c\7G\2\2\u096c"+
		"\u0973\t\33\2\2\u096d\u096e\7J\2\2\u096e\u0973\7N\2\2\u096f\u0970\5\u0096"+
		"L\2\u0970\u0971\t\33\2\2\u0971\u0973\3\2\2\2\u0972\u096b\3\2\2\2\u0972"+
		"\u096d\3\2\2\2\u0972\u096f\3\2\2\2\u0973\u00c7\3\2\2\2\u0974\u0979\5\u00ca"+
		"f\2\u0975\u0976\7\6\2\2\u0976\u0978\5\u00caf\2\u0977\u0975\3\2\2\2\u0978"+
		"\u097b\3\2\2\2\u0979\u0977\3\2\2\2\u0979\u097a\3\2\2\2\u097a\u00c9\3\2"+
		"\2\2\u097b\u0979\3\2\2\2\u097c\u098c\5\u00ccg\2\u097d\u098c\7\u00f0\2"+
		"\2\u097e\u098c\7=\2\2\u097f\u098c\79\2\2\u0980\u098c\7:\2\2\u0981\u098c"+
		"\7;\2\2\u0982\u098c\7<\2\2\u0983\u098c\7>\2\2\u0984\u098c\7\66\2\2\u0985"+
		"\u098c\7\67\2\2\u0986\u098c\7?\2\2\u0987\u098c\7i\2\2\u0988\u098c\7l\2"+
		"\2\u0989\u098c\7j\2\2\u098a\u098c\7k\2\2\u098b\u097c\3\2\2\2\u098b\u097d"+
		"\3\2\2\2\u098b\u097e\3\2\2\2\u098b\u097f\3\2\2\2\u098b\u0980\3\2\2\2\u098b"+
		"\u0981\3\2\2\2\u098b\u0982\3\2\2\2\u098b\u0983\3\2\2\2\u098b\u0984\3\2"+
		"\2\2\u098b\u0985\3\2\2\2\u098b\u0986\3\2\2\2\u098b\u0987\3\2\2\2\u098b"+
		"\u0988\3\2\2\2\u098b\u0989\3\2\2\2\u098b\u098a\3\2\2\2\u098c\u00cb\3\2"+
		"\2\2\u098d\u0991\7\u00fb\2\2\u098e\u0991\5\u00ceh\2\u098f\u0991\5\u00d2"+
		"j\2\u0990\u098d\3\2\2\2\u0990\u098e\3\2\2\2\u0990\u098f\3\2\2\2\u0991"+
		"\u00cd\3\2\2\2\u0992\u0993\7\u00fc\2\2\u0993\u00cf\3\2\2\2\u0994\u0996"+
		"\7\u008e\2\2\u0995\u0994\3\2\2\2\u0995\u0996\3\2\2\2\u0996\u0997\3\2\2"+
		"\2\u0997\u09b1\7\u00f8\2\2\u0998\u099a\7\u008e\2\2\u0999\u0998\3\2\2\2"+
		"\u0999\u099a\3\2\2\2\u099a\u099b\3\2\2\2\u099b\u09b1\7\u00f7\2\2\u099c"+
		"\u099e\7\u008e\2\2\u099d\u099c\3\2\2\2\u099d\u099e\3\2\2\2\u099e\u099f"+
		"\3\2\2\2\u099f\u09b1\7\u00f4\2\2\u09a0\u09a2\7\u008e\2\2\u09a1\u09a0\3"+
		"\2\2\2\u09a1\u09a2\3\2\2\2\u09a2\u09a3\3\2\2\2\u09a3\u09b1\7\u00f5\2\2"+
		"\u09a4\u09a6\7\u008e\2\2\u09a5\u09a4\3\2\2\2\u09a5\u09a6\3\2\2\2\u09a6"+
		"\u09a7\3\2\2\2\u09a7\u09b1\7\u00f6\2\2\u09a8\u09aa\7\u008e\2\2\u09a9\u09a8"+
		"\3\2\2\2\u09a9\u09aa\3\2\2\2\u09aa\u09ab\3\2\2\2\u09ab\u09b1\7\u00f9\2"+
		"\2\u09ac\u09ae\7\u008e\2\2\u09ad\u09ac\3\2\2\2\u09ad\u09ae\3\2\2\2\u09ae"+
		"\u09af\3\2\2\2\u09af\u09b1\7\u00fa\2\2\u09b0\u0995\3\2\2\2\u09b0\u0999"+
		"\3\2\2\2\u09b0\u099d\3\2\2\2\u09b0\u09a1\3\2\2\2\u09b0\u09a5\3\2\2\2\u09b0"+
		"\u09a9\3\2\2\2\u09b0\u09ad\3\2\2\2\u09b1\u00d1\3\2\2\2\u09b2\u09b3\t\34"+
		"\2\2\u09b3\u00d3\3\2\2\2\u014b\u00f0\u00f5\u00f8\u00fd\u010a\u010e\u0115"+
		"\u0123\u0125\u0129\u012c\u0133\u0144\u0146\u014a\u014d\u0154\u015a\u0160"+
		"\u0168\u0188\u0190\u0194\u0199\u019f\u01a7\u01ad\u01ba\u01bf\u01c8\u01cd"+
		"\u01dd\u01e4\u01e8\u01f0\u01f7\u01fe\u020d\u0211\u0217\u021d\u0220\u0223"+
		"\u0229\u022d\u0231\u0236\u023a\u0242\u0245\u024e\u0253\u0259\u0260\u0263"+
		"\u0269\u0274\u0277\u027b\u0280\u0285\u028c\u028f\u0292\u0299\u029e\u02a3"+
		"\u02a6\u02af\u02b7\u02bd\u02c1\u02c5\u02c9\u02cb\u02d4\u02da\u02df\u02e2"+
		"\u02e6\u02e9\u02f3\u02f6\u02fa\u02ff\u0302\u0308\u0310\u0315\u031b\u0321"+
		"\u032c\u0334\u033b\u0343\u0346\u034e\u0352\u0359\u03cd\u03d5\u03dd\u03e6"+
		"\u03f0\u03f7\u03ff\u0406\u040f\u0412\u0418\u0422\u042e\u0433\u0439\u0445"+
		"\u0447\u044c\u0450\u0455\u045a\u045d\u0462\u0466\u046b\u046d\u0471\u047a"+
		"\u0482\u0489\u0490\u0499\u049e\u04ad\u04b4\u04b7\u04be\u04c2\u04c8\u04d0"+
		"\u04db\u04e6\u04ed\u04f3\u04f9\u0502\u0504\u050d\u0510\u0519\u051c\u0525"+
		"\u0528\u0531\u0534\u0537\u053c\u053e\u0541\u054d\u0554\u055b\u055e\u0560"+
		"\u056b\u056f\u0573\u057f\u0582\u0586\u0590\u0594\u0596\u0599\u059d\u05a0"+
		"\u05a4\u05aa\u05ae\u05b2\u05b7\u05ba\u05bc\u05c1\u05c6\u05c9\u05cd\u05d0"+
		"\u05d2\u05d7\u05dc\u05e9\u05ee\u05f6\u05fc\u0600\u0609\u0618\u061d\u0629"+
		"\u062e\u0636\u0639\u063d\u064b\u0658\u065d\u0661\u0664\u0669\u0672\u0675"+
		"\u067a\u0681\u0684\u068c\u0693\u069a\u069d\u06a2\u06a8\u06ac\u06af\u06b2"+
		"\u06bd\u06c2\u06c7\u06cc\u06de\u06e0\u06e3\u06ee\u06f7\u06fe\u0706\u070e"+
		"\u0712\u071a\u0722\u0728\u0730\u073c\u073f\u0745\u0749\u074b\u0754\u0760"+
		"\u0762\u0769\u0770\u0776\u077c\u077e\u0783\u078a\u0790\u0794\u0796\u079d"+
		"\u07ac\u07ae\u07b6\u07b8\u07bc\u07c4\u07cd\u07d3\u07db\u07e1\u07e6\u07eb"+
		"\u07f1\u0806\u0808\u0810\u0814\u081d\u0821\u0833\u0836\u083e\u0847\u085e"+
		"\u0869\u0870\u0873\u0878\u088b\u089c\u08a6\u08a8\u08b5\u08b7\u08c5\u08cc"+
		"\u08cf\u08d3\u08d8\u08e9\u08ed\u08f6\u08fa\u08fc\u0903\u090a\u0911\u0919"+
		"\u0926\u0936\u0940\u0943\u094c\u094f\u0951\u0954\u0957\u0969\u0972\u0979"+
		"\u098b\u0990\u0995\u0999\u099d\u09a1\u09a5\u09a9\u09ad\u09b0";
	public static final String _serializedATN = Utils.join(
		new String[] {
			_serializedATNSegment0,
			_serializedATNSegment1
		},
		""
	);
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}