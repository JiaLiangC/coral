/**
   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

// This file is copied from HiveParser.g of Hive 1.2.1

parser grammar HiveParser;

options
{
tokenVocab=HiveLexer;
output=AST;
ASTLabelType=CommonTree;
backtrack=false;
k=3;
}
import SelectClauseParser, FromClauseParser, IdentifiersParser;

tokens {
TOK_INSERT;
TOK_QUERY;
TOK_SELECT;
TOK_SELECTDI;
TOK_SELEXPR;
TOK_FROM;
TOK_TAB;
TOK_PARTSPEC;
TOK_PARTVAL;
TOK_DIR;
TOK_TABREF;
TOK_SUBQUERY;
TOK_INSERT_INTO;
TOK_DESTINATION;
TOK_ALLCOLREF;
TOK_TABLE_OR_COL;
TOK_FUNCTION;
TOK_FUNCTIONDI;
TOK_FUNCTIONSTAR;
TOK_WHERE;
TOK_OP_EQ;
TOK_OP_NE;
TOK_OP_LE;
TOK_OP_LT;
TOK_OP_GE;
TOK_OP_GT;
TOK_OP_DIV;
TOK_OP_ADD;
TOK_OP_SUB;
TOK_OP_MUL;
TOK_OP_MOD;
TOK_OP_BITAND;
TOK_OP_BITNOT;
TOK_OP_BITOR;
TOK_OP_BITXOR;
TOK_OP_AND;
TOK_OP_OR;
TOK_OP_NOT;
TOK_OP_LIKE;
TOK_TRUE;
TOK_FALSE;
TOK_TRANSFORM;
TOK_SERDE;
TOK_SERDENAME;
TOK_SERDEPROPS;
TOK_EXPLIST;
TOK_ALIASLIST;
TOK_GROUPBY;
TOK_ROLLUP_GROUPBY;
TOK_CUBE_GROUPBY;
TOK_GROUPING_SETS;
TOK_GROUPING_SETS_EXPRESSION;
TOK_HAVING;
TOK_ORDERBY;
TOK_CLUSTERBY;
TOK_DISTRIBUTEBY;
TOK_SORTBY;
TOK_UNIONALL;
TOK_UNIONDISTINCT;
TOK_JOIN;
TOK_LEFTOUTERJOIN;
TOK_RIGHTOUTERJOIN;
TOK_FULLOUTERJOIN;
TOK_UNIQUEJOIN;
TOK_CROSSJOIN;
TOK_LOAD;
TOK_EXPORT;
TOK_IMPORT;
TOK_REPLICATION;
TOK_METADATA;
TOK_NULL;
TOK_ISNULL;
TOK_ISNOTNULL;
TOK_TINYINT;
TOK_SMALLINT;
TOK_INT;
TOK_BIGINT;
TOK_BOOLEAN;
TOK_FLOAT;
TOK_DOUBLE;
TOK_DATE;
TOK_DATELITERAL;
TOK_DATETIME;
TOK_TIMESTAMP;
TOK_TIMESTAMPLITERAL;
TOK_INTERVAL_YEAR_MONTH;
TOK_INTERVAL_YEAR_MONTH_LITERAL;
TOK_INTERVAL_DAY_TIME;
TOK_INTERVAL_DAY_TIME_LITERAL;
TOK_INTERVAL_YEAR_LITERAL;
TOK_INTERVAL_MONTH_LITERAL;
TOK_INTERVAL_DAY_LITERAL;
TOK_INTERVAL_HOUR_LITERAL;
TOK_INTERVAL_MINUTE_LITERAL;
TOK_INTERVAL_SECOND_LITERAL;
TOK_STRING;
TOK_CHAR;
TOK_VARCHAR;
TOK_BINARY;
TOK_DECIMAL;
TOK_LIST;
TOK_STRUCT;
TOK_MAP;
TOK_UNIONTYPE;
TOK_COLTYPELIST;
TOK_CREATEDATABASE;
TOK_CREATETABLE;
TOK_TRUNCATETABLE;
TOK_CREATEINDEX;
TOK_CREATEINDEX_INDEXTBLNAME;
TOK_DEFERRED_REBUILDINDEX;
TOK_DROPINDEX;
TOK_LIKETABLE;
TOK_DESCTABLE;
TOK_DESCFUNCTION;
TOK_ALTERTABLE;
TOK_ALTERTABLE_RENAME;
TOK_ALTERTABLE_ADDCOLS;
TOK_ALTERTABLE_RENAMECOL;
TOK_ALTERTABLE_RENAMEPART;
TOK_ALTERTABLE_REPLACECOLS;
TOK_ALTERTABLE_ADDPARTS;
TOK_ALTERTABLE_DROPPARTS;
TOK_ALTERTABLE_PARTCOLTYPE;
TOK_ALTERTABLE_PROTECTMODE;
TOK_ALTERTABLE_MERGEFILES;
TOK_ALTERTABLE_TOUCH;
TOK_ALTERTABLE_ARCHIVE;
TOK_ALTERTABLE_UNARCHIVE;
TOK_ALTERTABLE_SERDEPROPERTIES;
TOK_ALTERTABLE_SERIALIZER;
TOK_ALTERTABLE_UPDATECOLSTATS;
TOK_TABLE_PARTITION;
TOK_ALTERTABLE_FILEFORMAT;
TOK_ALTERTABLE_LOCATION;
TOK_ALTERTABLE_PROPERTIES;
TOK_ALTERTABLE_CHANGECOL_AFTER_POSITION;
TOK_ALTERTABLE_DROPPROPERTIES;
TOK_ALTERTABLE_SKEWED;
TOK_ALTERTABLE_EXCHANGEPARTITION;
TOK_ALTERTABLE_SKEWED_LOCATION;
TOK_ALTERTABLE_BUCKETS;
TOK_ALTERTABLE_CLUSTER_SORT;
TOK_ALTERTABLE_COMPACT;
TOK_ALTERINDEX_REBUILD;
TOK_ALTERINDEX_PROPERTIES;
TOK_MSCK;
TOK_SHOWDATABASES;
TOK_SHOWTABLES;
TOK_SHOWCOLUMNS;
TOK_SHOWFUNCTIONS;
TOK_SHOWPARTITIONS;
TOK_SHOW_CREATETABLE;
TOK_SHOW_TABLESTATUS;
TOK_SHOW_TBLPROPERTIES;
TOK_SHOWLOCKS;
TOK_SHOWCONF;
TOK_LOCKTABLE;
TOK_UNLOCKTABLE;
TOK_LOCKDB;
TOK_UNLOCKDB;
TOK_SWITCHDATABASE;
TOK_DROPDATABASE;
TOK_DROPTABLE;
TOK_DATABASECOMMENT;
TOK_TABCOLLIST;
TOK_TABCOL;
TOK_TABLECOMMENT;
TOK_TABLEPARTCOLS;
TOK_TABLEROWFORMAT;
TOK_TABLEROWFORMATFIELD;
TOK_TABLEROWFORMATCOLLITEMS;
TOK_TABLEROWFORMATMAPKEYS;
TOK_TABLEROWFORMATLINES;
TOK_TABLEROWFORMATNULL;
TOK_TABLEFILEFORMAT;
TOK_FILEFORMAT_GENERIC;
TOK_OFFLINE;
TOK_ENABLE;
TOK_DISABLE;
TOK_READONLY;
TOK_NO_DROP;
TOK_STORAGEHANDLER;
TOK_NOT_CLUSTERED;
TOK_NOT_SORTED;
TOK_TABCOLNAME;
TOK_TABLELOCATION;
TOK_PARTITIONLOCATION;
TOK_TABLEBUCKETSAMPLE;
TOK_TABLESPLITSAMPLE;
TOK_PERCENT;
TOK_LENGTH;
TOK_ROWCOUNT;
TOK_TMP_FILE;
TOK_TABSORTCOLNAMEASC;
TOK_TABSORTCOLNAMEDESC;
TOK_STRINGLITERALSEQUENCE;
TOK_CHARSETLITERAL;
TOK_CREATEFUNCTION;
TOK_DROPFUNCTION;
TOK_RELOADFUNCTION;
TOK_CREATEMACRO;
TOK_DROPMACRO;
TOK_TEMPORARY;
TOK_CREATEVIEW;
TOK_DROPVIEW;
TOK_ALTERVIEW;
TOK_ALTERVIEW_PROPERTIES;
TOK_ALTERVIEW_DROPPROPERTIES;
TOK_ALTERVIEW_ADDPARTS;
TOK_ALTERVIEW_DROPPARTS;
TOK_ALTERVIEW_RENAME;
TOK_VIEWPARTCOLS;
TOK_EXPLAIN;
TOK_EXPLAIN_SQ_REWRITE;
TOK_TABLESERIALIZER;
TOK_TABLEPROPERTIES;
TOK_TABLEPROPLIST;
TOK_INDEXPROPERTIES;
TOK_INDEXPROPLIST;
TOK_TABTYPE;
TOK_LIMIT;
TOK_TABLEPROPERTY;
TOK_IFEXISTS;
TOK_IFNOTEXISTS;
TOK_ORREPLACE;
TOK_HINTLIST;
TOK_HINT;
TOK_MAPJOIN;
TOK_STREAMTABLE;
TOK_HOLD_DDLTIME;
TOK_HINTARGLIST;
TOK_USERSCRIPTCOLNAMES;
TOK_USERSCRIPTCOLSCHEMA;
TOK_RECORDREADER;
TOK_RECORDWRITER;
TOK_LEFTSEMIJOIN;
TOK_LATERAL_VIEW;
TOK_LATERAL_VIEW_OUTER;
TOK_TABALIAS;
TOK_ANALYZE;
TOK_CREATEROLE;
TOK_DROPROLE;
TOK_GRANT;
TOK_REVOKE;
TOK_SHOW_GRANT;
TOK_PRIVILEGE_LIST;
TOK_PRIVILEGE;
TOK_PRINCIPAL_NAME;
TOK_USER;
TOK_GROUP;
TOK_ROLE;
TOK_RESOURCE_ALL;
TOK_GRANT_WITH_OPTION;
TOK_GRANT_WITH_ADMIN_OPTION;
TOK_ADMIN_OPTION_FOR;
TOK_GRANT_OPTION_FOR;
TOK_PRIV_ALL;
TOK_PRIV_ALTER_METADATA;
TOK_PRIV_ALTER_DATA;
TOK_PRIV_DELETE;
TOK_PRIV_DROP;
TOK_PRIV_INDEX;
TOK_PRIV_INSERT;
TOK_PRIV_LOCK;
TOK_PRIV_SELECT;
TOK_PRIV_SHOW_DATABASE;
TOK_PRIV_CREATE;
TOK_PRIV_OBJECT;
TOK_PRIV_OBJECT_COL;
TOK_GRANT_ROLE;
TOK_REVOKE_ROLE;
TOK_SHOW_ROLE_GRANT;
TOK_SHOW_ROLES;
TOK_SHOW_SET_ROLE;
TOK_SHOW_ROLE_PRINCIPALS;
TOK_SHOWINDEXES;
TOK_SHOWDBLOCKS;
TOK_INDEXCOMMENT;
TOK_DESCDATABASE;
TOK_DATABASEPROPERTIES;
TOK_DATABASELOCATION;
TOK_DBPROPLIST;
TOK_ALTERDATABASE_PROPERTIES;
TOK_ALTERDATABASE_OWNER;
TOK_TABNAME;
TOK_TABSRC;
TOK_RESTRICT;
TOK_CASCADE;
TOK_TABLESKEWED;
TOK_TABCOLVALUE;
TOK_TABCOLVALUE_PAIR;
TOK_TABCOLVALUES;
TOK_SKEWED_LOCATIONS;
TOK_SKEWED_LOCATION_LIST;
TOK_SKEWED_LOCATION_MAP;
TOK_STOREDASDIRS;
TOK_PARTITIONINGSPEC;
TOK_PTBLFUNCTION;
TOK_WINDOWDEF;
TOK_WINDOWSPEC;
TOK_WINDOWVALUES;
TOK_WINDOWRANGE;
TOK_IGNOREPROTECTION;
TOK_SUBQUERY_EXPR;
TOK_SUBQUERY_OP;
TOK_SUBQUERY_OP_NOTIN;
TOK_SUBQUERY_OP_NOTEXISTS;
TOK_DB_TYPE;
TOK_TABLE_TYPE;
TOK_CTE;
TOK_ARCHIVE;
TOK_FILE;
TOK_JAR;
TOK_RESOURCE_URI;
TOK_RESOURCE_LIST;
TOK_SHOW_COMPACTIONS;
TOK_SHOW_TRANSACTIONS;
TOK_DELETE_FROM;
TOK_UPDATE_TABLE;
TOK_SET_COLUMNS_CLAUSE;
TOK_VALUE_ROW;
TOK_VALUES_TABLE;
TOK_VIRTUAL_TABLE;
TOK_VIRTUAL_TABREF;
TOK_ANONYMOUS;
TOK_COL_NAME;
TOK_URI_TYPE;
TOK_SERVER_TYPE;
}


// Package headers
@header {
package com.linkedin.coral.hive.hive2rel.parsetree.parser;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
}


@members {
  private static final Logger LOG = LoggerFactory.getLogger(HiveParser.class);
  ArrayList<ParseError> errors = new ArrayList<ParseError>();
  Stack msgs = new Stack<String>();

  private static HashMap<String, String> xlateMap;
  static {
    xlateMap = new HashMap<String, String>();

    // Keywords
    xlateMap.put("KW_TRUE", "TRUE");
    xlateMap.put("KW_FALSE", "FALSE");
    xlateMap.put("KW_ALL", "ALL");
    xlateMap.put("KW_NONE", "NONE");
    xlateMap.put("KW_AND", "AND");
    xlateMap.put("KW_OR", "OR");
    xlateMap.put("KW_NOT", "NOT");
    xlateMap.put("KW_LIKE", "LIKE");

    xlateMap.put("KW_ASC", "ASC");
    xlateMap.put("KW_DESC", "DESC");
    xlateMap.put("KW_ORDER", "ORDER");
    xlateMap.put("KW_BY", "BY");
    xlateMap.put("KW_GROUP", "GROUP");
    xlateMap.put("KW_WHERE", "WHERE");
    xlateMap.put("KW_FROM", "FROM");
    xlateMap.put("KW_AS", "AS");
    xlateMap.put("KW_SELECT", "SELECT");
    xlateMap.put("KW_DISTINCT", "DISTINCT");
    xlateMap.put("KW_INSERT", "INSERT");
    xlateMap.put("KW_OVERWRITE", "OVERWRITE");
    xlateMap.put("KW_OUTER", "OUTER");
    xlateMap.put("KW_JOIN", "JOIN");
    xlateMap.put("KW_LEFT", "LEFT");
    xlateMap.put("KW_RIGHT", "RIGHT");
    xlateMap.put("KW_FULL", "FULL");
    xlateMap.put("KW_ON", "ON");
    xlateMap.put("KW_PARTITION", "PARTITION");
    xlateMap.put("KW_PARTITIONS", "PARTITIONS");
    xlateMap.put("KW_TABLE", "TABLE");
    xlateMap.put("KW_TABLES", "TABLES");
    xlateMap.put("KW_TBLPROPERTIES", "TBLPROPERTIES");
    xlateMap.put("KW_SHOW", "SHOW");
    xlateMap.put("KW_MSCK", "MSCK");
    xlateMap.put("KW_DIRECTORY", "DIRECTORY");
    xlateMap.put("KW_LOCAL", "LOCAL");
    xlateMap.put("KW_TRANSFORM", "TRANSFORM");
    xlateMap.put("KW_USING", "USING");
    xlateMap.put("KW_CLUSTER", "CLUSTER");
    xlateMap.put("KW_DISTRIBUTE", "DISTRIBUTE");
    xlateMap.put("KW_SORT", "SORT");
    xlateMap.put("KW_UNION", "UNION");
    xlateMap.put("KW_LOAD", "LOAD");
    xlateMap.put("KW_DATA", "DATA");
    xlateMap.put("KW_INPATH", "INPATH");
    xlateMap.put("KW_IS", "IS");
    xlateMap.put("KW_NULL", "NULL");
    xlateMap.put("KW_CREATE", "CREATE");
    xlateMap.put("KW_EXTERNAL", "EXTERNAL");
    xlateMap.put("KW_ALTER", "ALTER");
    xlateMap.put("KW_DESCRIBE", "DESCRIBE");
    xlateMap.put("KW_DROP", "DROP");
    xlateMap.put("KW_RENAME", "RENAME");
    xlateMap.put("KW_TO", "TO");
    xlateMap.put("KW_COMMENT", "COMMENT");
    xlateMap.put("KW_BOOLEAN", "BOOLEAN");
    xlateMap.put("KW_TINYINT", "TINYINT");
    xlateMap.put("KW_SMALLINT", "SMALLINT");
    xlateMap.put("KW_INT", "INT");
    xlateMap.put("KW_BIGINT", "BIGINT");
    xlateMap.put("KW_FLOAT", "FLOAT");
    xlateMap.put("KW_DOUBLE", "DOUBLE");
    xlateMap.put("KW_DATE", "DATE");
    xlateMap.put("KW_DATETIME", "DATETIME");
    xlateMap.put("KW_TIMESTAMP", "TIMESTAMP");
    xlateMap.put("KW_STRING", "STRING");
    xlateMap.put("KW_BINARY", "BINARY");
    xlateMap.put("KW_ARRAY", "ARRAY");
    xlateMap.put("KW_MAP", "MAP");
    xlateMap.put("KW_REDUCE", "REDUCE");
    xlateMap.put("KW_PARTITIONED", "PARTITIONED");
    xlateMap.put("KW_CLUSTERED", "CLUSTERED");
    xlateMap.put("KW_SORTED", "SORTED");
    xlateMap.put("KW_INTO", "INTO");
    xlateMap.put("KW_BUCKETS", "BUCKETS");
    xlateMap.put("KW_ROW", "ROW");
    xlateMap.put("KW_FORMAT", "FORMAT");
    xlateMap.put("KW_DELIMITED", "DELIMITED");
    xlateMap.put("KW_FIELDS", "FIELDS");
    xlateMap.put("KW_TERMINATED", "TERMINATED");
    xlateMap.put("KW_COLLECTION", "COLLECTION");
    xlateMap.put("KW_ITEMS", "ITEMS");
    xlateMap.put("KW_KEYS", "KEYS");
    xlateMap.put("KW_KEY_TYPE", "\$KEY\$");
    xlateMap.put("KW_LINES", "LINES");
    xlateMap.put("KW_STORED", "STORED");
    xlateMap.put("KW_SEQUENCEFILE", "SEQUENCEFILE");
    xlateMap.put("KW_TEXTFILE", "TEXTFILE");
    xlateMap.put("KW_INPUTFORMAT", "INPUTFORMAT");
    xlateMap.put("KW_OUTPUTFORMAT", "OUTPUTFORMAT");
    xlateMap.put("KW_LOCATION", "LOCATION");
    xlateMap.put("KW_TABLESAMPLE", "TABLESAMPLE");
    xlateMap.put("KW_BUCKET", "BUCKET");
    xlateMap.put("KW_OUT", "OUT");
    xlateMap.put("KW_OF", "OF");
    xlateMap.put("KW_CAST", "CAST");
    xlateMap.put("KW_ADD", "ADD");
    xlateMap.put("KW_REPLACE", "REPLACE");
    xlateMap.put("KW_COLUMNS", "COLUMNS");
    xlateMap.put("KW_RLIKE", "RLIKE");
    xlateMap.put("KW_REGEXP", "REGEXP");
    xlateMap.put("KW_TEMPORARY", "TEMPORARY");
    xlateMap.put("KW_FUNCTION", "FUNCTION");
    xlateMap.put("KW_EXPLAIN", "EXPLAIN");
    xlateMap.put("KW_EXTENDED", "EXTENDED");
    xlateMap.put("KW_SERDE", "SERDE");
    xlateMap.put("KW_WITH", "WITH");
    xlateMap.put("KW_SERDEPROPERTIES", "SERDEPROPERTIES");
    xlateMap.put("KW_LIMIT", "LIMIT");
    xlateMap.put("KW_SET", "SET");
    xlateMap.put("KW_PROPERTIES", "TBLPROPERTIES");
    xlateMap.put("KW_VALUE_TYPE", "\$VALUE\$");
    xlateMap.put("KW_ELEM_TYPE", "\$ELEM\$");
    xlateMap.put("KW_DEFINED", "DEFINED");
    xlateMap.put("KW_SUBQUERY", "SUBQUERY");
    xlateMap.put("KW_REWRITE", "REWRITE");
    xlateMap.put("KW_UPDATE", "UPDATE");
    xlateMap.put("KW_VALUES", "VALUES");
    xlateMap.put("KW_PURGE", "PURGE");


    // Operators
    xlateMap.put("DOT", ".");
    xlateMap.put("COLON", ":");
    xlateMap.put("COMMA", ",");
    xlateMap.put("SEMICOLON", ");");

    xlateMap.put("LPAREN", "(");
    xlateMap.put("RPAREN", ")");
    xlateMap.put("LSQUARE", "[");
    xlateMap.put("RSQUARE", "]");

    xlateMap.put("EQUAL", "=");
    xlateMap.put("NOTEQUAL", "<>");
    xlateMap.put("EQUAL_NS", "<=>");
    xlateMap.put("LESSTHANOREQUALTO", "<=");
    xlateMap.put("LESSTHAN", "<");
    xlateMap.put("GREATERTHANOREQUALTO", ">=");
    xlateMap.put("GREATERTHAN", ">");

    xlateMap.put("DIVIDE", "/");
    xlateMap.put("PLUS", "+");
    xlateMap.put("MINUS", "-");
    xlateMap.put("STAR", "*");
    xlateMap.put("MOD", "\%");

    xlateMap.put("AMPERSAND", "&");
    xlateMap.put("TILDE", "~");
    xlateMap.put("BITWISEOR", "|");
    xlateMap.put("CONCATENATE", "||");
    xlateMap.put("BITWISEXOR", "^");
    xlateMap.put("CharSetLiteral", "\\'");
  }

  public static Collection<String> getKeywords() {
    return xlateMap.values();
  }

  private static String xlate(String name) {

    String ret = xlateMap.get(name);
    if (ret == null) {
      ret = name;
    }

    return ret;
  }

  @Override
  public Object recoverFromMismatchedSet(IntStream input,
      RecognitionException re, BitSet follow) throws RecognitionException {
    throw re;
  }

  @Override
  public void displayRecognitionError(String[] tokenNames,
      RecognitionException e) {
    errors.add(new ParseError(this, e, tokenNames));
  }

  @Override
  public String getErrorHeader(RecognitionException e) {
    String header = null;
    if (e.charPositionInLine < 0 && input.LT(-1) != null) {
      Token t = input.LT(-1);
      header = "line " + t.getLine() + ":" + t.getCharPositionInLine();
    } else {
      header = super.getErrorHeader(e);
    }

    return header;
  }

  @Override
  public String getErrorMessage(RecognitionException e, String[] tokenNames) {
    String msg = null;

    // Translate the token names to something that the user can understand
    String[] xlateNames = new String[tokenNames.length];
    for (int i = 0; i < tokenNames.length; ++i) {
      xlateNames[i] = HiveParser.xlate(tokenNames[i]);
    }

    if (e instanceof NoViableAltException) {
      @SuppressWarnings("unused")
      NoViableAltException nvae = (NoViableAltException) e;
      // for development, can add
      // "decision=<<"+nvae.grammarDecisionDescription+">>"
      // and "(decision="+nvae.decisionNumber+") and
      // "state "+nvae.stateNumber
      msg = "cannot recognize input near"
              + (input.LT(1) != null ? " " + getTokenErrorDisplay(input.LT(1)) : "")
              + (input.LT(2) != null ? " " + getTokenErrorDisplay(input.LT(2)) : "")
              + (input.LT(3) != null ? " " + getTokenErrorDisplay(input.LT(3)) : "");
    } else if (e instanceof MismatchedTokenException) {
      MismatchedTokenException mte = (MismatchedTokenException) e;
      msg = super.getErrorMessage(e, xlateNames) + (input.LT(-1) == null ? "":" near '" + input.LT(-1).getText()) + "'";
    } else if (e instanceof FailedPredicateException) {
      FailedPredicateException fpe = (FailedPredicateException) e;
      msg = "Failed to recognize predicate '" + fpe.token.getText() + "'. Failed rule: '" + fpe.ruleName + "'";
    } else {
      msg = super.getErrorMessage(e, xlateNames);
    }

    if (msgs.size() > 0) {
      msg = msg + " in " + msgs.peek();
    }
    return msg;
  }

  public void pushMsg(String msg, RecognizerSharedState state) {
    // ANTLR generated code does not wrap the @init code wit this backtracking check,
    //  even if the matching @after has it. If we have parser rules with that are doing
    // some lookahead with syntactic predicates this can cause the push() and pop() calls
    // to become unbalanced, so make sure both push/pop check the backtracking state.
    if (state.backtracking == 0) {
      msgs.push(msg);
    }
  }

  public void popMsg(RecognizerSharedState state) {
    if (state.backtracking == 0) {
      msgs.pop();
    }
  }

  // counter to generate unique union aliases
  private int aliasCounter;
  private String generateUnionAlias() {
    return "_u" + (++aliasCounter);
  }
  private char [] excludedCharForColumnName = {'.', ':'};
  private boolean containExcludedCharForCreateTableColumnName(String input) {
    for(char c : excludedCharForColumnName) {
      if(input.indexOf(c)>-1) {
        return true;
      }
    }
    return false;
  }
  private CommonTree throwSetOpException() throws RecognitionException {
    throw new FailedPredicateException(input, "orderByClause clusterByClause distributeByClause sortByClause limitClause can only be applied to the whole union.", "");
  }
  private CommonTree throwColumnNameException() throws RecognitionException {
    throw new FailedPredicateException(input, Arrays.toString(excludedCharForColumnName) + " can not be used in column name in create table statement.", "");
  }
  private Configuration hiveConf;
  public void setHiveConf(Configuration hiveConf) {
    this.hiveConf = hiveConf;
  }
  protected boolean useSQL11ReservedKeywordsForIdentifier() {
    try {
      /*
       * Use the config string hive.support.sql11.reserved.keywords directly as
       * HiveConf.ConfVars.HIVE_SUPPORT_SQL11_RESERVED_KEYWORDS might not be available in the hive-common present in the
       * classpath during translation triggering the exception path defaulting to false
       */
      return !hiveConf.get("hive.support.sql11.reserved.keywords").equalsIgnoreCase("true");
    } catch (Throwable throwable) {
      LOG.warn(throwable.getMessage());
      return false;
    }
  }
}

@rulecatch {
catch (RecognitionException e) {
 reportError(e);
  throw e;
}
}

// starting rule
//explainStatement 和 execStatement 作为你的两个备选子规则。
//语法树的根节点通常（大多数情况下）会是具体匹配的子规则的根节点。也就是说，如果 explainStatement 匹配成功，那么根节点会是 explainStatement 的生成节点
statement
	: explainStatement EOF
	| execStatement EOF
	;

//AST.1 TOK_EXPLAIN //AST.2 TOK_EXPLAIN_SQ_REWRITE
explainStatement
@init { pushMsg("explain statement", state); }
@after { popMsg(state); }
	: KW_EXPLAIN (
	    explainOption* execStatement -> ^(TOK_EXPLAIN execStatement explainOption*)
        |
        KW_REWRITE queryStatementExpression[true] -> ^(TOK_EXPLAIN_SQ_REWRITE queryStatementExpression))
	;

//成为上一个的子节点 AST.leafnode of AST.1
explainOption
@init { msgs.push("explain option"); }
@after { msgs.pop(); }
    : KW_EXTENDED|KW_FORMATTED|KW_DEPENDENCY|KW_LOGICAL|KW_AUTHORIZATION
    ;

//AST.3 AST.subnode of  AST.1
execStatement
@init { pushMsg("statement", state); }
@after { popMsg(state); }
    : queryStatementExpression[true]
    | loadStatement
    | exportStatement
    | importStatement
    | ddlStatement
    | deleteStatement
    | updateStatement
    ;


//AST.4 TOK_LOAD AST.subnode of AST.3
loadStatement
@init { pushMsg("load statement", state); }
@after { popMsg(state); }
    : KW_LOAD KW_DATA (islocal=KW_LOCAL)? KW_INPATH (path=StringLiteral) (isoverwrite=KW_OVERWRITE)? KW_INTO KW_TABLE (tab=tableOrPartition)
    -> ^(TOK_LOAD $path $tab $islocal? $isoverwrite?)
    ;


//AST.5 TOK_REPLICATION
replicationClause
@init { pushMsg("replication clause", state); }
@after { popMsg(state); }
    : KW_FOR (isMetadataOnly=KW_METADATA)? KW_REPLICATION LPAREN (replId=StringLiteral) RPAREN
    -> ^(TOK_REPLICATION $replId $isMetadataOnly?)
    ;

//AST.6 TOK_EXPORT AST.subnode of AST.3
//Export命令可以导出一张表或分区的数据和元数据信息到一个输出位置，并且导出数据可以被移动到另一个hadoop集群或hive实例，并且可以通过import命令导入数据。
//export table table_name to '/tmp/bak/20221220/xc_ods/table_name';
exportStatement
@init { pushMsg("export statement", state); }
@after { popMsg(state); }
    : KW_EXPORT
      KW_TABLE (tab=tableOrPartition)
      KW_TO (path=StringLiteral)
      replicationClause?
    -> ^(TOK_EXPORT $tab $path replicationClause?)
    ;


//AST.7 TOK_IMPORT AST.subnode of AST.3
//import from '/home/hadoop/data'; 通过Import方式可以把数据加载到指定Hive表中。但是这种方法需要先用Export导出后，再将数据导入。
importStatement
@init { pushMsg("import statement", state); }
@after { popMsg(state); }
       : KW_IMPORT
         ((ext=KW_EXTERNAL)? KW_TABLE (tab=tableOrPartition))?
         KW_FROM (path=StringLiteral)
         tableLocation?
    -> ^(TOK_IMPORT $path $tab? $ext? tableLocation?)
    ;


//AST.8
ddlStatement
@init { pushMsg("ddl statement", state); }
@after { popMsg(state); }
    : createDatabaseStatement
    | switchDatabaseStatement
    | dropDatabaseStatement
    | createTableStatement
    | dropTableStatement
    | truncateTableStatement
    | alterStatement
    | descStatement
    | showStatement
    | metastoreCheck
    | createViewStatement
    | dropViewStatement
    | createFunctionStatement
    | createMacroStatement
    | createIndexStatement
    | dropIndexStatement
    | dropFunctionStatement
    | reloadFunctionStatement
    | dropMacroStatement
    | analyzeStatement
    | lockStatement
    | unlockStatement
    | lockDatabase
    | unlockDatabase
    | createRoleStatement
    | dropRoleStatement
    | (grantPrivileges) => grantPrivileges
    | (revokePrivileges) => revokePrivileges
    | showGrants
    | showRoleGrants
    | showRolePrincipals
    | showRoles
    | grantRole
    | revokeRole
    | setRole
    | showCurrentRole
    ;

//AST.9  TOK_IFEXISTS
ifExists
@init { pushMsg("if exists clause", state); }
@after { popMsg(state); }
    : KW_IF KW_EXISTS
    -> ^(TOK_IFEXISTS)
    ;

// hive CASCADE的中文翻译为"级联"，顾名思义就是就是与之有联系的。在针对HIVE也就是不仅变更新分区的表结构（metadata），同时也变更旧分区的表结构。
//在生产环境中,由于用户对HIVE知识了解不深,在针对分区表时对表新增字段,没有使用cascade关键字,那么此时对于历史分区无论是使用insert into还是insert overwrite table 插入数据新增的列显示都是null值
//ALTER TABLE aiops.par_c  CHANGE COLUMN loc loc2 string CASCADE;
//AST.10 AST.11 TOK_CASCADE
restrictOrCascade
@init { pushMsg("restrict or cascade clause", state); }
@after { popMsg(state); }
    : KW_RESTRICT
    -> ^(TOK_RESTRICT)
    | KW_CASCADE
    -> ^(TOK_CASCADE)
    ;

// create table ifNotExists AST.12 TOK_IFNOTEXISTS
ifNotExists
@init { pushMsg("if not exists clause", state); }
@after { popMsg(state); }
    : KW_IF KW_NOT KW_EXISTS
    -> ^(TOK_IFNOTEXISTS)
    ;

//HIVE Skewed Table TOK_STOREDASDIRS  AST.13 TOK_STOREDASDIRS
//CREATE TABLE list_bucket_single (key STRING, value STRING) SKEWED BY (key) ON (1,5,6) [STORED AS DIRECTORIES];
storedAsDirs
@init { pushMsg("stored as directories", state); }
@after { popMsg(state); }
    : KW_STORED KW_AS KW_DIRECTORIES
    -> ^(TOK_STOREDASDIRS)
    ;

//  TOK_ORREPLACE  AST.14  CREATE OR REPLACE VIEW
orReplace
@init { pushMsg("or replace clause", state); }
@after { popMsg(state); }
    : KW_OR KW_REPLACE
    -> ^(TOK_ORREPLACE)
    ;


//  TOK_ORREPLACE  AST.15 TOK_IGNOREPROTECTION ALTER TABLE table_name DROP [IF EXISTS] PARTITION partition_spec IGNORE PROTECTION;
ignoreProtection
@init { pushMsg("ignore protection clause", state); }
@after { popMsg(state); }
        : KW_IGNORE KW_PROTECTION
        -> ^(TOK_IGNOREPROTECTION)
        ;

/* CREATE [REMOTE] (DATABASE|SCHEMA) [IF NOT EXISTS] database_name
  *   [COMMENT database_comment]
  *   [LOCATION hdfs_path]
  *   [MANAGEDLOCATION hdfs_path]
  *   [WITH DBPROPERTIES (property_name=property_value, ...)];
  * AST.16  TOK_CREATEDATABASE
  */
createDatabaseStatement
@init { pushMsg("create database statement", state); }
@after { popMsg(state); }
    : KW_CREATE (KW_DATABASE|KW_SCHEMA)
        ifNotExists?
        name=identifier
        databaseComment?
        dbLocation?
        (KW_WITH KW_DBPROPERTIES dbprops=dbProperties)?
    -> ^(TOK_CREATEDATABASE $name ifNotExists? dbLocation? databaseComment? $dbprops?)
    ;


//AST.17 subnode of AST.16  TOK_DATABASELOCATION
dbLocation
@init { pushMsg("database location specification", state); }
@after { popMsg(state); }
    :
      KW_LOCATION locn=StringLiteral -> ^(TOK_DATABASELOCATION $locn)
    ;

//AST.18 subnode of AST.16  TOK_DATABASEPROPERTIES
dbProperties
@init { pushMsg("dbproperties", state); }
@after { popMsg(state); }
    :
      LPAREN dbPropertiesList RPAREN -> ^(TOK_DATABASEPROPERTIES dbPropertiesList)
    ;


//AST.19 subnode of AST.16  TOK_DATABASEPROPERTIES
dbPropertiesList
@init { pushMsg("database properties list", state); }
@after { popMsg(state); }
    :
      keyValueProperty (COMMA keyValueProperty)* -> ^(TOK_DBPROPLIST keyValueProperty+)
    ;

//AST.20 TOK_SWITCHDATABASE use db;
switchDatabaseStatement
@init { pushMsg("switch database statement", state); }
@after { popMsg(state); }
    : KW_USE identifier
    -> ^(TOK_SWITCHDATABASE identifier)
    ;

//AST.21 TOK_DROPDATABASE  drop database
dropDatabaseStatement
@init { pushMsg("drop database statement", state); }
@after { popMsg(state); }
    : KW_DROP (KW_DATABASE|KW_SCHEMA) ifExists? identifier restrictOrCascade?
    -> ^(TOK_DROPDATABASE identifier ifExists? restrictOrCascade?)
    ;


/*
*CREATE TABLE page_view(viewTime INT, userid BIGINT,
*     page_url STRING, referrer_url STRING,
*     ip STRING COMMENT 'IP Address of the User')
* COMMENT 'This is the page view table'
* PARTITIONED BY(dt STRING, country STRING)
* ROW FORMAT DELIMITED
*  FIELDS TERMINATED BY '\001'
*STORED AS SEQUENCEFILE;
*/
//AST.22 TOK_DATABASECOMMENT   subnode of AST.16
databaseComment
@init { pushMsg("database's comment", state); }
@after { popMsg(state); }
    : KW_COMMENT comment=StringLiteral
    -> ^(TOK_DATABASECOMMENT $comment)
    ;

//AST.23 TOK_CREATETABLE
createTableStatement
@init { pushMsg("create table statement", state); }
@after { popMsg(state); }
    : KW_CREATE (temp=KW_TEMPORARY)? (ext=KW_EXTERNAL)? KW_TABLE ifNotExists? name=tableName
      (  like=KW_LIKE likeName=tableName
         tableRowFormat?
         tableFileFormat?
         tableLocation?
         tablePropertiesPrefixed?
       | (LPAREN columnNameTypeList RPAREN)?
         tableComment?
         tablePartition?
         tableBuckets?
         tableSkewed?
         tableRowFormat?
         tableFileFormat?
         tableLocation?
         tablePropertiesPrefixed?
         (KW_AS selectStatementWithCTE)?
      )
    -> ^(TOK_CREATETABLE $name $temp? $ext? ifNotExists?
         ^(TOK_LIKETABLE $likeName?)
         columnNameTypeList?
         tableComment?
         tablePartition?
         tableBuckets?
         tableSkewed?
         tableRowFormat?
         tableFileFormat?
         tableLocation?
         tablePropertiesPrefixed?
         selectStatementWithCTE?
        )
    ;

//AST.24 TOK_TRUNCATETABLE TRUNCATE [TABLE] table_name [PARTITION partition_spec];
//partition_spec:
// : (partition_column = partition_col_value, partition_column = partition_col_value, ...)
truncateTableStatement
@init { pushMsg("truncate table statement", state); }
@after { popMsg(state); }
    : KW_TRUNCATE KW_TABLE tablePartitionPrefix (KW_COLUMNS LPAREN columnNameList RPAREN)? -> ^(TOK_TRUNCATETABLE tablePartitionPrefix columnNameList?);


/*
*CREATE INDEX index_name
*   ON TABLE base_table_name (col_name, ...)
*   AS index_type
*   [WITH DEFERRED REBUILD]
*   [IDXPROPERTIES (property_name=property_value, ...)]
*   [IN TABLE index_table_name]
*   [
*      [ ROW FORMAT ...] STORED AS ...
*      | STORED BY ...
*   ]
*   [LOCATION hdfs_path]
*   [TBLPROPERTIES (...)]
*   [COMMENT "index comment"];
*/
//AST.25 TOK_CREATEINDEX
createIndexStatement
@init { pushMsg("create index statement", state);}
@after {popMsg(state);}
    : KW_CREATE KW_INDEX indexName=identifier
      KW_ON KW_TABLE tab=tableName LPAREN indexedCols=columnNameList RPAREN
      KW_AS typeName=StringLiteral
      autoRebuild?
      indexPropertiesPrefixed?
      indexTblName?
      tableRowFormat?
      tableFileFormat?
      tableLocation?
      tablePropertiesPrefixed?
      indexComment?
    ->^(TOK_CREATEINDEX $indexName $typeName $tab $indexedCols
        autoRebuild?
        indexPropertiesPrefixed?
        indexTblName?
        tableRowFormat?
        tableFileFormat?
        tableLocation?
        tablePropertiesPrefixed?
        indexComment?)
    ;

//AST.26 TOK_INDEXCOMMENT  subnode of AST.25
indexComment
@init { pushMsg("comment on an index", state);}
@after {popMsg(state);}
        :
                KW_COMMENT comment=StringLiteral  -> ^(TOK_INDEXCOMMENT $comment)
        ;

//AST.27 TOK_DEFERRED_REBUILDINDEX 用法 WITH DEFERRED REBUILD ;   subnode of AST.25
autoRebuild
@init { pushMsg("auto rebuild index", state);}
@after {popMsg(state);}
    : KW_WITH KW_DEFERRED KW_REBUILD
    ->^(TOK_DEFERRED_REBUILDINDEX)
    ;

//AST.28 TOK_INDEXCOMMENT
indexTblName
@init { pushMsg("index table name", state);}
@after {popMsg(state);}
    : KW_IN KW_TABLE indexTbl=tableName
    ->^(TOK_CREATEINDEX_INDEXTBLNAME $indexTbl)
    ;

//AST.29 subnode of AST.25
indexPropertiesPrefixed
@init { pushMsg("table properties with prefix", state); }
@after { popMsg(state); }
    :
        KW_IDXPROPERTIES! indexProperties
    ;

//AST.30 TOK_INDEXPROPERTIES subnode of AST.25
indexProperties
@init { pushMsg("index properties", state); }
@after { popMsg(state); }
    :
      LPAREN indexPropertiesList RPAREN -> ^(TOK_INDEXPROPERTIES indexPropertiesList)
    ;

//AST.31 TOK_INDEXPROPLIST subnode of AST.30
indexPropertiesList
@init { pushMsg("index properties list", state); }
@after { popMsg(state); }
    :
      keyValueProperty (COMMA keyValueProperty)* -> ^(TOK_INDEXPROPLIST keyValueProperty+)
    ;

//AST.32 TOK_DROPINDEX  subnode of AST.8 是否是这样的？
dropIndexStatement
@init { pushMsg("drop index statement", state);}
@after {popMsg(state);}
    : KW_DROP KW_INDEX ifExists? indexName=identifier KW_ON tab=tableName
    ->^(TOK_DROPINDEX $indexName $tab ifExists?)
    ;
//AST.33 TOK_DROPTABLE subnode of AST.8 是否是这样的？
dropTableStatement
@init { pushMsg("drop statement", state); }
@after { popMsg(state); }
    : KW_DROP KW_TABLE ifExists? tableName KW_PURGE? replicationClause?
    -> ^(TOK_DROPTABLE tableName ifExists? KW_PURGE? replicationClause?)
    ;

//AST.34 TOK_ALTERTABLE subnode of AST.8 是否是这样的？
//AST.35 TOK_ALTERVIEW subnode of AST.8 是否是这样的？
//AST.36 alterIndexStatementSuffix
//AST.37 alterDatabaseStatementSuffix
alterStatement
@init { pushMsg("alter statement", state); }
@after { popMsg(state); }
    : KW_ALTER KW_TABLE tableName alterTableStatementSuffix -> ^(TOK_ALTERTABLE tableName alterTableStatementSuffix)
    | KW_ALTER KW_VIEW tableName KW_AS? alterViewStatementSuffix -> ^(TOK_ALTERVIEW tableName alterViewStatementSuffix)
    | KW_ALTER KW_INDEX alterIndexStatementSuffix -> alterIndexStatementSuffix
    | KW_ALTER (KW_DATABASE|KW_SCHEMA) alterDatabaseStatementSuffix -> alterDatabaseStatementSuffix
    ;

//AST.38 alterDatabaseStatementSuffix
alterTableStatementSuffix
@init { pushMsg("alter table statement", state); }
@after { popMsg(state); }
    : (alterStatementSuffixRename[true]) => alterStatementSuffixRename[true]
    | alterStatementSuffixDropPartitions[true]
    | alterStatementSuffixAddPartitions[true]
    | alterStatementSuffixTouch
    | alterStatementSuffixArchive
    | alterStatementSuffixUnArchive
    | alterStatementSuffixProperties
    | alterStatementSuffixSkewedby
    | alterStatementSuffixExchangePartition
    | alterStatementPartitionKeyType
    | partitionSpec? alterTblPartitionStatementSuffix -> alterTblPartitionStatementSuffix partitionSpec?
    ;

//AST.39 alterTblPartitionStatementSuffix
//todo 验证这种没有树操作符的语法的根节点是否是自己,并不是，只有树操作符才会构建树
alterTblPartitionStatementSuffix
@init {pushMsg("alter table partition statement suffix", state);}
@after {popMsg(state);}
  : alterStatementSuffixFileFormat
  | alterStatementSuffixLocation
  | alterStatementSuffixProtectMode
  | alterStatementSuffixMergeFiles
  | alterStatementSuffixSerdeProperties
  | alterStatementSuffixRenamePart
  | alterStatementSuffixBucketNum
  | alterTblPartitionStatementSuffixSkewedLocation
  | alterStatementSuffixClusterbySortby
  | alterStatementSuffixCompact
  | alterStatementSuffixUpdateStatsCol
  | alterStatementSuffixRenameCol
  | alterStatementSuffixAddCol
  ;

//AST.40 TOK_ALTERTABLE_PARTCOLTYPE subnode of AST.38
alterStatementPartitionKeyType
@init {msgs.push("alter partition key type"); }
@after {msgs.pop();}
	: KW_PARTITION KW_COLUMN LPAREN columnNameType RPAREN
	-> ^(TOK_ALTERTABLE_PARTCOLTYPE columnNameType)
	;

//AST.41  alterViewStatementSuffix subnode of AST.35
alterViewStatementSuffix
@init { pushMsg("alter view statement", state); }
@after { popMsg(state); }
    : alterViewSuffixProperties
    | alterStatementSuffixRename[false]
    | alterStatementSuffixAddPartitions[false]
    | alterStatementSuffixDropPartitions[false]
    | selectStatementWithCTE
    ;

//AST.42 subnode of AST.36 alter table 的片段
alterIndexStatementSuffix
@init { pushMsg("alter index statement", state); }
@after { popMsg(state); }
    : indexName=identifier KW_ON tableName partitionSpec?
    (
      KW_REBUILD
      ->^(TOK_ALTERINDEX_REBUILD tableName $indexName partitionSpec?)
    |
      KW_SET KW_IDXPROPERTIES
      indexProperties
      ->^(TOK_ALTERINDEX_PROPERTIES tableName $indexName indexProperties)
    )
    ;

//AST.43  subnode of AST.37
alterDatabaseStatementSuffix
@init { pushMsg("alter database statement", state); }
@after { popMsg(state); }
    : alterDatabaseSuffixProperties
    | alterDatabaseSuffixSetOwner
    ;


//AST.44  TOK_ALTERDATABASE_PROPERTIES subnode of AST.43
alterDatabaseSuffixProperties
@init { pushMsg("alter database properties statement", state); }
@after { popMsg(state); }
    : name=identifier KW_SET KW_DBPROPERTIES dbProperties
    -> ^(TOK_ALTERDATABASE_PROPERTIES $name dbProperties)
    ;

//AST.45 TOK_ALTERDATABASE_OWNER subnode of AST.43
alterDatabaseSuffixSetOwner
@init { pushMsg("alter database set owner", state); }
@after { popMsg(state); }
    : dbName=identifier KW_SET KW_OWNER principalName
    -> ^(TOK_ALTERDATABASE_OWNER $dbName principalName)
    ;


//{ table }?: 这是一个条件检查，如果参数 table 为真则选用第一个构造树（TOK_ALTERTABLE_RENAME tableName），否则选用第二个构造树（TOK_ALTERVIEW_RENAME tableName）
//AST.46 AST.47 TOK_ALTERTABLE_RENAME subnode of AST.41
alterStatementSuffixRename[boolean table]
@init { pushMsg("rename statement", state); }
@after { popMsg(state); }
    : KW_RENAME KW_TO tableName
    -> { table }? ^(TOK_ALTERTABLE_RENAME tableName)
    ->            ^(TOK_ALTERVIEW_RENAME tableName)
    ;

//AST.48  AST.49 TOK_ALTERTABLE_ADDCOLS subnode of AST.39
alterStatementSuffixAddCol
@init { pushMsg("add column statement", state); }
@after { popMsg(state); }
    : (add=KW_ADD | replace=KW_REPLACE) KW_COLUMNS LPAREN columnNameTypeList RPAREN restrictOrCascade?
    -> {$add != null}? ^(TOK_ALTERTABLE_ADDCOLS columnNameTypeList restrictOrCascade?)
    ->                 ^(TOK_ALTERTABLE_REPLACECOLS columnNameTypeList restrictOrCascade?)
    ;


//AST.50 TOK_ALTERTABLE_RENAMECOL subnode of AST.39
alterStatementSuffixRenameCol
@init { pushMsg("rename column name", state); }
@after { popMsg(state); }
    : KW_CHANGE KW_COLUMN? oldName=identifier newName=identifier colType (KW_COMMENT comment=StringLiteral)? alterStatementChangeColPosition? restrictOrCascade?
    ->^(TOK_ALTERTABLE_RENAMECOL $oldName $newName colType $comment? alterStatementChangeColPosition? restrictOrCascade?)
    ;

//AST.51 TOK_ALTERTABLE_UPDATECOLSTATS subnode of AST.39
alterStatementSuffixUpdateStatsCol
@init { pushMsg("update column statistics", state); }
@after { popMsg(state); }
    : KW_UPDATE KW_STATISTICS KW_FOR KW_COLUMN? colName=identifier KW_SET tableProperties (KW_COMMENT comment=StringLiteral)?
    ->^(TOK_ALTERTABLE_UPDATECOLSTATS $colName tableProperties $comment?)
    ;

//AST.52 AST.53 TOK_ALTERTABLE_CHANGECOL_AFTER_POSITION subnode of AST.50
alterStatementChangeColPosition
    : first=KW_FIRST|KW_AFTER afterCol=identifier
    ->{$first != null}? ^(TOK_ALTERTABLE_CHANGECOL_AFTER_POSITION )
    -> ^(TOK_ALTERTABLE_CHANGECOL_AFTER_POSITION $afterCol)
    ;

//AST.54 AST.55 TOK_ALTERTABLE_ADDPARTS subnode of AST.30 or 41
alterStatementSuffixAddPartitions[boolean table]
@init { pushMsg("add partition statement", state); }
@after { popMsg(state); }
    : KW_ADD ifNotExists? alterStatementSuffixAddPartitionsElement+
    -> { table }? ^(TOK_ALTERTABLE_ADDPARTS ifNotExists? alterStatementSuffixAddPartitionsElement+)
    ->            ^(TOK_ALTERVIEW_ADDPARTS ifNotExists? alterStatementSuffixAddPartitionsElement+)
    ;

//AST.56 TOK_ALTERTABLE_ADDPARTS subnode of AST.54
alterStatementSuffixAddPartitionsElement
    : partitionSpec partitionLocation?
    ;

//AST.57 TOK_ALTERTABLE_TOUCH subnode of AST.38
alterStatementSuffixTouch
@init { pushMsg("touch statement", state); }
@after { popMsg(state); }
    : KW_TOUCH (partitionSpec)*
    -> ^(TOK_ALTERTABLE_TOUCH (partitionSpec)*)
    ;

//AST.58 TOK_ALTERTABLE_ARCHIVE subnode of AST.38
alterStatementSuffixArchive
@init { pushMsg("archive statement", state); }
@after { popMsg(state); }
    : KW_ARCHIVE (partitionSpec)*
    -> ^(TOK_ALTERTABLE_ARCHIVE (partitionSpec)*)
    ;

//AST.59 TOK_ALTERTABLE_UNARCHIVE subnode of AST.38
alterStatementSuffixUnArchive
@init { pushMsg("unarchive statement", state); }
@after { popMsg(state); }
    : KW_UNARCHIVE (partitionSpec)*
    -> ^(TOK_ALTERTABLE_UNARCHIVE (partitionSpec)*)
    ;

//AST.60 TOK_PARTITIONLOCATION subnode of AST.56
partitionLocation
@init { pushMsg("partition location", state); }
@after { popMsg(state); }
    :
      KW_LOCATION locn=StringLiteral -> ^(TOK_PARTITIONLOCATION $locn)
    ;

//AST.61,62 TOK_ALTERTABLE_DROPPARTS subnode of AST.38
alterStatementSuffixDropPartitions[boolean table]
@init { pushMsg("drop partition statement", state); }
@after { popMsg(state); }
    : KW_DROP ifExists? dropPartitionSpec (COMMA dropPartitionSpec)* ignoreProtection? KW_PURGE? replicationClause?
    -> { table }? ^(TOK_ALTERTABLE_DROPPARTS dropPartitionSpec+ ifExists? ignoreProtection? KW_PURGE? replicationClause?)
    ->            ^(TOK_ALTERVIEW_DROPPARTS dropPartitionSpec+ ifExists? ignoreProtection? replicationClause?)
    ;

//AST.63,64 TOK_ALTERTABLE_PROPERTIES subnode of AST.38
alterStatementSuffixProperties
@init { pushMsg("alter properties statement", state); }
@after { popMsg(state); }
    : KW_SET KW_TBLPROPERTIES tableProperties
    -> ^(TOK_ALTERTABLE_PROPERTIES tableProperties)
    | KW_UNSET KW_TBLPROPERTIES ifExists? tableProperties
    -> ^(TOK_ALTERTABLE_DROPPROPERTIES tableProperties ifExists?)
    ;

//AST.65,66 TOK_ALTERVIEW_PROPERTIES subnode of AST.41
alterViewSuffixProperties
@init { pushMsg("alter view properties statement", state); }
@after { popMsg(state); }
    : KW_SET KW_TBLPROPERTIES tableProperties
    -> ^(TOK_ALTERVIEW_PROPERTIES tableProperties)
    | KW_UNSET KW_TBLPROPERTIES ifExists? tableProperties
    -> ^(TOK_ALTERVIEW_DROPPROPERTIES tableProperties ifExists?)
    ;

//AST.68,68 TOK_ALTERTABLE_SERIALIZER subnode of AST.39
alterStatementSuffixSerdeProperties
@init { pushMsg("alter serdes statement", state); }
@after { popMsg(state); }
    : KW_SET KW_SERDE serdeName=StringLiteral (KW_WITH KW_SERDEPROPERTIES tableProperties)?
    -> ^(TOK_ALTERTABLE_SERIALIZER $serdeName tableProperties?)
    | KW_SET KW_SERDEPROPERTIES tableProperties
    -> ^(TOK_ALTERTABLE_SERDEPROPERTIES tableProperties)
    ;

//AST.69 TOK_TABLE_PARTITION subnode of AST.24
tablePartitionPrefix
@init {pushMsg("table partition prefix", state);}
@after {popMsg(state);}
  : tableName partitionSpec?
  ->^(TOK_TABLE_PARTITION tableName partitionSpec?)
  ;

//AST.70 TOK_ALTERTABLE_FILEFORMAT subnode of AST.39
alterStatementSuffixFileFormat
@init {pushMsg("alter fileformat statement", state); }
@after {popMsg(state);}
	: KW_SET KW_FILEFORMAT fileFormat
	-> ^(TOK_ALTERTABLE_FILEFORMAT fileFormat)
	;

//AST.71,72,73 TOK_ALTERTABLE_CLUSTER_SORT subnode of AST.39
alterStatementSuffixClusterbySortby
@init {pushMsg("alter partition cluster by sort by statement", state);}
@after {popMsg(state);}
  : KW_NOT KW_CLUSTERED -> ^(TOK_ALTERTABLE_CLUSTER_SORT TOK_NOT_CLUSTERED)
  | KW_NOT KW_SORTED -> ^(TOK_ALTERTABLE_CLUSTER_SORT TOK_NOT_SORTED)
  | tableBuckets -> ^(TOK_ALTERTABLE_CLUSTER_SORT tableBuckets)
  ;

//AST.74 TOK_ALTERTABLE_SKEWED_LOCATION subnode of AST.39
alterTblPartitionStatementSuffixSkewedLocation
@init {pushMsg("alter partition skewed location", state);}
@after {popMsg(state);}
  : KW_SET KW_SKEWED KW_LOCATION skewedLocations
  -> ^(TOK_ALTERTABLE_SKEWED_LOCATION skewedLocations)
  ;

//AST.75 TOK_SKEWED_LOCATIONS subnode of AST.74
skewedLocations
@init { pushMsg("skewed locations", state); }
@after { popMsg(state); }
    :
      LPAREN skewedLocationsList RPAREN -> ^(TOK_SKEWED_LOCATIONS skewedLocationsList)
    ;

//AST.76 TOK_SKEWED_LOCATION_LIST subnode of AST.75
skewedLocationsList
@init { pushMsg("skewed locations list", state); }
@after { popMsg(state); }
    :
      skewedLocationMap (COMMA skewedLocationMap)* -> ^(TOK_SKEWED_LOCATION_LIST skewedLocationMap+)
    ;

//AST.77 TOK_SKEWED_LOCATION_LIST subnode of AST.76
skewedLocationMap
@init { pushMsg("specifying skewed location map", state); }
@after { popMsg(state); }
    :
      key=skewedValueLocationElement EQUAL value=StringLiteral -> ^(TOK_SKEWED_LOCATION_MAP $key $value)
    ;

//AST.78 TOK_ALTERTABLE_LOCATION subnode of AST.39
alterStatementSuffixLocation
@init {pushMsg("alter location", state);}
@after {popMsg(state);}
  : KW_SET KW_LOCATION newLoc=StringLiteral
  -> ^(TOK_ALTERTABLE_LOCATION $newLoc)
  ;

//AST.79,80,81 TOK_ALTERTABLE_SKEWED subnode of AST.38
alterStatementSuffixSkewedby
@init {pushMsg("alter skewed by statement", state);}
@after{popMsg(state);}
	: tableSkewed
	->^(TOK_ALTERTABLE_SKEWED tableSkewed)
	|
	 KW_NOT KW_SKEWED
	->^(TOK_ALTERTABLE_SKEWED)
	|
	 KW_NOT storedAsDirs
	->^(TOK_ALTERTABLE_SKEWED storedAsDirs)
	;

//AST.82 TOK_ALTERTABLE_EXCHANGEPARTITION subnode of AST.38
alterStatementSuffixExchangePartition
@init {pushMsg("alter exchange partition", state);}
@after{popMsg(state);}
    : KW_EXCHANGE partitionSpec KW_WITH KW_TABLE exchangename=tableName
    -> ^(TOK_ALTERTABLE_EXCHANGEPARTITION partitionSpec $exchangename)
    ;

//AST.83 TOK_ALTERTABLE_PROTECTMODE subnode of AST.39
alterStatementSuffixProtectMode
@init { pushMsg("alter partition protect mode statement", state); }
@after { popMsg(state); }
    : alterProtectMode
    -> ^(TOK_ALTERTABLE_PROTECTMODE alterProtectMode)
    ;

//AST.84 TOK_ALTERTABLE_RENAMEPART subnode of AST.39
alterStatementSuffixRenamePart
@init { pushMsg("alter table rename partition statement", state); }
@after { popMsg(state); }
    : KW_RENAME KW_TO partitionSpec
    ->^(TOK_ALTERTABLE_RENAMEPART partitionSpec)
    ;

//AST.85 TOK_ALTERTABLE_UPDATECOLSTATS
alterStatementSuffixStatsPart
@init { pushMsg("alter table stats partition statement", state); }
@after { popMsg(state); }
    : KW_UPDATE KW_STATISTICS KW_FOR KW_COLUMN? colName=identifier KW_SET tableProperties (KW_COMMENT comment=StringLiteral)?
    ->^(TOK_ALTERTABLE_UPDATECOLSTATS $colName tableProperties $comment?)
    ;

//AST.86 TOK_ALTERTABLE_MERGEFILES   subnode of AST.39
alterStatementSuffixMergeFiles
@init { pushMsg("", state); }
@after { popMsg(state); }
    : KW_CONCATENATE
    -> ^(TOK_ALTERTABLE_MERGEFILES)
    ;

//AST.87,88 TOK_ENABLE   subnode of AST.83
alterProtectMode
@init { pushMsg("protect mode specification enable", state); }
@after { popMsg(state); }
    : KW_ENABLE alterProtectModeMode  -> ^(TOK_ENABLE alterProtectModeMode)
    | KW_DISABLE alterProtectModeMode  -> ^(TOK_DISABLE alterProtectModeMode)
    ;

//AST.89 TOK_OFFLINE   subnode of AST.87
alterProtectModeMode
@init { pushMsg("protect mode specification enable", state); }
@after { popMsg(state); }
    : KW_OFFLINE  -> ^(TOK_OFFLINE)
    | KW_NO_DROP KW_CASCADE? -> ^(TOK_NO_DROP KW_CASCADE?)
    | KW_READONLY  -> ^(TOK_READONLY)
    ;


//AST.90 TOK_OFFLINE   subnode of AST.87
alterStatementSuffixBucketNum
@init { pushMsg("", state); }
@after { popMsg(state); }
    : KW_INTO num=Number KW_BUCKETS
    -> ^(TOK_ALTERTABLE_BUCKETS $num)
    ;

//AST.91 TOK_ALTERTABLE_COMPACT   subnode of AST.39
alterStatementSuffixCompact
@init { msgs.push("compaction request"); }
@after { msgs.pop(); }
    : KW_COMPACT compactType=StringLiteral
    -> ^(TOK_ALTERTABLE_COMPACT $compactType)
    ;

//AST.92 TOK_TABLEFILEFORMAT
fileFormat
@init { pushMsg("file format specification", state); }
@after { popMsg(state); }
    : KW_INPUTFORMAT inFmt=StringLiteral KW_OUTPUTFORMAT outFmt=StringLiteral KW_SERDE serdeCls=StringLiteral (KW_INPUTDRIVER inDriver=StringLiteral KW_OUTPUTDRIVER outDriver=StringLiteral)?
      -> ^(TOK_TABLEFILEFORMAT $inFmt $outFmt $serdeCls $inDriver? $outDriver?)
    | genericSpec=identifier -> ^(TOK_FILEFORMAT_GENERIC $genericSpec)
    ;

//AST.93 tabTypeExpr
tabTypeExpr
@init { pushMsg("specifying table types", state); }
@after { popMsg(state); }
   : identifier (DOT^
   (
   (KW_ELEM_TYPE) => KW_ELEM_TYPE
   |
   (KW_KEY_TYPE) => KW_KEY_TYPE
   |
   (KW_VALUE_TYPE) => KW_VALUE_TYPE
   | identifier
   ))* identifier?
   ;

//AST.94 partTypeExpr TOK_TABTYPE
partTypeExpr
@init { pushMsg("specifying table partitions", state); }
@after { popMsg(state); }
    :  tabTypeExpr partitionSpec? -> ^(TOK_TABTYPE tabTypeExpr partitionSpec?)
    ;

//AST.95  TOK_DESCDATABASE subnode of AST.8
descStatement
@init { pushMsg("describe statement", state); }
@after { popMsg(state); }
    :
    (KW_DESCRIBE|KW_DESC)
    (
    (KW_DATABASE|KW_SCHEMA) => (KW_DATABASE|KW_SCHEMA) KW_EXTENDED? (dbName=identifier) -> ^(TOK_DESCDATABASE $dbName KW_EXTENDED?)
    |
    (KW_FUNCTION) => KW_FUNCTION KW_EXTENDED? (name=descFuncNames) -> ^(TOK_DESCFUNCTION $name KW_EXTENDED?)
    |
    (KW_FORMATTED|KW_EXTENDED|KW_PRETTY) => ((descOptions=KW_FORMATTED|descOptions=KW_EXTENDED|descOptions=KW_PRETTY) parttype=partTypeExpr) -> ^(TOK_DESCTABLE $parttype $descOptions)
    |
    parttype=partTypeExpr -> ^(TOK_DESCTABLE $parttype)
    )
    ;

//AST.96  TOK_DESCDATABASE subnode of AST.8
analyzeStatement
@init { pushMsg("analyze statement", state); }
@after { popMsg(state); }
    : KW_ANALYZE KW_TABLE (parttype=tableOrPartition) KW_COMPUTE KW_STATISTICS ((noscan=KW_NOSCAN) | (partialscan=KW_PARTIALSCAN)
                                                      | (KW_FOR KW_COLUMNS (statsColumnName=columnNameList)?))?
      -> ^(TOK_ANALYZE $parttype $noscan? $partialscan? KW_COLUMNS? $statsColumnName?)
    ;

//AST.97  TOK_SHOWCOLUMNS subnode of AST.8
showStatement
@init { pushMsg("show statement", state); }
@after { popMsg(state); }
    : KW_SHOW (KW_DATABASES|KW_SCHEMAS) (KW_LIKE showStmtIdentifier)? -> ^(TOK_SHOWDATABASES showStmtIdentifier?)
    | KW_SHOW KW_TABLES ((KW_FROM|KW_IN) db_name=identifier)? (KW_LIKE showStmtIdentifier|showStmtIdentifier)?  -> ^(TOK_SHOWTABLES (TOK_FROM $db_name)? showStmtIdentifier?)
    | KW_SHOW KW_COLUMNS (KW_FROM|KW_IN) tableName ((KW_FROM|KW_IN) db_name=identifier)?
    -> ^(TOK_SHOWCOLUMNS tableName $db_name?)
    | KW_SHOW KW_FUNCTIONS (KW_LIKE showFunctionIdentifier|showFunctionIdentifier)?  -> ^(TOK_SHOWFUNCTIONS KW_LIKE? showFunctionIdentifier?)
    | KW_SHOW KW_PARTITIONS tabName=tableName partitionSpec? -> ^(TOK_SHOWPARTITIONS $tabName partitionSpec?)
    | KW_SHOW KW_CREATE KW_TABLE tabName=tableName -> ^(TOK_SHOW_CREATETABLE $tabName)
    | KW_SHOW KW_TABLE KW_EXTENDED ((KW_FROM|KW_IN) db_name=identifier)? KW_LIKE showStmtIdentifier partitionSpec?
    -> ^(TOK_SHOW_TABLESTATUS showStmtIdentifier $db_name? partitionSpec?)
    | KW_SHOW KW_TBLPROPERTIES tableName (LPAREN prptyName=StringLiteral RPAREN)? -> ^(TOK_SHOW_TBLPROPERTIES tableName $prptyName?)
    | KW_SHOW KW_LOCKS
      (
      (KW_DATABASE|KW_SCHEMA) => (KW_DATABASE|KW_SCHEMA) (dbName=Identifier) (isExtended=KW_EXTENDED)? -> ^(TOK_SHOWDBLOCKS $dbName $isExtended?)
      |
      (parttype=partTypeExpr)? (isExtended=KW_EXTENDED)? -> ^(TOK_SHOWLOCKS $parttype? $isExtended?)
      )
    | KW_SHOW (showOptions=KW_FORMATTED)? (KW_INDEX|KW_INDEXES) KW_ON showStmtIdentifier ((KW_FROM|KW_IN) db_name=identifier)?
    -> ^(TOK_SHOWINDEXES showStmtIdentifier $showOptions? $db_name?)
    | KW_SHOW KW_COMPACTIONS -> ^(TOK_SHOW_COMPACTIONS)
    | KW_SHOW KW_TRANSACTIONS -> ^(TOK_SHOW_TRANSACTIONS)
    | KW_SHOW KW_CONF StringLiteral -> ^(TOK_SHOWCONF StringLiteral)
    ;

//AST.97  TOK_LOCKTABLE subnode of AST.8
lockStatement
@init { pushMsg("lock statement", state); }
@after { popMsg(state); }
    : KW_LOCK KW_TABLE tableName partitionSpec? lockMode -> ^(TOK_LOCKTABLE tableName lockMode partitionSpec?)
    ;


//AST.98  TOK_LOCKDB subnode of AST.8
lockDatabase
@init { pushMsg("lock database statement", state); }
@after { popMsg(state); }
    : KW_LOCK (KW_DATABASE|KW_SCHEMA) (dbName=Identifier) lockMode -> ^(TOK_LOCKDB $dbName lockMode)
    ;


//AST.99  lockMode subnode of AST.97
lockMode
@init { pushMsg("lock mode", state); }
@after { popMsg(state); }
    : KW_SHARED | KW_EXCLUSIVE
    ;

//AST.100  TOK_UNLOCKTABLE subnode of AST.8
unlockStatement
@init { pushMsg("unlock statement", state); }
@after { popMsg(state); }
    : KW_UNLOCK KW_TABLE tableName partitionSpec?  -> ^(TOK_UNLOCKTABLE tableName partitionSpec?)
    ;

//AST.101  TOK_UNLOCKTABLE subnode of AST.8
unlockDatabase
@init { pushMsg("unlock database statement", state); }
@after { popMsg(state); }
    : KW_UNLOCK (KW_DATABASE|KW_SCHEMA) (dbName=Identifier) -> ^(TOK_UNLOCKDB $dbName)
    ;

//AST.102  TOK_CREATEROLE subnode of AST.8
createRoleStatement
@init { pushMsg("create role", state); }
@after { popMsg(state); }
    : KW_CREATE KW_ROLE roleName=identifier
    -> ^(TOK_CREATEROLE $roleName)
    ;

//AST.103  TOK_DROPROLE subnode of AST.8
dropRoleStatement
@init {pushMsg("drop role", state);}
@after {popMsg(state);}
    : KW_DROP KW_ROLE roleName=identifier
    -> ^(TOK_DROPROLE $roleName)
    ;

//AST.104  TOK_GRANT subnode of AST.8
grantPrivileges
@init {pushMsg("grant privileges", state);}
@after {popMsg(state);}
    : KW_GRANT privList=privilegeList
      privilegeObject?
      KW_TO principalSpecification
      withGrantOption?
    -> ^(TOK_GRANT $privList principalSpecification privilegeObject? withGrantOption?)
    ;

//AST.105  TOK_REVOKE subnode of AST.8
revokePrivileges
@init {pushMsg("revoke privileges", state);}
@afer {popMsg(state);}
    : KW_REVOKE grantOptionFor? privilegeList privilegeObject? KW_FROM principalSpecification
    -> ^(TOK_REVOKE privilegeList principalSpecification privilegeObject? grantOptionFor?)
    ;

//AST.106  TOK_GRANT_ROLE subnode of AST.8
grantRole
@init {pushMsg("grant role", state);}
@after {popMsg(state);}
    : KW_GRANT KW_ROLE? identifier (COMMA identifier)* KW_TO principalSpecification withAdminOption?
    -> ^(TOK_GRANT_ROLE principalSpecification withAdminOption? identifier+)
    ;

//AST.107  TOK_REVOKE_ROLE subnode of AST.8
revokeRole
@init {pushMsg("revoke role", state);}
@after {popMsg(state);}
    : KW_REVOKE adminOptionFor? KW_ROLE? identifier (COMMA identifier)* KW_FROM principalSpecification
    -> ^(TOK_REVOKE_ROLE principalSpecification adminOptionFor? identifier+)
    ;

//AST.108  TOK_SHOW_ROLE_GRANT subnode of AST.8
showRoleGrants
@init {pushMsg("show role grants", state);}
@after {popMsg(state);}
    : KW_SHOW KW_ROLE KW_GRANT principalName
    -> ^(TOK_SHOW_ROLE_GRANT principalName)
    ;


//AST.109  TOK_SHOW_ROLES subnode of AST.8
showRoles
@init {pushMsg("show roles", state);}
@after {popMsg(state);}
    : KW_SHOW KW_ROLES
    -> ^(TOK_SHOW_ROLES)
    ;

//AST.110  TOK_SHOW_SET_ROLE subnode of AST.8
showCurrentRole
@init {pushMsg("show current role", state);}
@after {popMsg(state);}
    : KW_SHOW KW_CURRENT KW_ROLES
    -> ^(TOK_SHOW_SET_ROLE)
    ;


//AST.111  TOK_SHOW_SET_ROLE subnode of AST.8
setRole
@init {pushMsg("set role", state);}
@after {popMsg(state);}
    : KW_SET KW_ROLE
    (
    (KW_ALL) => (all=KW_ALL) -> ^(TOK_SHOW_SET_ROLE Identifier[$all.text])
    |
    identifier -> ^(TOK_SHOW_SET_ROLE identifier)
    )
    ;

//AST.112  TOK_SHOW_GRANT subnode of AST.8
showGrants
@init {pushMsg("show grants", state);}
@after {popMsg(state);}
    : KW_SHOW KW_GRANT principalName? (KW_ON privilegeIncludeColObject)?
    -> ^(TOK_SHOW_GRANT principalName? privilegeIncludeColObject?)
    ;

//AST.113  TOK_SHOW_ROLE_PRINCIPALS subnode of AST.8
showRolePrincipals
@init {pushMsg("show role principals", state);}
@after {popMsg(state);}
    : KW_SHOW KW_PRINCIPALS roleName=identifier
    -> ^(TOK_SHOW_ROLE_PRINCIPALS $roleName)
    ;


//AST.114  TOK_RESOURCE_ALL subnode of AST.112
privilegeIncludeColObject
@init {pushMsg("privilege object including columns", state);}
@after {popMsg(state);}
    : (KW_ALL) => KW_ALL -> ^(TOK_RESOURCE_ALL)
    | privObjectCols -> ^(TOK_PRIV_OBJECT_COL privObjectCols)
    ;

//AST.115  TOK_PRIV_OBJECT subnode of AST.104
privilegeObject
@init {pushMsg("privilege object", state);}
@after {popMsg(state);}
    : KW_ON privObject -> ^(TOK_PRIV_OBJECT privObject)
    ;


//AST.116  TOK_DB_TYPE subnode of AST.114
// database or table type. Type is optional, default type is table
privObject
    : (KW_DATABASE|KW_SCHEMA) identifier -> ^(TOK_DB_TYPE identifier)
    | KW_TABLE? tableName partitionSpec? -> ^(TOK_TABLE_TYPE tableName partitionSpec?)
    | KW_URI (path=StringLiteral) ->  ^(TOK_URI_TYPE $path)
    | KW_SERVER identifier -> ^(TOK_SERVER_TYPE identifier)
    ;

//AST.117  TOK_DB_TYPE subnode of AST.114
privObjectCols
    : (KW_DATABASE|KW_SCHEMA) identifier -> ^(TOK_DB_TYPE identifier)
    | KW_TABLE? tableName (LPAREN cols=columnNameList RPAREN)? partitionSpec? -> ^(TOK_TABLE_TYPE tableName $cols? partitionSpec?)
    | KW_URI (path=StringLiteral) ->  ^(TOK_URI_TYPE $path)
    | KW_SERVER identifier -> ^(TOK_SERVER_TYPE identifier)
    ;


//AST.118  TOK_PRIVILEGE_LIST subnode of AST.105
privilegeList
@init {pushMsg("grant privilege list", state);}
@after {popMsg(state);}
    : privlegeDef (COMMA privlegeDef)*
    -> ^(TOK_PRIVILEGE_LIST privlegeDef+)
    ;

//AST.119  TOK_PRIVILEGE subnode of AST.118
privlegeDef
@init {pushMsg("grant privilege", state);}
@after {popMsg(state);}
    : privilegeType (LPAREN cols=columnNameList RPAREN)?
    -> ^(TOK_PRIVILEGE privilegeType $cols?)
    ;


//AST.120  TOK_PRIV_ALL subnode of AST.119
privilegeType
@init {pushMsg("privilege type", state);}
@after {popMsg(state);}
    : KW_ALL -> ^(TOK_PRIV_ALL)
    | KW_ALTER -> ^(TOK_PRIV_ALTER_METADATA)
    | KW_UPDATE -> ^(TOK_PRIV_ALTER_DATA)
    | KW_CREATE -> ^(TOK_PRIV_CREATE)
    | KW_DROP -> ^(TOK_PRIV_DROP)
    | KW_INDEX -> ^(TOK_PRIV_INDEX)
    | KW_LOCK -> ^(TOK_PRIV_LOCK)
    | KW_SELECT -> ^(TOK_PRIV_SELECT)
    | KW_SHOW_DATABASE -> ^(TOK_PRIV_SHOW_DATABASE)
    | KW_INSERT -> ^(TOK_PRIV_INSERT)
    | KW_DELETE -> ^(TOK_PRIV_DELETE)
    ;


//AST.121  TOK_PRINCIPAL_NAME subnode of AST.104
principalSpecification
@init { pushMsg("user/group/role name list", state); }
@after { popMsg(state); }
    : principalName (COMMA principalName)* -> ^(TOK_PRINCIPAL_NAME principalName+)
    ;


//AST.122  TOK_USER subnode of AST.121
principalName
@init {pushMsg("user|group|role name", state);}
@after {popMsg(state);}
    : KW_USER principalIdentifier -> ^(TOK_USER principalIdentifier)
    | KW_GROUP principalIdentifier -> ^(TOK_GROUP principalIdentifier)
    | KW_ROLE identifier -> ^(TOK_ROLE identifier)
    ;

//AST.123  TOK_GRANT_WITH_OPTION subnode of AST.104
withGrantOption
@init {pushMsg("with grant option", state);}
@after {popMsg(state);}
    : KW_WITH KW_GRANT KW_OPTION
    -> ^(TOK_GRANT_WITH_OPTION)
    ;


//AST.124  TOK_GRANT_WITH_OPTION subnode of AST.105
grantOptionFor
@init {pushMsg("grant option for", state);}
@after {popMsg(state);}
    : KW_GRANT KW_OPTION KW_FOR
    -> ^(TOK_GRANT_OPTION_FOR)
;

//AST.125  TOK_ADMIN_OPTION_FOR subnode of AST.107
adminOptionFor
@init {pushMsg("admin option for", state);}
@after {popMsg(state);}
    : KW_ADMIN KW_OPTION KW_FOR
    -> ^(TOK_ADMIN_OPTION_FOR)
;


//AST.126  TOK_GRANT_WITH_ADMIN_OPTION subnode of AST.106
withAdminOption
@init {pushMsg("with admin option", state);}
@after {popMsg(state);}
    : KW_WITH KW_ADMIN KW_OPTION
    -> ^(TOK_GRANT_WITH_ADMIN_OPTION)
    ;


//AST.127  TOK_MSCK subnode of AST.8
metastoreCheck
@init { pushMsg("metastore check statement", state); }
@after { popMsg(state); }
    : KW_MSCK (repair=KW_REPAIR)? (KW_TABLE tableName partitionSpec? (COMMA partitionSpec)*)?
    -> ^(TOK_MSCK $repair? (tableName partitionSpec*)?)
    ;

//AST.128  TOK_RESOURCE_LIST
resourceList
@init { pushMsg("resource list", state); }
@after { popMsg(state); }
  :
  resource (COMMA resource)* -> ^(TOK_RESOURCE_LIST resource+)
  ;


//AST.129  TOK_RESOURCE_URI  subnode of AST.128
resource
@init { pushMsg("resource", state); }
@after { popMsg(state); }
  :
  resType=resourceType resPath=StringLiteral -> ^(TOK_RESOURCE_URI $resType $resPath)
  ;


//AST.130  TOK_JAR  subnode of AST.129
resourceType
@init { pushMsg("resource type", state); }
@after { popMsg(state); }
  :
  KW_JAR -> ^(TOK_JAR)
  |
  KW_FILE -> ^(TOK_FILE)
  |
  KW_ARCHIVE -> ^(TOK_ARCHIVE)
  ;


//AST.131  TOK_CREATEFUNCTION  subnode of AST.8
createFunctionStatement
@init { pushMsg("create function statement", state); }
@after { popMsg(state); }
    : KW_CREATE (temp=KW_TEMPORARY)? KW_FUNCTION functionIdentifier KW_AS StringLiteral
      (KW_USING rList=resourceList)?
    -> {$temp != null}? ^(TOK_CREATEFUNCTION functionIdentifier StringLiteral $rList? TOK_TEMPORARY)
    ->                  ^(TOK_CREATEFUNCTION functionIdentifier StringLiteral $rList?)
    ;

//AST.132  TOK_DROPFUNCTION  subnode of AST.8
dropFunctionStatement
@init { pushMsg("drop function statement", state); }
@after { popMsg(state); }
    : KW_DROP (temp=KW_TEMPORARY)? KW_FUNCTION ifExists? functionIdentifier
    -> {$temp != null}? ^(TOK_DROPFUNCTION functionIdentifier ifExists? TOK_TEMPORARY)
    ->                  ^(TOK_DROPFUNCTION functionIdentifier ifExists?)
    ;


//AST.133  TOK_RELOADFUNCTION  subnode of AST.8
reloadFunctionStatement
@init { pushMsg("reload function statement", state); }
@after { popMsg(state); }
    : KW_RELOAD KW_FUNCTION -> ^(TOK_RELOADFUNCTION);


//AST.134  TOK_CREATEMACRO  subnode of AST.8
createMacroStatement
@init { pushMsg("create macro statement", state); }
@after { popMsg(state); }
    : KW_CREATE KW_TEMPORARY KW_MACRO Identifier
      LPAREN columnNameTypeList? RPAREN expression
    -> ^(TOK_CREATEMACRO Identifier columnNameTypeList? expression)
    ;


//AST.135  TOK_DROPMACRO  subnode of AST.8
dropMacroStatement
@init { pushMsg("drop macro statement", state); }
@after { popMsg(state); }
    : KW_DROP KW_TEMPORARY KW_MACRO ifExists? Identifier
    -> ^(TOK_DROPMACRO Identifier ifExists?)
    ;


//AST.136  TOK_CREATEVIEW  subnode of AST.8
createViewStatement
@init {
    pushMsg("create view statement", state);
}
@after { popMsg(state); }
    : KW_CREATE (orReplace)? KW_VIEW (ifNotExists)? name=tableName
        (LPAREN columnNameCommentList RPAREN)? tableComment? viewPartition?
        tablePropertiesPrefixed?
        KW_AS
        selectStatementWithCTE
    -> ^(TOK_CREATEVIEW $name orReplace?
         ifNotExists?
         columnNameCommentList?
         tableComment?
         viewPartition?
         tablePropertiesPrefixed?
         selectStatementWithCTE
        )
    ;


//AST.137  TOK_VIEWPARTCOLS
viewPartition
@init { pushMsg("view partition specification", state); }
@after { popMsg(state); }
    : KW_PARTITIONED KW_ON LPAREN columnNameList RPAREN
    -> ^(TOK_VIEWPARTCOLS columnNameList)
    ;


//AST.138  TOK_DROPVIEW  subnode of AST.8
dropViewStatement
@init { pushMsg("drop view statement", state); }
@after { popMsg(state); }
    : KW_DROP KW_VIEW ifExists? viewName -> ^(TOK_DROPVIEW viewName ifExists?)
    ;


//AST.139  showFunctionIdentifier  subnode of AST.97
showFunctionIdentifier
@init { pushMsg("identifier for show function statement", state); }
@after { popMsg(state); }
    : functionIdentifier
    | StringLiteral
    ;

//AST.140  showStmtIdentifier  subnode of AST.97
showStmtIdentifier
@init { pushMsg("identifier for show statement", state); }
@after { popMsg(state); }
    : identifier
    | StringLiteral
    ;


//AST.141  TOK_TABLECOMMENT  subnode of AST.23
tableComment
@init { pushMsg("table's comment", state); }
@after { popMsg(state); }
    :
      KW_COMMENT comment=StringLiteral  -> ^(TOK_TABLECOMMENT $comment)
    ;


//AST.142 TOK_TABLEPARTCOLS  subnode of AST.23
tablePartition
@init { pushMsg("table partition specification", state); }
@after { popMsg(state); }
    : KW_PARTITIONED KW_BY LPAREN columnNameTypeList RPAREN
    -> ^(TOK_TABLEPARTCOLS columnNameTypeList)
    ;


//AST.143 TOK_ALTERTABLE_BUCKETS  subnode of AST.23
tableBuckets
@init { pushMsg("table buckets specification", state); }
@after { popMsg(state); }
    :
      KW_CLUSTERED KW_BY LPAREN bucketCols=columnNameList RPAREN (KW_SORTED KW_BY LPAREN sortCols=columnNameOrderList RPAREN)? KW_INTO num=Number KW_BUCKETS
    -> ^(TOK_ALTERTABLE_BUCKETS $bucketCols $sortCols? $num)
    ;


//AST.144 TOK_TABLESKEWED  subnode of AST.23
tableSkewed
@init { pushMsg("table skewed specification", state); }
@after { popMsg(state); }
    :
     KW_SKEWED KW_BY LPAREN skewedCols=columnNameList RPAREN KW_ON LPAREN (skewedValues=skewedValueElement) RPAREN ((storedAsDirs) => storedAsDirs)?
    -> ^(TOK_TABLESKEWED $skewedCols $skewedValues storedAsDirs?)
    ;


//AST.145 TOK_SERDE
rowFormat
@init { pushMsg("serde specification", state); }
@after { popMsg(state); }
    : rowFormatSerde -> ^(TOK_SERDE rowFormatSerde)
    | rowFormatDelimited -> ^(TOK_SERDE rowFormatDelimited)
    |   -> ^(TOK_SERDE)
    ;


//AST.146 TOK_RECORDREADER
recordReader
@init { pushMsg("record reader specification", state); }
@after { popMsg(state); }
    : KW_RECORDREADER StringLiteral -> ^(TOK_RECORDREADER StringLiteral)
    |   -> ^(TOK_RECORDREADER)
    ;


//AST.147 TOK_RECORDWRITER
recordWriter
@init { pushMsg("record writer specification", state); }
@after { popMsg(state); }
    : KW_RECORDWRITER StringLiteral -> ^(TOK_RECORDWRITER StringLiteral)
    |   -> ^(TOK_RECORDWRITER)
    ;


//AST.148 TOK_SERDENAME subnode of AST.145
rowFormatSerde
@init { pushMsg("serde format specification", state); }
@after { popMsg(state); }
    : KW_ROW KW_FORMAT KW_SERDE name=StringLiteral (KW_WITH KW_SERDEPROPERTIES serdeprops=tableProperties)?
    -> ^(TOK_SERDENAME $name $serdeprops?)
    ;


//AST.149 TOK_SERDEPROPS subnode of AST.145
rowFormatDelimited
@init { pushMsg("serde properties specification", state); }
@after { popMsg(state); }
    :
      KW_ROW KW_FORMAT KW_DELIMITED tableRowFormatFieldIdentifier? tableRowFormatCollItemsIdentifier? tableRowFormatMapKeysIdentifier? tableRowFormatLinesIdentifier? tableRowNullFormat?
    -> ^(TOK_SERDEPROPS tableRowFormatFieldIdentifier? tableRowFormatCollItemsIdentifier? tableRowFormatMapKeysIdentifier? tableRowFormatLinesIdentifier? tableRowNullFormat?)
    ;


//AST.150 TOK_TABLEROWFORMAT subnode of AST.23
tableRowFormat
@init { pushMsg("table row format specification", state); }
@after { popMsg(state); }
    :
      rowFormatDelimited
    -> ^(TOK_TABLEROWFORMAT rowFormatDelimited)
    | rowFormatSerde
    -> ^(TOK_TABLESERIALIZER rowFormatSerde)
    ;


//AST.151 tablePropertiesPrefixed subnode of AST.23
//整体结构意味着这个规则首先匹配一个关键字 KW_TBLPROPERTIES（且它不会出现在最终的语法树中），然后是 tableProperties。
tablePropertiesPrefixed
@init { pushMsg("table properties with prefix", state); }
@after { popMsg(state); }
    :
        KW_TBLPROPERTIES! tableProperties
    ;

//AST.152 TOK_TABLEPROPERTIES subnode of AST.151
tableProperties
@init { pushMsg("table properties", state); }
@after { popMsg(state); }
    :
      LPAREN tablePropertiesList RPAREN -> ^(TOK_TABLEPROPERTIES tablePropertiesList)
    ;

//AST.153 TOK_TABLEPROPLIST subnode of AST.152
tablePropertiesList
@init { pushMsg("table properties list", state); }
@after { popMsg(state); }
    :
      keyValueProperty (COMMA keyValueProperty)* -> ^(TOK_TABLEPROPLIST keyValueProperty+)
    |
      keyProperty (COMMA keyProperty)* -> ^(TOK_TABLEPROPLIST keyProperty+)
    ;


//AST.154 TOK_TABLEPROPERTY subnode of AST.154
keyValueProperty
@init { pushMsg("specifying key/value property", state); }
@after { popMsg(state); }
    :
      key=StringLiteral EQUAL value=StringLiteral -> ^(TOK_TABLEPROPERTY $key $value)
    ;

//AST.155 TOK_TABLEPROPERTY subnode of AST.153
keyProperty
@init { pushMsg("specifying key property", state); }
@after { popMsg(state); }
    :
      key=StringLiteral -> ^(TOK_TABLEPROPERTY $key TOK_NULL)
    ;

//AST.156 TOK_TABLEPROPERTY subnode of AST.149
tableRowFormatFieldIdentifier
@init { pushMsg("table row format's field separator", state); }
@after { popMsg(state); }
    :
      KW_FIELDS KW_TERMINATED KW_BY fldIdnt=StringLiteral (KW_ESCAPED KW_BY fldEscape=StringLiteral)?
    -> ^(TOK_TABLEROWFORMATFIELD $fldIdnt $fldEscape?)
    ;

//AST.157 TOK_TABLEROWFORMATCOLLITEMS subnode of AST.149
tableRowFormatCollItemsIdentifier
@init { pushMsg("table row format's column separator", state); }
@after { popMsg(state); }
    :
      KW_COLLECTION KW_ITEMS KW_TERMINATED KW_BY collIdnt=StringLiteral
    -> ^(TOK_TABLEROWFORMATCOLLITEMS $collIdnt)
    ;

//AST.158 TOK_TABLEROWFORMATMAPKEYS subnode of AST.149
tableRowFormatMapKeysIdentifier
@init { pushMsg("table row format's map key separator", state); }
@after { popMsg(state); }
    :
      KW_MAP KW_KEYS KW_TERMINATED KW_BY mapKeysIdnt=StringLiteral
    -> ^(TOK_TABLEROWFORMATMAPKEYS $mapKeysIdnt)
    ;


//AST.159 TOK_TABLEROWFORMATLINES subnode of AST.149
tableRowFormatLinesIdentifier
@init { pushMsg("table row format's line separator", state); }
@after { popMsg(state); }
    :
      KW_LINES KW_TERMINATED KW_BY linesIdnt=StringLiteral
    -> ^(TOK_TABLEROWFORMATLINES $linesIdnt)
    ;


//AST.160 TOK_TABLEROWFORMATNULL subnode of AST.149
tableRowNullFormat
@init { pushMsg("table row format's null specifier", state); }
@after { popMsg(state); }
    :
      KW_NULL KW_DEFINED KW_AS nullIdnt=StringLiteral
    -> ^(TOK_TABLEROWFORMATNULL $nullIdnt)
    ;

//AST.161 TOK_TABLEFILEFORMAT subnode of AST.23
tableFileFormat
@init { pushMsg("table file format specification", state); }
@after { popMsg(state); }
    :
      (KW_STORED KW_AS KW_INPUTFORMAT) => KW_STORED KW_AS KW_INPUTFORMAT inFmt=StringLiteral KW_OUTPUTFORMAT outFmt=StringLiteral (KW_INPUTDRIVER inDriver=StringLiteral KW_OUTPUTDRIVER outDriver=StringLiteral)?
      -> ^(TOK_TABLEFILEFORMAT $inFmt $outFmt $inDriver? $outDriver?)
      | KW_STORED KW_BY storageHandler=StringLiteral
         (KW_WITH KW_SERDEPROPERTIES serdeprops=tableProperties)?
      -> ^(TOK_STORAGEHANDLER $storageHandler $serdeprops?)
      | KW_STORED KW_AS genericSpec=identifier
      -> ^(TOK_FILEFORMAT_GENERIC $genericSpec)
    ;

//AST.162 TOK_TABLELOCATION subnode of AST.23 AST.7
tableLocation
@init { pushMsg("table location specification", state); }
@after { popMsg(state); }
    :
      KW_LOCATION locn=StringLiteral -> ^(TOK_TABLELOCATION $locn)
    ;






//AST.163 TOK_TABCOLLIST subnode of AST.23

columnNameTypeList
@init { pushMsg("column name type list", state); }
@after { popMsg(state); }
    : columnNameType (COMMA columnNameType)* -> ^(TOK_TABCOLLIST columnNameType+)
    ;


//AST.164 TOK_TABCOLLIST
columnNameColonTypeList
@init { pushMsg("column name type list", state); }
@after { popMsg(state); }
    : columnNameColonType (COMMA columnNameColonType)* -> ^(TOK_TABCOLLIST columnNameColonType+)
    ;

//AST.165 TOK_TABCOLNAME
columnNameList
@init { pushMsg("column name list", state); }
@after { popMsg(state); }
    : columnName (COMMA columnName)* -> ^(TOK_TABCOLNAME columnName+)
    ;


//AST.166 TOK_TABCOLNAME  subnode of AST.165
columnName
@init { pushMsg("column name", state); }
@after { popMsg(state); }
    :
      identifier
    ;

//AST.167 TOK_TABCOLNAME  subnode of AST.166
columnNameOrderList
@init { pushMsg("column name order list", state); }
@after { popMsg(state); }
    : columnNameOrder (COMMA columnNameOrder)* -> ^(TOK_TABCOLNAME columnNameOrder+)
    ;


//AST.168 skewedValueElement
skewedValueElement
@init { pushMsg("skewed value element", state); }
@after { popMsg(state); }
    :
      skewedColumnValues
     | skewedColumnValuePairList
    ;


//AST.169 TOK_TABCOLVALUE_PAIR
skewedColumnValuePairList
@init { pushMsg("column value pair list", state); }
@after { popMsg(state); }
    : skewedColumnValuePair (COMMA skewedColumnValuePair)* -> ^(TOK_TABCOLVALUE_PAIR skewedColumnValuePair+)
    ;

//AST.170 TOK_TABCOLVALUES
skewedColumnValuePair
@init { pushMsg("column value pair", state); }
@after { popMsg(state); }
    :
      LPAREN colValues=skewedColumnValues RPAREN
      -> ^(TOK_TABCOLVALUES $colValues)
    ;


//AST.171 TOK_TABCOLVALUE   subnode of AST.170
skewedColumnValues
@init { pushMsg("column values", state); }
@after { popMsg(state); }
    : skewedColumnValue (COMMA skewedColumnValue)* -> ^(TOK_TABCOLVALUE skewedColumnValue+)
    ;

//AST.172 skewedColumnValue   subnode of AST.171
skewedColumnValue
@init { pushMsg("column value", state); }
@after { popMsg(state); }
    :
      constant
    ;

//AST.173 skewedValueLocationElement   subnode of AST.172
skewedValueLocationElement
@init { pushMsg("skewed value location element", state); }
@after { popMsg(state); }
    :
      skewedColumnValue
     | skewedColumnValuePair
    ;

//AST.174 TOK_TABSORTCOLNAMEASC   subnode of AST.167
columnNameOrder
@init { pushMsg("column name order", state); }
@after { popMsg(state); }
    : identifier (asc=KW_ASC | desc=KW_DESC)?
    -> {$desc == null}? ^(TOK_TABSORTCOLNAMEASC identifier)
    ->                  ^(TOK_TABSORTCOLNAMEDESC identifier)
    ;

//AST.175 TOK_TABCOLNAME   subnode of AST.136
columnNameCommentList
@init { pushMsg("column name comment list", state); }
@after { popMsg(state); }
    : columnNameComment (COMMA columnNameComment)* -> ^(TOK_TABCOLNAME columnNameComment+)
    ;

//AST.176 TOK_TABCOL   subnode of AST.175
columnNameComment
@init { pushMsg("column name comment", state); }
@after { popMsg(state); }
    : colName=identifier (KW_COMMENT comment=StringLiteral)?
    -> ^(TOK_TABCOL $colName TOK_NULL $comment?)
    ;

//AST.177 TOK_TABSORTCOLNAMEASC
columnRefOrder
@init { pushMsg("column order", state); }
@after { popMsg(state); }
    : expression (asc=KW_ASC | desc=KW_DESC)?
    -> {$desc == null}? ^(TOK_TABSORTCOLNAMEASC expression)
    ->                  ^(TOK_TABSORTCOLNAMEDESC expression)
    ;

//AST.178 TOK_TABCOL   subnode of AST.40
columnNameType
@init { pushMsg("column specification", state); }
@after { popMsg(state); }
    : colName=identifier colType (KW_COMMENT comment=StringLiteral)?
    -> {containExcludedCharForCreateTableColumnName($colName.text)}? {throwColumnNameException()}
    -> {$comment == null}? ^(TOK_TABCOL $colName colType)
    ->                     ^(TOK_TABCOL $colName colType $comment)
    ;

//AST.179 TOK_TABCOL   subnode of AST.164
columnNameColonType
@init { pushMsg("column specification", state); }
@after { popMsg(state); }
    : colName=identifier COLON colType (KW_COMMENT comment=StringLiteral)?
    -> {$comment == null}? ^(TOK_TABCOL $colName colType)
    ->                     ^(TOK_TABCOL $colName colType $comment)
    ;

//AST.180 colType   subnode of AST.179
colType
@init { pushMsg("column type", state); }
@after { popMsg(state); }
    : type
    ;

//AST.181 TOK_COLTYPELIST   subnode of AST.174
colTypeList
@init { pushMsg("column type list", state); }
@after { popMsg(state); }
    : colType (COMMA colType)* -> ^(TOK_COLTYPELIST colType+)
    ;

//AST.182 type
type
    : primitiveType
    | listType
    | structType
    | mapType
    | unionType;

//AST.183 TOK_TINYINT   subnode of AST.182 174
primitiveType
@init { pushMsg("primitive type specification", state); }
@after { popMsg(state); }
    : KW_TINYINT       ->    TOK_TINYINT
    | KW_SMALLINT      ->    TOK_SMALLINT
    | KW_INT           ->    TOK_INT
    | KW_BIGINT        ->    TOK_BIGINT
    | KW_BOOLEAN       ->    TOK_BOOLEAN
    | KW_FLOAT         ->    TOK_FLOAT
    | KW_DOUBLE        ->    TOK_DOUBLE
    | KW_DATE          ->    TOK_DATE
    | KW_DATETIME      ->    TOK_DATETIME
    | KW_TIMESTAMP     ->    TOK_TIMESTAMP
    // Uncomment to allow intervals as table column types
    //| KW_INTERVAL KW_YEAR KW_TO KW_MONTH -> TOK_INTERVAL_YEAR_MONTH
    //| KW_INTERVAL KW_DAY KW_TO KW_SECOND -> TOK_INTERVAL_DAY_TIME
    | KW_STRING        ->    TOK_STRING
    | KW_BINARY        ->    TOK_BINARY
    | KW_DECIMAL (LPAREN prec=Number (COMMA scale=Number)? RPAREN)? -> ^(TOK_DECIMAL $prec? $scale?)
    | KW_VARCHAR LPAREN length=Number RPAREN      ->    ^(TOK_VARCHAR $length)
    | KW_CHAR LPAREN length=Number RPAREN      ->    ^(TOK_CHAR $length)
    ;

//AST.184 TOK_LIST  subnode of AST.182
listType
@init { pushMsg("list type", state); }
@after { popMsg(state); }
    : KW_ARRAY LESSTHAN type GREATERTHAN   -> ^(TOK_LIST type)
    ;

//AST.185 TOK_STRUCT   subnode of AST.182
structType
@init { pushMsg("struct type", state); }
@after { popMsg(state); }
    : KW_STRUCT LESSTHAN columnNameColonTypeList GREATERTHAN -> ^(TOK_STRUCT columnNameColonTypeList)
    ;

//AST.186 TOK_MAP   subnode of AST.182
mapType
@init { pushMsg("map type", state); }
@after { popMsg(state); }
    : KW_MAP LESSTHAN left=primitiveType COMMA right=type GREATERTHAN
    -> ^(TOK_MAP $left $right)
    ;

//AST.187 TOK_UNIONTYPE   subnode of AST.182
unionType
@init { pushMsg("uniontype type", state); }
@after { popMsg(state); }
    : KW_UNIONTYPE LESSTHAN colTypeList GREATERTHAN -> ^(TOK_UNIONTYPE colTypeList)
    ;

//AST.188 TOK_UNIONALL
setOperator
@init { pushMsg("set operator", state); }
@after { popMsg(state); }
    : KW_UNION KW_ALL -> ^(TOK_UNIONALL)
    | KW_UNION KW_DISTINCT? -> ^(TOK_UNIONDISTINCT)
    ;

//AST.189 queryStatementExpressionBody
queryStatementExpression[boolean topLevel]
    :
    /* Would be nice to do this as a gated semantic perdicate
       But the predicate gets pushed as a lookahead decision.
       Calling rule doesnot know about topLevel
    */
    (w=withClause {topLevel}?)?
    queryStatementExpressionBody[topLevel] {
      if ($w.tree != null) {
      adaptor.addChild($queryStatementExpressionBody.tree, $w.tree);
      }
    }
    ->  queryStatementExpressionBody
    ;

//AST.190 queryStatementExpressionBody   subnode of AST.189
queryStatementExpressionBody[boolean topLevel]
    :
    fromStatement[topLevel]
    | regularBody[topLevel]
    ;

//AST.191 TOK_CTE
withClause
  :
  KW_WITH cteStatement (COMMA cteStatement)* -> ^(TOK_CTE cteStatement+)
;

//AST.192 TOK_SUBQUERY   subnode of AST.191
cteStatement
   :
   identifier KW_AS LPAREN queryStatementExpression[false] RPAREN
   -> ^(TOK_SUBQUERY queryStatementExpression identifier)
;

//AST.193 TOK_FROM   subnode of AST.190
//todo 这里结构略复杂,还没清楚
fromStatement[boolean topLevel]
: (singleFromStatement  -> singleFromStatement)
	(u=setOperator r=singleFromStatement
	  -> ^($u {$fromStatement.tree} $r)
	)*
	 -> {u != null && topLevel}? ^(TOK_QUERY
	       ^(TOK_FROM
	         ^(TOK_SUBQUERY
	           {$fromStatement.tree}
	            {adaptor.create(Identifier, generateUnionAlias())}
	           )
	        )
	       ^(TOK_INSERT
	          ^(TOK_DESTINATION ^(TOK_DIR TOK_TMP_FILE))
	          ^(TOK_SELECT ^(TOK_SELEXPR TOK_ALLCOLREF))
	        )
	      )
    -> {$fromStatement.tree}
	;


//AST.194 TOK_QUERY   subnode of AST.193
singleFromStatement
    :
    fromClause
    ( b+=body )+ -> ^(TOK_QUERY fromClause body+)
    ;


//AST.195 TOK_QUERY   subnode of AST.190
/*
The valuesClause rule below ensures that the parse tree for
"insert into table FOO values (1,2),(3,4)" looks the same as
"insert into table FOO select a,b from (values(1,2),(3,4)) as BAR(a,b)" which itself is made to look
very similar to the tree for "insert into table FOO select a,b from BAR".  Since virtual table name
is implicit, it's represented as TOK_ANONYMOUS.
*/
regularBody[boolean topLevel]
   :
   i=insertClause
   (
   s=selectStatement[topLevel]
     {$s.tree.getChild(1) !=null}? {$s.tree.getChild(1).replaceChildren(0, 0, $i.tree);} -> {$s.tree}
     |
     valuesClause
      -> ^(TOK_QUERY
            ^(TOK_FROM
              ^(TOK_VIRTUAL_TABLE ^(TOK_VIRTUAL_TABREF ^(TOK_ANONYMOUS)) valuesClause)
             )
            ^(TOK_INSERT {$i.tree} ^(TOK_SELECT ^(TOK_SELEXPR TOK_ALLCOLREF)))
          )
   )
   |
   selectStatement[topLevel]
   ;


//AST.196 $selectStatement   subnode of AST.195
//s=selectClause 这是捕获结果的写法，s为 selectClause的结果，后面的同上
//(set=setOpSelectStatement[$selectStatement.tree, topLevel])?: 定义 set 为 setOpSelectStatement 的结果，该操作是可选的。
//setOpSelectStatement 通常用来处理 SQL 的联合查询（如 UNION、INTERSECT 等）。[] 是解析的时候传入的参数
//->{$selectStatement.tree}: 条件判断 {set == null} 是否为真，如果为真则执行 {$selectStatement.tree}， 返回当前的查询树。
//如果前面条件不为真，则再次检查 {o==null && c==null && d==null && sort==null && l==null} 是否为真，如果为真则执行 {$set.tree}， 返回 setOpSelectStatement 生成的树。
//->{throwSetOpException()}: 如果上述条件都不满足，则调用 throwSetOpException() 方法抛出一个异常。
selectStatement[boolean topLevel]
   :
   (
   s=selectClause
   f=fromClause?
   w=whereClause?
   g=groupByClause?
   h=havingClause?
   o=orderByClause?
   c=clusterByClause?
   d=distributeByClause?
   sort=sortByClause?
   win=window_clause?
   l=limitClause?
   -> ^(TOK_QUERY $f? ^(TOK_INSERT ^(TOK_DESTINATION ^(TOK_DIR TOK_TMP_FILE))
                     $s $w? $g? $h? $o? $c?
                     $d? $sort? $win? $l?))
   )
   (set=setOpSelectStatement[$selectStatement.tree, topLevel])?
   -> {set == null}?
      {$selectStatement.tree}
   -> {o==null && c==null && d==null && sort==null && l==null}?
      {$set.tree}
   -> {throwSetOpException()}
   ;

//AST.197 TOK_QUERY   subnode of AST.196
setOpSelectStatement[CommonTree t, boolean topLevel]
   :
   (u=setOperator b=simpleSelectStatement
   -> {$setOpSelectStatement.tree != null && u.tree.getType()==HiveParser.TOK_UNIONDISTINCT}?
      ^(TOK_QUERY
          ^(TOK_FROM
            ^(TOK_SUBQUERY
              ^(TOK_UNIONALL {$setOpSelectStatement.tree} $b)
              {adaptor.create(Identifier, generateUnionAlias())}
             )
          )
          ^(TOK_INSERT
             ^(TOK_DESTINATION ^(TOK_DIR TOK_TMP_FILE))
             ^(TOK_SELECTDI ^(TOK_SELEXPR TOK_ALLCOLREF))
          )
       )
   -> {$setOpSelectStatement.tree != null && u.tree.getType()!=HiveParser.TOK_UNIONDISTINCT}?
      ^(TOK_UNIONALL {$setOpSelectStatement.tree} $b)
   -> {$setOpSelectStatement.tree == null && u.tree.getType()==HiveParser.TOK_UNIONDISTINCT}?
      ^(TOK_QUERY
          ^(TOK_FROM
            ^(TOK_SUBQUERY
              ^(TOK_UNIONALL {$t} $b)
              {adaptor.create(Identifier, generateUnionAlias())}
             )
           )
          ^(TOK_INSERT
            ^(TOK_DESTINATION ^(TOK_DIR TOK_TMP_FILE))
            ^(TOK_SELECTDI ^(TOK_SELEXPR TOK_ALLCOLREF))
         )
       )
   -> ^(TOK_UNIONALL {$t} $b)
   )+
   o=orderByClause?
   c=clusterByClause?
   d=distributeByClause?
   sort=sortByClause?
   win=window_clause?
   l=limitClause?
   -> {o==null && c==null && d==null && sort==null && win==null && l==null && !topLevel}?
      {$setOpSelectStatement.tree}
   -> ^(TOK_QUERY
          ^(TOK_FROM
            ^(TOK_SUBQUERY
              {$setOpSelectStatement.tree}
              {adaptor.create(Identifier, generateUnionAlias())}
             )
          )
          ^(TOK_INSERT
             ^(TOK_DESTINATION ^(TOK_DIR TOK_TMP_FILE))
             ^(TOK_SELECT ^(TOK_SELEXPR TOK_ALLCOLREF))
             $o? $c? $d? $sort? $win? $l?
          )
       )
   ;


//AST.198 TOK_QUERY   subnode of AST.197
//select from(可选) where group_by(可选) having_by(可选) window_clause(可选)
// ((a)=>b)?：表示这个预测路径是可选的，即这个路径可以被采取，也可以不被采取.
// 箭头 (=>)：在 ANTLR 中，箭头用于引导解析器采取某种特定的动作或者进入特定的路径。特别地，=> 被称为“预测（predicated）解析”，它用于指导解析器在某些特殊情况下如何处理输入。
//-> denotes a rewrite rule in ANTLR 3 that constructs an AST from the parser rule.
simpleSelectStatement
   :
   selectClause
   fromClause?
   whereClause?
   groupByClause?
   havingClause?
   ((window_clause) => window_clause)?
   -> ^(TOK_QUERY fromClause? ^(TOK_INSERT ^(TOK_DESTINATION ^(TOK_DIR TOK_TMP_FILE))
                     selectClause whereClause? groupByClause? havingClause? window_clause?))
   ;


//AST.199 selectStatement   subnode of AST.23,41,136
// selectStatementWithCTE 最终会构造为一个selectStatement 树，如果withClause存在就将它传给w 变量，在花括号包围的action中添加为子节点，构造一个新的树selectStatement替代selectStatementWithCTE
selectStatementWithCTE
    :
    (w=withClause)?
    selectStatement[true] {
      if ($w.tree != null) {
      adaptor.addChild($selectStatement.tree, $w.tree);
      }
    }
    ->  selectStatement
    ;


//AST.200 TOK_INSERT   subnode of AST.194
body
   :
   insertClause
   selectClause
   lateralView?
   whereClause?
   groupByClause?
   havingClause?
   orderByClause?
   clusterByClause?
   distributeByClause?
   sortByClause?
   window_clause?
   limitClause? -> ^(TOK_INSERT insertClause
                     selectClause lateralView? whereClause? groupByClause? havingClause? orderByClause? clusterByClause?
                     distributeByClause? sortByClause? window_clause? limitClause?)
   |
   selectClause
   lateralView?
   whereClause?
   groupByClause?
   havingClause?
   orderByClause?
   clusterByClause?
   distributeByClause?
   sortByClause?
   window_clause?
   limitClause? -> ^(TOK_INSERT ^(TOK_DESTINATION ^(TOK_DIR TOK_TMP_FILE))
                     selectClause lateralView? whereClause? groupByClause? havingClause? orderByClause? clusterByClause?
                     distributeByClause? sortByClause? window_clause? limitClause?)
   ;

//AST.201 TOK_INSERT_INTO   subnode of AST.200
insertClause
@init { pushMsg("insert clause", state); }
@after { popMsg(state); }
   :
     KW_INSERT KW_OVERWRITE destination ifNotExists? -> ^(TOK_DESTINATION destination ifNotExists?)
   | KW_INSERT KW_INTO KW_TABLE? tableOrPartition (LPAREN targetCols=columnNameList RPAREN)?
       -> ^(TOK_INSERT_INTO tableOrPartition $targetCols?)
   ;


//AST.202 TOK_DIR   subnode of AST.201
destination
@init { pushMsg("destination specification", state); }
@after { popMsg(state); }
   :
     (local = KW_LOCAL)? KW_DIRECTORY StringLiteral tableRowFormat? tableFileFormat?
       -> ^(TOK_DIR StringLiteral $local? tableRowFormat? tableFileFormat?)
   | KW_TABLE tableOrPartition -> tableOrPartition
   ;

//AST.203 TOK_LIMIT   subnode of AST.200
limitClause
@init { pushMsg("limit clause", state); }
@after { popMsg(state); }
   :
   KW_LIMIT num=Number -> ^(TOK_LIMIT $num)
   ;

//AST.204 TOK_LIMIT   subnode of AST.3
//DELETE FROM <tableName> WHERE ...;
deleteStatement
@init { pushMsg("delete statement", state); }
@after { popMsg(state); }
   :
   KW_DELETE KW_FROM tableName (whereClause)? -> ^(TOK_DELETE_FROM tableName whereClause?)
   ;

//AST.205 EQUAL
/*SET <columName> = (3 + col2)*/
columnAssignmentClause
   :
   tableOrColumn EQUAL^ precedencePlusExpression
   ;

//AST.206 TOK_SET_COLUMNS_CLAUSE  subnode of AST.207
/*SET col1 = 5, col2 = (4 + col4), ...*/
setColumnsClause
   :
   KW_SET columnAssignmentClause (COMMA columnAssignmentClause)* -> ^(TOK_SET_COLUMNS_CLAUSE columnAssignmentClause* )
   ;

//AST.207 TOK_UPDATE_TABLE subnode of AST.3
/*
  UPDATE <table>
  SET col1 = val1, col2 = val2... WHERE ...
*/
updateStatement
@init { pushMsg("update statement", state); }
@after { popMsg(state); }
   :
   KW_UPDATE tableName setColumnsClause whereClause? -> ^(TOK_UPDATE_TABLE tableName setColumnsClause whereClause?)
   ;
