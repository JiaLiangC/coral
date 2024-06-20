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

// This file is copied from SelectClauseParser.g of Hive 1.2.1

parser grammar SelectClauseParser;

options
{
output=AST;
ASTLabelType=CommonTree;
backtrack=false;
k=3;
}

@members {
  @Override
  public Object recoverFromMismatchedSet(IntStream input,
      RecognitionException re, BitSet follow) throws RecognitionException {
    throw re;
  }
  @Override
  public void displayRecognitionError(String[] tokenNames,
      RecognitionException e) {
    gParent.errors.add(new ParseError(gParent, e, tokenNames));
  }
  protected boolean useSQL11ReservedKeywordsForIdentifier() {
    return gParent.useSQL11ReservedKeywordsForIdentifier();
  }
}

@rulecatch {
catch (RecognitionException e) {
  throw e;
}
}

//----------------------- Rules for parsing selectClause -----------------------------
// select a,b,c ...
//AST.208 TOK_SELECT  subnode of AST.196,198,200
selectClause
@init { gParent.pushMsg("select clause", state); }
@after { gParent.popMsg(state); }
    :
    KW_SELECT hintClause? (((KW_ALL | dist=KW_DISTINCT)? selectList)
                          | (transform=KW_TRANSFORM selectTrfmClause))
     -> {$transform == null && $dist == null}? ^(TOK_SELECT hintClause? selectList)
     -> {$transform == null && $dist != null}? ^(TOK_SELECTDI hintClause? selectList)
     -> ^(TOK_SELECT hintClause? ^(TOK_SELEXPR selectTrfmClause) )
    |
    trfmClause  ->^(TOK_SELECT ^(TOK_SELEXPR trfmClause))
    ;

//AST.209 selectItem  subnode of AST.208
selectList
@init { gParent.pushMsg("select list", state); }
@after { gParent.popMsg(state); }
    :
    selectItem ( COMMA  selectItem )* -> selectItem+
    ;


//AST.210 TOK_TRANSFORM  subnode of AST.208
selectTrfmClause
@init { gParent.pushMsg("transform clause", state); }
@after { gParent.popMsg(state); }
    :
    LPAREN selectExpressionList RPAREN
    inSerde=rowFormat inRec=recordWriter
    KW_USING StringLiteral
    ( KW_AS ((LPAREN (aliasList | columnNameTypeList) RPAREN) | (aliasList | columnNameTypeList)))?
    outSerde=rowFormat outRec=recordReader
    -> ^(TOK_TRANSFORM selectExpressionList $inSerde $inRec StringLiteral $outSerde $outRec aliasList? columnNameTypeList?)
    ;

//AST.211 TOK_HINTLIST  subnode of AST.208
hintClause
@init { gParent.pushMsg("hint clause", state); }
@after { gParent.popMsg(state); }
    :
    DIVIDE STAR PLUS hintList STAR DIVIDE -> ^(TOK_HINTLIST hintList)
    ;


//AST.212 hintItem  subnode of AST.211
hintList
@init { gParent.pushMsg("hint list", state); }
@after { gParent.popMsg(state); }
    :
    hintItem (COMMA hintItem)* -> hintItem+
    ;

//AST.213 TOK_HINT  subnode of AST.212
hintItem
@init { gParent.pushMsg("hint item", state); }
@after { gParent.popMsg(state); }
    :
    hintName (LPAREN hintArgs RPAREN)? -> ^(TOK_HINT hintName hintArgs?)
    ;

//AST.214 TOK_MAPJOIN  subnode of AST.213
hintName
@init { gParent.pushMsg("hint name", state); }
@after { gParent.popMsg(state); }
    :
    KW_MAPJOIN -> TOK_MAPJOIN
    | KW_STREAMTABLE -> TOK_STREAMTABLE
    | KW_HOLD_DDLTIME -> TOK_HOLD_DDLTIME
    ;

//AST.215 TOK_HINTARGLIST  subnode of AST.213
hintArgs
@init { gParent.pushMsg("hint arguments", state); }
@after { gParent.popMsg(state); }
    :
    hintArgName (COMMA hintArgName)* -> ^(TOK_HINTARGLIST hintArgName+)
    ;

//AST.216 hintArgName  subnode of AST.215
hintArgName
@init { gParent.pushMsg("hint argument name", state); }
@after { gParent.popMsg(state); }
    :
    identifier
    ;


//AST.217 TOK_SELEXPR  subnode of AST.209
selectItem
@init { gParent.pushMsg("selection target", state); }
@after { gParent.popMsg(state); }
    :
    (tableAllColumns) => tableAllColumns -> ^(TOK_SELEXPR tableAllColumns)
    |
    ( expression
      ((KW_AS? identifier) | (KW_AS LPAREN identifier (COMMA identifier)* RPAREN))?
    ) -> ^(TOK_SELEXPR expression identifier*)
    ;

//AST.218 TOK_TRANSFORM  subnode of AST.208
trfmClause
@init { gParent.pushMsg("transform clause", state); }
@after { gParent.popMsg(state); }
    :
    (   KW_MAP    selectExpressionList
      | KW_REDUCE selectExpressionList )
    inSerde=rowFormat inRec=recordWriter
    KW_USING StringLiteral
    ( KW_AS ((LPAREN (aliasList | columnNameTypeList) RPAREN) | (aliasList | columnNameTypeList)))?
    outSerde=rowFormat outRec=recordReader
    -> ^(TOK_TRANSFORM selectExpressionList $inSerde $inRec StringLiteral $outSerde $outRec aliasList? columnNameTypeList?)
    ;

//AST.219 selectExpression
selectExpression
@init { gParent.pushMsg("select expression", state); }
@after { gParent.popMsg(state); }
    :
    (tableAllColumns) => tableAllColumns
    |
    expression
    ;

//AST.220 TOK_EXPLIST  subnode of AST.218
selectExpressionList
@init { gParent.pushMsg("select expression list", state); }
@after { gParent.popMsg(state); }
    :
    selectExpression (COMMA selectExpression)* -> ^(TOK_EXPLIST selectExpression+)
    ;

//---------------------- Rules for windowing clauses -------------------------------
//AST.221 KW_WINDOW
window_clause
@init { gParent.pushMsg("window_clause", state); }
@after { gParent.popMsg(state); }
:
  KW_WINDOW window_defn (COMMA window_defn)* -> ^(KW_WINDOW window_defn+)
;

//AST.222 TOK_WINDOWDEF  subnode of AST.221
window_defn
@init { gParent.pushMsg("window_defn", state); }
@after { gParent.popMsg(state); }
:
  Identifier KW_AS window_specification -> ^(TOK_WINDOWDEF Identifier window_specification)
;

//AST.223 TOK_WINDOWSPEC  subnode of AST.221
window_specification
@init { gParent.pushMsg("window_specification", state); }
@after { gParent.popMsg(state); }
:
  (Identifier | ( LPAREN Identifier? partitioningSpec? window_frame? RPAREN)) -> ^(TOK_WINDOWSPEC Identifier? partitioningSpec? window_frame?)
;

//AST.224 window_frame  subnode of AST.223
window_frame :
 window_range_expression |
 window_value_expression
;

//AST.225 TOK_WINDOWRANGE  subnode of AST.224
window_range_expression
@init { gParent.pushMsg("window_range_expression", state); }
@after { gParent.popMsg(state); }
:
 KW_ROWS sb=window_frame_start_boundary -> ^(TOK_WINDOWRANGE $sb) |
 KW_ROWS KW_BETWEEN s=window_frame_boundary KW_AND end=window_frame_boundary -> ^(TOK_WINDOWRANGE $s $end)
;

//AST.226 TOK_WINDOWVALUES  subnode of AST.224
window_value_expression
@init { gParent.pushMsg("window_value_expression", state); }
@after { gParent.popMsg(state); }
:
 KW_RANGE sb=window_frame_start_boundary -> ^(TOK_WINDOWVALUES $sb) |
 KW_RANGE KW_BETWEEN s=window_frame_boundary KW_AND end=window_frame_boundary -> ^(TOK_WINDOWVALUES $s $end)
;

//AST.227 KW_CURRENT  subnode of AST.226
window_frame_start_boundary
@init { gParent.pushMsg("windowframestartboundary", state); }
@after { gParent.popMsg(state); }
:
  KW_UNBOUNDED KW_PRECEDING  -> ^(KW_PRECEDING KW_UNBOUNDED) |
  KW_CURRENT KW_ROW  -> ^(KW_CURRENT) |
  Number KW_PRECEDING -> ^(KW_PRECEDING Number)
;

//AST.228 KW_CURRENT  subnode of AST.226
window_frame_boundary
@init { gParent.pushMsg("windowframeboundary", state); }
@after { gParent.popMsg(state); }
:
  KW_UNBOUNDED (r=KW_PRECEDING|r=KW_FOLLOWING)  -> ^($r KW_UNBOUNDED) |
  KW_CURRENT KW_ROW  -> ^(KW_CURRENT) |
  Number (d=KW_PRECEDING | d=KW_FOLLOWING ) -> ^($d Number)
;
