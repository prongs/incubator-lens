/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.lens.cube.parse;

import static org.apache.hadoop.hive.ql.parse.HiveParser.*;

import java.util.Iterator;
import java.util.List;

import org.apache.lens.cube.metadata.CubeMeasure;
import org.apache.lens.cube.parse.CandidateTablePruneCause.CandidateTablePruneCode;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import org.antlr.runtime.CommonToken;

import com.google.common.collect.Lists;

/**
 * <p> Replace select and having columns with default aggregate functions on them, if default aggregate is defined and
 * if there isn't already an aggregate function specified on the columns. </p> <p/> <p> Expressions which already
 * contain aggregate sub-expressions will not be changed. </p> <p/> <p> At this point it's assumed that aliases have
 * been added to all columns. </p>
 */
class AggregateResolver implements ContextRewriter {
  public static final Log LOG = LogFactory.getLog(AggregateResolver.class.getName());
  private final boolean whetherPushwhereToHaving;

  public AggregateResolver(Configuration conf) {
    whetherPushwhereToHaving =
      conf.getBoolean(CubeQueryConfUtil.ENABLE_WHERE_TO_HAVING,
        CubeQueryConfUtil.DEFAULT_ENABLE_WHERE_TO_HAVING);
  }

  @Override
  public void rewriteContext(CubeQueryContext cubeql) throws SemanticException {
    if (cubeql.getCube() == null) {
      return;
    }
    pushWhereToHaving(cubeql);
    boolean nonDefaultAggregates = false;
    boolean aggregateResolverDisabled = cubeql.getConf().getBoolean(CubeQueryConfUtil.DISABLE_AGGREGATE_RESOLVER,
      CubeQueryConfUtil.DEFAULT_DISABLE_AGGREGATE_RESOLVER);
    // Check if the query contains measures
    // 1. not inside default aggregate expressions
    // 2. With no default aggregate defined
    // 3. there are distinct selection of measures
    // If yes, only the raw (non aggregated) fact can answer this query.
    // In that case remove aggregate facts from the candidate fact list
    if (hasMeasuresInDistinctClause(cubeql, cubeql.getSelectAST(), false)
      || hasMeasuresInDistinctClause(cubeql, cubeql.getHavingAST(), false)
      || hasMeasuresNotInDefaultAggregates(cubeql, cubeql.getSelectAST(), null, aggregateResolverDisabled)
      || hasMeasuresNotInDefaultAggregates(cubeql, cubeql.getHavingAST(), null, aggregateResolverDisabled)
      || hasMeasuresNotInDefaultAggregates(cubeql, cubeql.getWhereAST(), null, aggregateResolverDisabled)
      || hasMeasures(cubeql, cubeql.getGroupByAST())
      || hasMeasures(cubeql, cubeql.getOrderByAST())) {
      Iterator<CandidateFact> factItr = cubeql.getCandidateFactTables().iterator();
      while (factItr.hasNext()) {
        CandidateFact candidate = factItr.next();
        if (candidate.fact.isAggregated()) {
          cubeql.addFactPruningMsgs(candidate.fact,
            CandidateTablePruneCause.missingDefaultAggregate(candidate.fact.getName()));
          factItr.remove();
        }
      }
      nonDefaultAggregates = true;
      LOG.info("Query has non default aggregates, no aggregate resolution will be done");
    }

    cubeql.pruneCandidateFactSet(CandidateTablePruneCode.MISSING_DEFAULT_AGGREGATE);

    if (nonDefaultAggregates || aggregateResolverDisabled) {
      return;
    }

    resolveClause(cubeql, cubeql.getSelectAST());

    resolveClause(cubeql, cubeql.getHavingAST());

    Configuration distConf = cubeql.getConf();
    boolean isDimOnlyDistinctEnabled = distConf.getBoolean(CubeQueryConfUtil.ENABLE_ATTRFIELDS_ADD_DISTINCT,
      CubeQueryConfUtil.DEFAULT_ATTR_FIELDS_ADD_DISTINCT);
    if (isDimOnlyDistinctEnabled) {
      // Check if any measure/aggregate columns and distinct clause used in
      // select tree. If not, update selectAST token "SELECT" to "SELECT DISTINCT"
      if (!hasMeasures(cubeql, cubeql.getSelectAST()) && !isDistinctClauseUsed(cubeql.getSelectAST())
        && !HQLParser.hasAggregate(cubeql.getSelectAST())) {
        cubeql.getSelectAST().getToken().setType(HiveParser.TOK_SELECTDI);
      }
    }
  }

  private ASTNode joinWithAnd(List<ASTNode> nodes) {
    if (nodes == null || nodes.size() == 0) {
      return null;
    }
    ASTNode firstNode = nodes.remove(0);
    ASTNode remaining = joinWithAnd(nodes);
    if (remaining == null) {
      return firstNode;
    }
    ASTNode newAST = new ASTNode(new CommonToken(KW_AND, "and"));
    newAST.addChild(firstNode);
    newAST.addChild(remaining);
    return newAST;
  }

  private void pushWhereToHaving(CubeQueryContext cubeql) {
    if (!whetherPushwhereToHaving) {
      // push to having disabled.
      return;
    }
    ASTNode whereAST = cubeql.getWhereAST();
    if (whereAST == null || whereAST.getChildCount() == 0) {
      // nothing in where clause. Can't extract having clauses.
      return;
    }
    List<ASTNode> havingASTs = Lists.newArrayList();
    // whereAST has TOK_WHERE and single child. the single child is where the clause starts.
    ASTNode whereChildAST = (ASTNode) whereAST.getChild(0);
    whereChildAST = extractHavingFromWhere(whereChildAST, havingASTs, cubeql);
    if (havingASTs.isEmpty()) {
      // could not find any clauses to be promoted to having. where clause would have been unchanged.
      return;
    }
    // some where clauses have been extracted as having clauses. Have to put remaining where clauses as
    // where and extracted where clauses as having.
    if (whereChildAST == null) {
      // all wheres extracted as having
      cubeql.setWhereAST(null);
    } else {
      whereAST.setChild(0, whereChildAST);
      cubeql.setWhereAST(whereAST);
    }
    // join the extracted having clauses with and, append them to existing having clauses.
    ASTNode joinedHavingAST = joinWithAnd(havingASTs);
    if (cubeql.getHavingAST() == null) {
      ASTNode newAST = new ASTNode(new CommonToken(TOK_HAVING));
      newAST.addChild(joinedHavingAST);
      cubeql.setHavingAST(newAST);
    } else {
      ASTNode existingHavingTree = (ASTNode) cubeql.getHavingAST().getChild(0);
      // existing having clause. Let's to to rightmost child(which would be last clause), and replace that
      // by and clause on itself and the joined extracted having tree.
      while (existingHavingTree.getToken().getType() == KW_AND) {
        existingHavingTree = (ASTNode) existingHavingTree.getChild(existingHavingTree.getChildCount() - 1);
      }
      ASTNode parent = (ASTNode) existingHavingTree.getParent();
      ASTNode newAST = new ASTNode(new CommonToken(KW_AND, "and"));
      newAST.addChild(existingHavingTree);
      newAST.addChild(joinedHavingAST);
      parent.setChild(parent.getChildCount() - 1, newAST);
    }

  }

  /**
   *
   * @param ast         where ast.
   * @param havingASTs  the list in which having asts are to be extracted.
   * @param cubeql      cube query context
   * @return            new where ast, after having asts have been extracted out. returns null if all where clauses
   *                    were extracted as having clauses.
   */
  private ASTNode extractHavingFromWhere(ASTNode ast, List<ASTNode> havingASTs, CubeQueryContext cubeql) {
    if (ast.getToken().getType() == KW_AND) {
      // recursive case.
      ASTNode left = extractHavingFromWhere((ASTNode) ast.getChild(0), havingASTs, cubeql);
      ASTNode right = extractHavingFromWhere((ASTNode) ast.getChild(1), havingASTs, cubeql);
      if (left == null && right == null) {
        return null;
      } else if (left == null) {
        return right;
      } else if (right == null) {
        return left;
      } else {
        ast.setChild(0, left);
        ast.setChild(1, right);
        return ast;
      }
    } else {
      // base case
      if (hasMeasures(cubeql, ast) || HQLParser.hasAggregate(ast)) {
        havingASTs.add(ast);
        return null;
      } else {
        return ast;
      }
    }
  }

  // We need to traverse the clause looking for eligible measures which can be
  // wrapped inside aggregates
  // We have to skip any columns that are already inside an aggregate UDAF
  private String resolveClause(CubeQueryContext cubeql, ASTNode clause) throws SemanticException {

    if (clause == null) {
      return null;
    }

    for (int i = 0; i < clause.getChildCount(); i++) {
      transform(cubeql, clause, (ASTNode) clause.getChild(i), i);
    }

    return HQLParser.getString(clause);
  }

  private void transform(CubeQueryContext cubeql, ASTNode parent, ASTNode node, int nodePos) throws SemanticException {
    if (parent == null || node == null) {
      return;
    }
    int nodeType = node.getToken().getType();

    if (!(HQLParser.isAggregateAST(node))) {
      if (nodeType == HiveParser.TOK_TABLE_OR_COL || nodeType == HiveParser.DOT) {
        // Leaf node
        ASTNode wrapped = wrapAggregate(cubeql, node);
        if (wrapped != node) {
          parent.setChild(nodePos, wrapped);
          // Check if this node has an alias
          ASTNode sibling = HQLParser.findNodeByPath(parent, Identifier);
          String expr;
          if (sibling != null) {
            expr = HQLParser.getString(parent);
          } else {
            expr = HQLParser.getString(wrapped);
          }
          cubeql.addAggregateExpr(expr.trim());
        }
      } else {
        // Dig deeper in non-leaf nodes
        for (int i = 0; i < node.getChildCount(); i++) {
          transform(cubeql, node, (ASTNode) node.getChild(i), i);
        }
      }
    }
  }

  // Wrap an aggregate function around the node if its a measure, leave it
  // unchanged otherwise
  private ASTNode wrapAggregate(CubeQueryContext cubeql, ASTNode node) throws SemanticException {

    String tabname = null;
    String colname;

    if (node.getToken().getType() == HiveParser.TOK_TABLE_OR_COL) {
      colname = ((ASTNode) node.getChild(0)).getText();
    } else {
      // node in 'alias.column' format
      ASTNode tabident = HQLParser.findNodeByPath(node, TOK_TABLE_OR_COL, Identifier);
      ASTNode colIdent = (ASTNode) node.getChild(1);

      colname = colIdent.getText();
      tabname = tabident.getText();
    }

    String msrname = StringUtils.isBlank(tabname) ? colname : tabname + "." + colname;

    if (cubeql.isCubeMeasure(msrname)) {
      CubeMeasure measure = cubeql.getCube().getMeasureByName(colname);
      String aggregateFn = measure.getAggregate();

      if (StringUtils.isBlank(aggregateFn)) {
        throw new SemanticException(ErrorMsg.NO_DEFAULT_AGGREGATE, colname);
      }
      ASTNode fnroot = new ASTNode(new CommonToken(HiveParser.TOK_FUNCTION));
      fnroot.setParent(node.getParent());

      ASTNode fnIdentNode = new ASTNode(new CommonToken(HiveParser.Identifier, aggregateFn));
      fnIdentNode.setParent(fnroot);
      fnroot.addChild(fnIdentNode);

      node.setParent(fnroot);
      fnroot.addChild(node);

      return fnroot;
    } else {
      return node;
    }
  }

  private boolean hasMeasuresNotInDefaultAggregates(CubeQueryContext cubeql, ASTNode node, String function,
    boolean aggregateResolverDisabled) {
    if (node == null) {
      return false;
    }

    if (HQLParser.isAggregateAST(node)) {
      if (node.getChild(0).getType() == HiveParser.Identifier) {
        function = BaseSemanticAnalyzer.unescapeIdentifier(node.getChild(0).getText());
      }
    } else if (isMeasure(cubeql, node)) {
      // Exit for the recursion

      String colname;
      if (node.getToken().getType() == HiveParser.TOK_TABLE_OR_COL) {
        colname = ((ASTNode) node.getChild(0)).getText();
      } else {
        // node in 'alias.column' format
        ASTNode colIdent = (ASTNode) node.getChild(1);
        colname = colIdent.getText();
      }
      CubeMeasure measure = cubeql.getCube().getMeasureByName(colname);
      if (function != null && !function.isEmpty()) {
        // Get the cube measure object and check if the passed function is the
        // default one set for this measure
        return !function.equalsIgnoreCase(measure.getAggregate());
      } else if (!aggregateResolverDisabled && measure.getAggregate() != null) {
        // not inside any aggregate, but default aggregate exists
        return false;
      }
      return true;
    }

    for (int i = 0; i < node.getChildCount(); i++) {
      if (hasMeasuresNotInDefaultAggregates(cubeql, (ASTNode) node.getChild(i), function, aggregateResolverDisabled)) {
        // Return on the first measure not inside its default aggregate
        return true;
      }
    }
    return false;
  }

  /*
   * Check if distinct keyword used in node
   */
  private boolean isDistinctClauseUsed(ASTNode node) {
    if (node == null) {
      return false;
    }
    if (node.getToken() != null) {
      if (node.getToken().getType() == HiveParser.TOK_FUNCTIONDI
        || node.getToken().getType() == HiveParser.TOK_SELECTDI) {
        return true;
      }
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      if (isDistinctClauseUsed((ASTNode) node.getChild(i))) {
        return true;
      }
    }
    return false;
  }

  private boolean hasMeasuresInDistinctClause(CubeQueryContext cubeql, ASTNode node, boolean hasDistinct) {
    if (node == null) {
      return false;
    }

    int exprTokenType = node.getToken().getType();
    boolean isDistinct = hasDistinct;
    if (exprTokenType == HiveParser.TOK_FUNCTIONDI || exprTokenType == HiveParser.TOK_SELECTDI) {
      isDistinct = true;
    } else if (isMeasure(cubeql, node) && isDistinct) {
      // Exit for the recursion
      return true;
    }

    for (int i = 0; i < node.getChildCount(); i++) {
      if (hasMeasuresInDistinctClause(cubeql, (ASTNode) node.getChild(i), isDistinct)) {
        // Return on the first measure in distinct clause
        return true;
      }
    }
    return false;
  }

  private boolean hasMeasures(CubeQueryContext cubeql, ASTNode node) {
    if (node == null) {
      return false;
    }

    if (isMeasure(cubeql, node)) {
      return true;
    }

    for (int i = 0; i < node.getChildCount(); i++) {
      if (hasMeasures(cubeql, (ASTNode) node.getChild(i))) {
        return true;
      }
    }

    return false;
  }

  private boolean isMeasure(CubeQueryContext cubeql, ASTNode node) {
    String tabname = null;
    String colname;
    int nodeType = node.getToken().getType();
    if (!(nodeType == HiveParser.TOK_TABLE_OR_COL || nodeType == HiveParser.DOT)) {
      return false;
    }

    if (nodeType == HiveParser.TOK_TABLE_OR_COL) {
      colname = ((ASTNode) node.getChild(0)).getText();
    } else {
      // node in 'alias.column' format
      ASTNode tabident = HQLParser.findNodeByPath(node, TOK_TABLE_OR_COL, Identifier);
      ASTNode colIdent = (ASTNode) node.getChild(1);

      colname = colIdent.getText();
      tabname = tabident.getText();
    }

    String msrname = StringUtils.isBlank(tabname) ? colname : tabname + "." + colname;

    return cubeql.isCubeMeasure(msrname);
  }

  static void updateAggregates(ASTNode root, CubeQueryContext cubeql) {
    if (root == null) {
      return;
    }

    if (HQLParser.isAggregateAST(root)) {
      cubeql.addAggregateExpr(HQLParser.getString(root).trim());
    } else {
      for (int i = 0; i < root.getChildCount(); i++) {
        ASTNode child = (ASTNode) root.getChild(i);
        updateAggregates(child, cubeql);
      }
    }
  }
}
