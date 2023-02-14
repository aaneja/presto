/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.sql.planner;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.TestingRowExpressionTranslator;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.facebook.presto.sql.planner.optimizations.AddNotNullFiltersToJoinNode.ExtractInferredNotNullVariablesVisitor;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.SystemSessionProperties.OPTIMIZE_NULLS_IN_JOINS;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.LEFT;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.RIGHT;
import static com.facebook.presto.sql.relational.Expressions.variable;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;

public class TestAddNotNullFiltersToJoinNode
        extends BasePlanTest
{
    private final Map<String, Type> testVariableTypeMap = ImmutableMap.of("a", BIGINT, "b", BIGINT, "c", BIGINT, "d", BIGINT, "e", BIGINT);
    private final ExtractInferredNotNullVariablesVisitor visitor;
    private final TestingRowExpressionTranslator rowExpressionTranslator;

    public TestAddNotNullFiltersToJoinNode()
    {
        super(ImmutableMap.of(OPTIMIZE_NULLS_IN_JOINS, Boolean.toString(true)));

        FunctionAndTypeManager functionAndTypeManager = createTestFunctionAndTypeManager();
        Metadata metadata = MetadataManager.createTestMetadataManager();
        rowExpressionTranslator = new TestingRowExpressionTranslator(metadata);
        visitor = new ExtractInferredNotNullVariablesVisitor(functionAndTypeManager);
    }

    @Test
    public void testNotNullPredicatesAddedForSingleEquiJoinClause()
    {
        String query = "select 1 from lineitem l join orders o on l.orderkey = o.orderkey";
        assertPlan(query,
                anyTree(
                        join(INNER,
                                ImmutableList.of(equiJoinClause("LINE_ORDER_KEY", "ORDERS_ORDER_KEY")),
                                anyTree(
                                        filter("LINE_ORDER_KEY IS NOT NULL",
                                                tableScan("lineitem", ImmutableMap.of("LINE_ORDER_KEY", "orderkey")))),
                                anyTree(
                                        filter("ORDERS_ORDER_KEY IS NOT NULL",
                                                tableScan("orders", ImmutableMap.of("ORDERS_ORDER_KEY", "orderkey")))))));
    }

    @Test
    public void testNotNullPredicatesAddedForCrossJoinReducedToInnerJoin()
    {
        String query = "select 1 from lineitem l, orders o where l.orderkey = o.orderkey";
        assertPlan(query,
                anyTree(
                        join(INNER,
                                ImmutableList.of(equiJoinClause("LINE_ORDER_KEY", "ORDERS_ORDER_KEY")),
                                anyTree(
                                        filter("LINE_ORDER_KEY IS NOT NULL",
                                                tableScan("lineitem", ImmutableMap.of("LINE_ORDER_KEY", "orderkey")))),
                                anyTree(
                                        filter("ORDERS_ORDER_KEY IS NOT NULL",
                                                tableScan("orders", ImmutableMap.of("ORDERS_ORDER_KEY", "orderkey")))))));

        query = "select 1 from lineitem l join orders o on l.orderkey = o.orderkey, customer c where c.custkey = o.custkey";
        assertPlan(query,
                anyTree(
                        join(INNER,
                                ImmutableList.of(equiJoinClause("LINE_ORDER_KEY", "ORDERS_ORDER_KEY")),
                                anyTree(
                                        filter("LINE_ORDER_KEY IS NOT NULL",
                                                tableScan("lineitem", ImmutableMap.of("LINE_ORDER_KEY", "orderkey")))),
                                anyTree(join(INNER,
                                        ImmutableList.of(equiJoinClause("ORDERS_CUSTOMER_KEY", "CUSTOMER_CUSTOMER_KEY")),
                                        anyTree(
                                                filter("ORDERS_CUSTOMER_KEY IS NOT NULL AND ORDERS_ORDER_KEY IS NOT NULL",
                                                        tableScan("orders", ImmutableMap.of("ORDERS_ORDER_KEY", "orderkey",
                                                                "ORDERS_CUSTOMER_KEY", "custkey")))),
                                        anyTree(
                                                filter("CUSTOMER_CUSTOMER_KEY IS NOT NULL",
                                                        tableScan("customer", ImmutableMap.of("CUSTOMER_CUSTOMER_KEY", "custkey")))))))));
    }

    @Test
    public void testMultipleNotNullAddedForMultipleEquiJoinClause()
    {
        String query = "select 1 from lineitem l join orders o on l.orderkey = o.orderkey and l.partkey = o.custkey";
        assertPlan(query,
                anyTree(
                        join(INNER,
                                ImmutableList.of(equiJoinClause("LINE_ORDER_KEY", "ORDERS_ORDER_KEY"),
                                        equiJoinClause("partkey", "custkey")),
                                anyTree(
                                        filter("LINE_ORDER_KEY IS NOT NULL AND partkey IS NOT NULL",
                                                tableScan("lineitem",
                                                        ImmutableMap.of(
                                                                "LINE_ORDER_KEY", "orderkey",
                                                                "partkey", "partkey")))),
                                anyTree(
                                        filter("ORDERS_ORDER_KEY IS NOT NULL AND custkey IS NOT NULL",
                                                tableScan("orders",
                                                        ImmutableMap.of(
                                                                "ORDERS_ORDER_KEY", "orderkey",
                                                                "custkey", "custkey")))))));
    }

    @Test
    public void testNotNullInferredForJoinFilter()
    {
        String query = "select 1 from lineitem l join orders o on l.orderkey = o.orderkey and partkey + custkey > 10";
        assertPlan(query,
                anyTree(
                        join(INNER,
                                //Only single equi join clause in this case
                                ImmutableList.of(equiJoinClause("LINE_ORDER_KEY", "ORDERS_ORDER_KEY")),
                                //Extra join filter is passed unchanged
                                Optional.of("partkey + custkey > 10"),
                                //We can infer NOT NULL filters on partkey and custkey since the ADD function cannot operate on NULL arguments
                                anyTree(
                                        filter("LINE_ORDER_KEY IS NOT NULL AND partkey IS NOT NULL",
                                                tableScan("lineitem",
                                                        ImmutableMap.of(
                                                                "LINE_ORDER_KEY", "orderkey",
                                                                "partkey", "partkey")))),
                                anyTree(
                                        filter("ORDERS_ORDER_KEY IS NOT NULL AND custkey IS NOT NULL",
                                                tableScan("orders",
                                                        ImmutableMap.of(
                                                                "ORDERS_ORDER_KEY", "orderkey",
                                                                "custkey", "custkey")))))));
    }

    @Test
    public void testNotNullPredicatesAddedOnlyForInnerSideTablesVariableReferences()
    {
        String query = "select 1 from lineitem l left join orders o on l.orderkey = o.orderkey and partkey - custkey > 10";
        assertPlan(query,
                anyTree(
                        join(LEFT,
                                ImmutableList.of(equiJoinClause("LINE_ORDER_KEY", "ORDERS_ORDER_KEY")),
                                Optional.of("partkey - custkey > 10"),
                                anyTree(
                                        tableScan("lineitem", ImmutableMap.of(
                                                "LINE_ORDER_KEY", "orderkey",
                                                "partkey", "partkey"))),
                                anyTree(
                                        filter("ORDERS_ORDER_KEY IS NOT NULL and custkey IS NOT NULL",
                                                tableScan("orders",
                                                        ImmutableMap.of(
                                                                "ORDERS_ORDER_KEY", "orderkey",
                                                                "custkey", "custkey")))))));

        query = "select 1 from lineitem l right join orders o on l.orderkey = o.orderkey and custkey > partkey";
        assertPlan(query,
                anyTree(
                        join(RIGHT,
                                ImmutableList.of(equiJoinClause("LINE_ORDER_KEY", "ORDERS_ORDER_KEY")),
                                Optional.of("custkey > partkey"),
                                anyTree(
                                        filter("LINE_ORDER_KEY IS NOT NULL and partkey IS NOT NULL",
                                                tableScan("lineitem", ImmutableMap.of(
                                                        "LINE_ORDER_KEY", "orderkey",
                                                        "partkey", "partkey")))),
                                anyTree(
                                        tableScan("orders",
                                                ImmutableMap.of(
                                                        "ORDERS_ORDER_KEY", "orderkey",
                                                        "custkey", "custkey"))))));
    }

    @Test
    public void testExtractInferredNotNullVariablesVisitor()
    {
        assertInferredNotNullVariableRefsListMatch("a + b > 10", buildListOfBigIntVariableRefs("a", "b"));
        assertInferredNotNullVariableRefsListMatch("a != 10 - b", buildListOfBigIntVariableRefs("a", "b"));
        assertInferredNotNullVariableRefsListMatch("a > b", buildListOfBigIntVariableRefs("a", "b"));
        assertInferredNotNullVariableRefsListMatch("a + NULL > b", buildListOfBigIntVariableRefs("a", "b"));

        //We can infer NOT NULL predicates on arguments of an AND expression
        assertInferredNotNullVariableRefsListMatch("a > b and c = d", buildListOfBigIntVariableRefs("a", "b", "c", "d"));
        //Cant infer NOT NULL on 'a' but can infer  on 'b > c'
        assertInferredNotNullVariableRefsListMatch("a IS NULL and b > c", buildListOfBigIntVariableRefs("b", "c"));
        //NOT won't have any impact on nullability checking; both b & c must be NOT NULL for this expression to evaluate to true
        assertInferredNotNullVariableRefsListMatch("NOT (b + 10 > c)", buildListOfBigIntVariableRefs("b", "c"));

        //We cannot infer NOT NULL predicates on arguments of an OR expression
        assertInferredNotNullVariableRefsListMatch("a > b or c = d", Collections.emptyList());
        //COALESCE can operate on NULL arguments, so cant infer predicates on its arguments
        assertInferredNotNullVariableRefsListMatch("COALESCE(a,b)", Collections.emptyList());
        //IN can operate on NULL arguments, so cant infer predicates on its arguments
        assertInferredNotNullVariableRefsListMatch("a IN (b,10,NULL)", Collections.emptyList());
    }

    private List<VariableReferenceExpression> buildListOfBigIntVariableRefs(String... var)
    {
        return Arrays.stream(var).map(x -> variable(x, BIGINT)).collect(Collectors.toList());
    }

    private void assertInferredNotNullVariableRefsListMatch(String filterSql, List<VariableReferenceExpression> expectedInferredNotNullVariables)
    {
        RowExpression rowExpression = rowExpressionTranslator.translate(filterSql, testVariableTypeMap);
        ImmutableSet.Builder<VariableReferenceExpression> builder = ImmutableSet.builder();
        rowExpression.accept(visitor, builder);
        assertEquals(builder.build(), expectedInferredNotNullVariables);
    }
}
