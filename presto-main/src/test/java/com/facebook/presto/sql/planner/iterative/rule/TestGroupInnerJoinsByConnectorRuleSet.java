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
package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleAssert;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleTester;
import com.facebook.presto.sql.planner.optimizations.GroupInnerJoinsByConnectorRuleSet;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Optional;

import static com.facebook.airlift.testing.Closeables.closeAllRuntimeException;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.SystemSessionProperties.INNER_JOIN_PUSHDOWN_ENABLED;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.spi.plan.JoinType.FULL;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

public class TestGroupInnerJoinsByConnectorRuleSet
{
    private static final Metadata METADATA = MetadataManager.createTestMetadataManager();
    private static final PlanBuilder PLAN_BUILDER = new PlanBuilder(TEST_SESSION, new PlanNodeIdAllocator(), METADATA);
    private RuleTester tester;

    @BeforeClass
    public void setUp()
    {
        tester = new RuleTester(
                ImmutableList.of(),
                ImmutableMap.of(
                        INNER_JOIN_PUSHDOWN_ENABLED, "true"),
                Optional.of(4));
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        closeAllRuntimeException(tester);
        tester = null;
    }
    @Test
    public void testDoesNotPushDownOuterJoin()
    {
        String connectorName = "test_catalog";

        VariableReferenceExpression left = newBigintVariable("a1");
        VariableReferenceExpression right = newBigintVariable("a2");
        EquiJoinClause joinClause = new EquiJoinClause(left, right);

        assertGroupInnerJoinsByConnectorRuleSet().on(p -> p.join(FULL,
                tableScan(connectorName, "a1", "b1"),
                tableScan(connectorName, "a2", "b2"),
                joinClause)).doesNotFire();
    }

    private RuleAssert assertGroupInnerJoinsByConnectorRuleSet()
    {
        return tester.assertThat(new GroupInnerJoinsByConnectorRuleSet.OnlyJoinRule(tester.getMetadata()));
    }

    private TableScanNode tableScan(String connectorName, String... columnNames)
    {
        return PLAN_BUILDER.tableScan(
                connectorName,
                Arrays.stream(columnNames).map(TestGroupInnerJoinsByConnectorRuleSet::newBigintVariable).collect(toImmutableList()),
                Arrays.stream(columnNames).map(TestGroupInnerJoinsByConnectorRuleSet::newBigintVariable).collect(toMap(identity(), variable -> new ColumnHandle() {})));
    }

    private static VariableReferenceExpression newBigintVariable(String name)
    {
        return new VariableReferenceExpression(Optional.empty(), name, BIGINT);
    }
}
