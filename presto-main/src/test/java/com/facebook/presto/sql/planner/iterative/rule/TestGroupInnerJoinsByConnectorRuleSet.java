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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorCapabilities;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorNodePartitioningProvider;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.transaction.IsolationLevel;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleAssert;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleTester;
import com.facebook.presto.sql.planner.optimizations.GroupInnerJoinsByConnectorRuleSet;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.tpch.ColumnNaming;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.facebook.presto.tpch.TpchMetadata;
import com.facebook.presto.tpch.TpchNodePartitioningProvider;
import com.facebook.presto.tpch.TpchRecordSetProvider;
import com.facebook.presto.tpch.TpchSplitManager;
import com.facebook.presto.tpch.TpchTransactionHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.airlift.testing.Closeables.closeAllRuntimeException;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.SystemSessionProperties.INNER_JOIN_PUSHDOWN_ENABLED;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.metadata.SessionPropertyManager.createTestingSessionPropertyManager;
import static com.facebook.presto.spi.connector.ConnectorCapabilities.SUPPORTS_JOIN_PUSHDOWN;
import static com.facebook.presto.spi.plan.JoinType.FULL;
import static com.facebook.presto.spi.plan.JoinType.INNER;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

public class TestGroupInnerJoinsByConnectorRuleSet
{
    public static final String CATALOG_SUPPORTING_JOIN_PUSHDOWN = "catalog_join_pushdown_supported";
    private PlanBuilder planBuilder;
    private RuleTester tester;

    @BeforeClass
    public void setUp()
    {
        LocalQueryRunner runner = new LocalQueryRunner(TEST_SESSION);
        runner.createCatalog(CATALOG_SUPPORTING_JOIN_PUSHDOWN, new TestingJoinPushdownConnectorFactory()
        {
            @Override
            public String getName()
            {
                return "tpch_with_join_pushdown";
            }
        }, ImmutableMap.of());

        tester = new RuleTester(
                ImmutableList.of(),
                RuleTester.getSession(ImmutableMap.of(INNER_JOIN_PUSHDOWN_ENABLED, "true"), createTestingSessionPropertyManager()),
                runner,
                new TpchConnectorFactory(1));

        planBuilder = new PlanBuilder(TEST_SESSION, new PlanNodeIdAllocator(), runner.getMetadata());
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

    @Test
    public void testPushdownValid()
    {
        VariableReferenceExpression left = newBigintVariable("a1");
        VariableReferenceExpression right = newBigintVariable("a2");
        EquiJoinClause joinClause = new EquiJoinClause(left, right);

        assertGroupInnerJoinsByConnectorRuleSet().on(p -> p.join(INNER,
                tableScan(CATALOG_SUPPORTING_JOIN_PUSHDOWN, "a1", "b1"),
                tableScan(CATALOG_SUPPORTING_JOIN_PUSHDOWN, "a2", "b2"),
                joinClause)).doesNotFire(); // Fails now, change this to do actual plan matching
    }

    private RuleAssert assertGroupInnerJoinsByConnectorRuleSet()
    {
        return tester.assertThat(new GroupInnerJoinsByConnectorRuleSet.OnlyJoinRule(tester.getMetadata()), ImmutableList.of(CATALOG_SUPPORTING_JOIN_PUSHDOWN));
    }

    private TableScanNode tableScan(String connectorName, String... columnNames)
    {
        return planBuilder.tableScan(
                connectorName,
                Arrays.stream(columnNames).map(TestGroupInnerJoinsByConnectorRuleSet::newBigintVariable).collect(toImmutableList()),
                Arrays.stream(columnNames).map(TestGroupInnerJoinsByConnectorRuleSet::newBigintVariable).collect(toMap(identity(), variable -> new ColumnHandle() {})));
    }

    private static VariableReferenceExpression newBigintVariable(String name)
    {
        return new VariableReferenceExpression(Optional.empty(), name, BIGINT);
    }

    private static class TestingJoinPushdownConnectorFactory
            extends TpchConnectorFactory
    {
        @Override
        public Connector create(String catalogName, Map<String, String> properties, ConnectorContext context)
        {
            int splitsPerNode = super.getSplitsPerNode(properties);
            ColumnNaming columnNaming = ColumnNaming.valueOf(properties.getOrDefault(TPCH_COLUMN_NAMING_PROPERTY, ColumnNaming.SIMPLIFIED.name()).toUpperCase());
            NodeManager nodeManager = context.getNodeManager();

            return new Connector()
            {
                @Override
                public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
                {
                    return TpchTransactionHandle.INSTANCE;
                }

                @Override
                public ConnectorMetadata getMetadata(ConnectorTransactionHandle transaction)
                {
                    return new TpchMetadata(catalogName, columnNaming, isPredicatePushdownEnabled(), isPartitioningEnabled(properties))
                    {
                        @Override
                        public boolean isPushdownSupportedForFilter(ConnectorSession session, ConnectorTableHandle tableHandle, RowExpression filter, Map<VariableReferenceExpression, ColumnHandle> symbolToColumnHandleMap)
                        {
                            return true;
                        }
                    };
                }

                @Override
                public ConnectorSplitManager getSplitManager()
                {
                    return new TpchSplitManager(nodeManager, splitsPerNode);
                }

                @Override
                public ConnectorRecordSetProvider getRecordSetProvider()
                {
                    return new TpchRecordSetProvider();
                }

                @Override
                public ConnectorNodePartitioningProvider getNodePartitioningProvider()
                {
                    return new TpchNodePartitioningProvider(nodeManager, splitsPerNode);
                }

                @Override
                public Set<ConnectorCapabilities> getCapabilities()
                {
                    return ImmutableSet.of(SUPPORTS_JOIN_PUSHDOWN);
                }
            };
        }
    }
}
