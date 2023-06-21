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

import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static com.facebook.presto.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.exchange;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.facebook.presto.sql.planner.plan.JoinNode.DistributionType.REPLICATED;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;

public class TestCrossJoinsReordering
        extends BasePlanTest
{

    public TestCrossJoinsReordering()
    {
        super(ImmutableMap.of(
                JOIN_DISTRIBUTION_TYPE, FeaturesConfig.JoinDistributionType.AUTOMATIC.name(),
                JOIN_REORDERING_STRATEGY, FeaturesConfig.JoinReorderingStrategy.AUTOMATIC.name()));
    }

    @Test
    public void testCrossJoinIsUsedInJoinReordering()
    {
        String query = "select 1 FROM supplier s , lineitem l , orders o where l.orderkey = o.orderkey";
        assertPlan(query,
                anyTree(
                        join(INNER,
                                ImmutableList.of(),
                                Optional.empty(),
                                Optional.of(REPLICATED),
                                tableScan("supplier"),
                                exchange(join(INNER,
                                        ImmutableList.of(equiJoinClause("L_ORDERKEY", "O_ORDERKEY")),
                                        anyTree(tableScan("lineitem", ImmutableMap.of("L_ORDERKEY", "orderkey"))),
                                        anyTree(tableScan("orders", ImmutableMap.of("O_ORDERKEY", "orderkey"))))))));
    }
}
