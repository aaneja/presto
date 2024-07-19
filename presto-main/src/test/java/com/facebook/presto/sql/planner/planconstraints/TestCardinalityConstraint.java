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
package com.facebook.presto.sql.planner.planconstraints;

import com.facebook.presto.Session;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.spi.plan.JoinType.INNER;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.output;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.facebook.presto.sql.planner.planconstraints.PlanConstraintsParser.parse;

public class TestCardinalityConstraint
        extends BasePlanTest
{
    @Test
    public void testSingleTableCardinality()
    {
        String query = "SELECT /*! card (n 100000)*/ COUNT(*) FROM nation n, customer c WHERE n.nationkey = c.nationkey";
        Session sessionWithPlanConstraint = buildSessionWithConstraint(query);
        PlanMatchPattern output = output(
                anyTree(
                        join(INNER,
                                ImmutableList.of(equiJoinClause("nationkey", "nationkey_1")),
                                anyTree(tableScan("nation", ImmutableMap.of("nationkey", "nationkey"))),
                                anyTree(tableScan("customer", ImmutableMap.of("nationkey_1", "nationkey"))))));

        assertPlan(query, sessionWithPlanConstraint, output);

        query = "SELECT /*! card (c 10)*/ COUNT(*) FROM nation n, customer c WHERE n.nationkey = c.nationkey";
        sessionWithPlanConstraint = buildSessionWithConstraint(query);
        assertPlan(query, sessionWithPlanConstraint, output);

        query = "SELECT /*! card (c 10) card (nation 15)*/ COUNT(*) FROM nation n, customer c WHERE n.nationkey = c.nationkey";
        sessionWithPlanConstraint = buildSessionWithConstraint(query);
        assertPlan(query, sessionWithPlanConstraint, output);

        query = "SELECT /*! card (c 100) card (nation 15)*/ COUNT(*) FROM nation n, customer c WHERE n.nationkey = c.nationkey";
        sessionWithPlanConstraint = buildSessionWithConstraint(query);
        output = output(
                anyTree(
                        join(INNER,
                                ImmutableList.of(equiJoinClause("nationkey_1", "nationkey")),
                                anyTree(tableScan("customer", ImmutableMap.of("nationkey_1", "nationkey"))),
                                anyTree(tableScan("nation", ImmutableMap.of("nationkey", "nationkey"))))));
        assertPlan(query, sessionWithPlanConstraint, output);

        query = "SELECT /*! card (c 500000) card (nation 250000)*/ COUNT(*) FROM nation n, customer c WHERE n.nationkey = c.nationkey";
        sessionWithPlanConstraint = buildSessionWithConstraint(query);
        assertPlan(query, sessionWithPlanConstraint, output);
    }

    @Test
    public void testCardinalityOnJoinConstraints()
    {
        String baseQuery = "SELECT COUNT(*) " +
                "FROM   nation n, " +
                "       orders o, " +
                "       customer c " +
                "WHERE  c.custkey = o.custkey" +
                "       AND c.nationkey = n.nationkey";
        assertPlan(baseQuery,
                output(
                        anyTree(
                                join(INNER,
                                        ImmutableList.of(equiJoinClause("custkey_4", "custkey")),
                                        anyTree(tableScan("orders", ImmutableMap.of("custkey_4", "custkey"))),
                                        anyTree(
                                                join(INNER,
                                                        ImmutableList.of(equiJoinClause("nationkey_1", "nationkey")),
                                                        anyTree(tableScan("customer", ImmutableMap.of("custkey", "custkey", "nationkey_1", "nationkey"))),
                                                        anyTree(tableScan("nation", ImmutableMap.of("nationkey", "nationkey")))))))));

        String queryWithConstraint = "/*! card ((c o) 10) */ " + baseQuery;
        Session sessionWithPlanConstraint = buildSessionWithConstraint(queryWithConstraint);

        assertPlan(queryWithConstraint,
                sessionWithPlanConstraint,
                output(
                        anyTree(
                                join(INNER,
                                        ImmutableList.of(equiJoinClause("nationkey", "nationkey_1")),
                                        // nation with a row count of 25 will be on the probe side
                                        anyTree(tableScan("nation", ImmutableMap.of("nationkey", "nationkey"))),
                                        anyTree(
                                                join(INNER,
                                                        ImmutableList.of(equiJoinClause("custkey", "custkey_4")),
                                                        anyTree(tableScan("customer", ImmutableMap.of("custkey", "custkey", "nationkey_1", "nationkey"))),
                                                        anyTree(tableScan("orders", ImmutableMap.of("custkey_4", "custkey")))))))));
    }


}
