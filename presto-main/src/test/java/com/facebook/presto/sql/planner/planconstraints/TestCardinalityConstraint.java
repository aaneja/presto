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
    public TestCardinalityConstraint()
    {}

    @Test
    public void testSingleTableCardinality()
    {
        String query = "SELECT /*! card (nation 100000)*/ COUNT(*) FROM nation n, customer c WHERE n.nationkey = c.nationkey";
        Session sessionWithPlanConstraint = Session.builder(getQueryRunner().getDefaultSession())
                .addPlanConstraints(parse(Optional.of(query)))
                .build();
        PlanMatchPattern output = output(
                anyTree(
                        join(INNER,
                                ImmutableList.of(equiJoinClause("nationkey", "nationkey_1")),
                                anyTree(tableScan("nation", ImmutableMap.of("nationkey", "nationkey"))),
                                anyTree(tableScan("customer", ImmutableMap.of("nationkey_1", "nationkey"))))));

        assertPlan(query, sessionWithPlanConstraint, output);

        query = "SELECT /*! card (customer 10)*/ COUNT(*) FROM nation n, customer c WHERE n.nationkey = c.nationkey";
        sessionWithPlanConstraint = Session.builder(getQueryRunner().getDefaultSession())
                .addPlanConstraints(parse(Optional.of(query)))
                .build();
        assertPlan(query, sessionWithPlanConstraint, output);

        query = "SELECT /*! card (customer 10) card (nation 15)*/ COUNT(*) FROM nation n, customer c WHERE n.nationkey = c.nationkey";
        sessionWithPlanConstraint = Session.builder(getQueryRunner().getDefaultSession())
                .addPlanConstraints(parse(Optional.of(query)))
                .build();
        assertPlan(query, sessionWithPlanConstraint, output);

        query = "SELECT /*! card (customer 100) card (nation 15)*/ COUNT(*) FROM nation n, customer c WHERE n.nationkey = c.nationkey";
        sessionWithPlanConstraint = Session.builder(getQueryRunner().getDefaultSession())
                .addPlanConstraints(parse(Optional.of(query)))
                .build();
        output = output(
                anyTree(
                        join(INNER,
                                ImmutableList.of(equiJoinClause("nationkey_1", "nationkey")),
                                anyTree(tableScan("customer", ImmutableMap.of("nationkey_1", "nationkey"))),
                                anyTree(tableScan("nation", ImmutableMap.of("nationkey", "nationkey"))))));
        assertPlan(query, sessionWithPlanConstraint, output);

        query = "SELECT /*! card (customer 500000) card (nation 250000)*/ COUNT(*) FROM nation n, customer c WHERE n.nationkey = c.nationkey";
        sessionWithPlanConstraint = Session.builder(getQueryRunner().getDefaultSession())
                .addPlanConstraints(parse(Optional.of(query)))
                .build();
        assertPlan(query, sessionWithPlanConstraint, output);
    }

    @Test
    public void testCardinalityOnJoinConstraints()
    {
        String query = "SELECT COUNT(*) " +
                "FROM   nation n, " +
                "       orders o, " +
                "       customer c " +
                "WHERE  c.custkey = o.custkey" +
                "       AND c.nationkey = n.nationkey";
        assertPlan(query,
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

        Session sessionWithPlanConstraint = Session.builder(getQueryRunner().getDefaultSession())
                .addPlanConstraints(parse(Optional.of("/*! card ((customer orders) 10) */")))
                .build();

        assertPlan(query,
                sessionWithPlanConstraint,
                output(
                        anyTree(
                                join(INNER,
                                        ImmutableList.of(equiJoinClause("nationkey", "nationkey_1")),
                                        anyTree(tableScan("nation", ImmutableMap.of("nationkey", "nationkey"))),
                                        anyTree(
                                                join(INNER,
                                                        ImmutableList.of(equiJoinClause("custkey", "custkey_4")),
                                                        anyTree(tableScan("customer", ImmutableMap.of("custkey", "custkey", "nationkey_1", "nationkey"))),
                                                        anyTree(tableScan("orders", ImmutableMap.of("custkey_4", "custkey")))))))));

        sessionWithPlanConstraint = Session.builder(getQueryRunner().getDefaultSession())
                .addPlanConstraints(parse(Optional.of("/*! card ((customer orders) 1000) card (nation 50000)*/")))
                .build();

        assertPlan(query,
                sessionWithPlanConstraint,
                output(
                        anyTree(
                                join(INNER,
                                        ImmutableList.of(equiJoinClause("nationkey", "nationkey_1")),
                                        anyTree(tableScan("nation", ImmutableMap.of("nationkey", "nationkey"))),
                                        anyTree(
                                                join(INNER,
                                                        ImmutableList.of(equiJoinClause("custkey", "custkey_4")),
                                                        anyTree(tableScan("customer", ImmutableMap.of("custkey", "custkey", "nationkey_1", "nationkey"))),
                                                        anyTree(tableScan("orders", ImmutableMap.of("custkey_4", "custkey")))))))));
    }
}
