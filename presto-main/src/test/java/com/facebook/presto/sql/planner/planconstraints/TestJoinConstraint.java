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
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.spi.plan.JoinDistributionType.REPLICATED;
import static com.facebook.presto.spi.plan.JoinType.INNER;
import static com.facebook.presto.sql.planner.planconstraints.JoinConstraint.matches;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestJoinConstraint
{
    private Metadata metadata;
    private Session session;
    private PlanNodeIdAllocator idAllocator;

    public TestJoinConstraint()
    {
        idAllocator = new PlanNodeIdAllocator();
        Session.SessionBuilder sessionBuilder = testSessionBuilder()
                .setCatalog("local")
                .setSchema("tiny")
                .setSystemProperty("task_concurrency", "1");
        session = sessionBuilder.build();
        LocalQueryRunner queryRunner = new LocalQueryRunner(session);
        queryRunner.createCatalog(session.getCatalog().get(),
                new TpchConnectorFactory(1),
                ImmutableMap.of());
        metadata = queryRunner.getMetadata();
    }

//    @Test
//    public void testBuildJoinConstraint()
//            throws IOException
//    {
//        ArrayList<JoinConstraint> joinConstraints = new ArrayList<>();
//        JoinConstraint.parse("JOIN(((t4 t3) t2) t1) CARD(t1 1000000) JOIN(((t4 t3) [P]) t2) [P] t1) [R]", joinConstraints);
//        for (JoinConstraint joinConstraint : joinConstraints) {
//            System.out.println(joinConstraint.baseIdSet());
//        }
//
//        System.out.println(joinConstraints);
//    }

    @Test
    public void testSmokeJoinConstraint()
    {
        PlanBuilder p = new PlanBuilder(session, idAllocator, metadata);
        PlanNode toCompare = p.join(
                INNER,
                p.values(new PlanNodeId("valuesA")),
                p.values(new PlanNodeId("valuesB")));

        JoinConstraint constraint = new JoinConstraint(INNER,
                Optional.empty(),
                ImmutableList.of(new RelationConstraint("valuesA"), new RelationConstraint("valuesB")));

        assertTrue(matches(constraint, toCompare));

        // Flipped order does not match
        constraint = new JoinConstraint(INNER,
                Optional.empty(),
                ImmutableList.of(new RelationConstraint("valuesB"), new RelationConstraint("valuesA")));

        assertFalse(matches(constraint, toCompare));

        // Distribution Type must match that of the constraint
        constraint = new JoinConstraint(INNER,
                Optional.of(REPLICATED),
                ImmutableList.of(new RelationConstraint("valuesA"), new RelationConstraint("valuesB")));

        assertFalse(matches(constraint, toCompare));

        toCompare = p.join(
                INNER,
                p.values(new PlanNodeId("valuesA")),
                p.values(new PlanNodeId("valuesB")),
                REPLICATED);

        assertTrue(matches(constraint, toCompare));
    }

    @Test
    public void testDeepJoinTree()
    {
        PlanBuilder p = new PlanBuilder(session, idAllocator, metadata);
        PlanNode toCompare = p.join(
                INNER,
                p.join(
                        INNER,
                        p.values(new PlanNodeId("valuesA")),
                        p.values(new PlanNodeId("valuesB"))),
                p.join(
                        INNER,
                        p.values(new PlanNodeId("valuesC")),
                        p.values(new PlanNodeId("valuesD"))));

        JoinConstraint constraint = new JoinConstraint(INNER,
                Optional.empty(),
                ImmutableList.of(
                        new JoinConstraint(INNER,
                                Optional.empty(),
                                ImmutableList.of(new RelationConstraint("valuesA"), new RelationConstraint("valuesB"))),
                        new JoinConstraint(INNER,
                                Optional.empty(),
                                ImmutableList.of(new RelationConstraint("valuesC"), new RelationConstraint("valuesD")))));
        assertTrue(matches(constraint, toCompare));
    }

    @Test
    public void testProjectNotConsideredALeafNode()
    {
        PlanBuilder p = new PlanBuilder(session, idAllocator, metadata);
        PlanNode toCompare = p.join(
                INNER,
                p.join(
                        INNER,
                        p.values(new PlanNodeId("valuesA")),
                        p.values(new PlanNodeId("valuesB"))),
                p.project(Assignments.of(), p.join(
                        INNER,
                        p.values(new PlanNodeId("valuesC")),
                        p.values(new PlanNodeId("valuesD")))));

        JoinConstraint constraint = new JoinConstraint(INNER,
                Optional.empty(),
                ImmutableList.of(
                        new JoinConstraint(INNER,
                                Optional.empty(),
                                ImmutableList.of(new RelationConstraint("valuesA"), new RelationConstraint("valuesB"))),
                        new JoinConstraint(INNER,
                                Optional.empty(),
                                ImmutableList.of(new RelationConstraint("valuesC"), new RelationConstraint("valuesD")))));
        assertTrue(matches(constraint, toCompare));
    }
}
