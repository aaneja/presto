package com.facebook.presto.sql.planner;

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Optional;

import static com.facebook.presto.spi.plan.JoinDistributionType.REPLICATED;
import static com.facebook.presto.spi.plan.JoinType.INNER;
import static com.facebook.presto.sql.planner.JoinConstraintNode2.matches;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestJoinConstraintNode
{

    private Metadata metadata;
    private Session session;
    private PlanNodeIdAllocator idAllocator;

    public TestJoinConstraintNode()
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

    @Test
    public void testSmokeJoinConstraintNode2()
    {
        PlanBuilder p = new PlanBuilder(session, idAllocator, metadata);
        PlanNode toCompare = p.join(
                INNER,
                p.values(new PlanNodeId("valuesA")),
                p.values(new PlanNodeId("valuesB")));

        JoinConstraintNode2 constraint = new JoinConstraintNode2(INNER,
                Optional.empty(),
                ImmutableList.of(new RelationConstraint("valuesA"), new RelationConstraint("valuesB")));

        assertTrue(matches(constraint, toCompare));

        // Flipped order does not match
        constraint = new JoinConstraintNode2(INNER,
                Optional.empty(),
                ImmutableList.of(new RelationConstraint("valuesB"), new RelationConstraint("valuesA")));

        assertFalse(matches(constraint, toCompare));

        // Distribution Type must match that of the constraint
        constraint = new JoinConstraintNode2(INNER,
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

        JoinConstraintNode2 constraint = new JoinConstraintNode2(INNER,
                Optional.empty(),
                ImmutableList.of(
                        new JoinConstraintNode2(INNER,
                                Optional.empty(),
                                ImmutableList.of(new RelationConstraint("valuesA"), new RelationConstraint("valuesB"))),
                        new JoinConstraintNode2(INNER,
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

        JoinConstraintNode2 constraint = new JoinConstraintNode2(INNER,
                Optional.empty(),
                ImmutableList.of(
                        new JoinConstraintNode2(INNER,
                                Optional.empty(),
                                ImmutableList.of(new RelationConstraint("valuesA"), new RelationConstraint("valuesB"))),
                        new JoinConstraintNode2(INNER,
                                Optional.empty(),
                                ImmutableList.of(new RelationConstraint("valuesC"), new RelationConstraint("valuesD")))));
        assertTrue(matches(constraint, toCompare));
    }
}
