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
import com.facebook.presto.spi.SourceLocation;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.TreeMap;

import static com.facebook.presto.spi.plan.JoinDistributionType.REPLICATED;
import static com.facebook.presto.spi.plan.JoinType.INNER;
import static com.facebook.presto.sql.planner.planconstraints.ConstraintMatcherUtil.matches;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestJoinConstraint
{
    private final Lookup lookup = Lookup.noLookup();
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

    @Test
    public void testSmokeJoinConstraint()
    {
        HashBiMap<SourceLocation, String> biMapSourceAlias = HashBiMap.create();
        biMapSourceAlias.put(new SourceLocation(1, 1), "valuesA");
        biMapSourceAlias.put(new SourceLocation(2, 1), "valuesB");

        TreeMap<SourceLocation, String> sourceLocationAliasMap = new TreeMap<>(biMapSourceAlias);

        PlanBuilder p = new PlanBuilder(session, idAllocator, metadata);
        PlanNode toCompare = p.join(
                INNER,
                p.values(biMapSourceAlias.inverse().get("valuesA")),
                p.values(biMapSourceAlias.inverse().get("valuesB")));

        JoinConstraint constraint = new JoinConstraint(INNER,
                Optional.empty(),
                ImmutableList.of(new RelationConstraint("valuesA"), new RelationConstraint("valuesB")));

        assertTrue(matches(lookup, constraint, toCompare, sourceLocationAliasMap));

        // Flipped order does not match
        constraint = new JoinConstraint(INNER,
                Optional.empty(),
                ImmutableList.of(new RelationConstraint("valuesB"), new RelationConstraint("valuesA")));

        assertFalse(matches(lookup, constraint, toCompare, sourceLocationAliasMap));

        // Distribution Type must match that of the constraint
        constraint = new JoinConstraint(INNER,
                Optional.of(REPLICATED),
                ImmutableList.of(new RelationConstraint("valuesA"), new RelationConstraint("valuesB")));

        assertFalse(matches(lookup, constraint, toCompare, sourceLocationAliasMap));

        toCompare = p.join(
                INNER,
                p.values(biMapSourceAlias.inverse().get("valuesA")),
                p.values(biMapSourceAlias.inverse().get("valuesB")),
                REPLICATED);

        assertTrue(matches(lookup, constraint, toCompare, sourceLocationAliasMap));
    }

    @Test
    public void testDeepJoinTree()
    {
        HashBiMap<SourceLocation, String> biMapSourceAlias = HashBiMap.create();
        biMapSourceAlias.put(new SourceLocation(1, 1), "valuesA");
        biMapSourceAlias.put(new SourceLocation(2, 1), "valuesB");
        biMapSourceAlias.put(new SourceLocation(3, 1), "valuesC");
        biMapSourceAlias.put(new SourceLocation(4, 1), "valuesD");

        PlanBuilder p = new PlanBuilder(session, idAllocator, metadata);
        PlanNode toCompare = p.join(
                INNER,
                p.join(
                        INNER,
                        p.values(biMapSourceAlias.inverse().get("valuesA")),
                        p.values(biMapSourceAlias.inverse().get("valuesB"))),
                p.join(
                        INNER,
                        p.values(biMapSourceAlias.inverse().get("valuesC")),
                        p.values(biMapSourceAlias.inverse().get("valuesD"))));

        JoinConstraint constraint = new JoinConstraint(INNER,
                Optional.empty(),
                ImmutableList.of(
                        new JoinConstraint(INNER,
                                Optional.empty(),
                                ImmutableList.of(new RelationConstraint("valuesA"), new RelationConstraint("valuesB"))),
                        new JoinConstraint(INNER,
                                Optional.empty(),
                                ImmutableList.of(new RelationConstraint("valuesC"), new RelationConstraint("valuesD")))));
        assertTrue(matches(lookup, constraint, toCompare, new TreeMap<>(biMapSourceAlias)));
    }

    @Test
    public void testProjectNotConsideredALeafNode()
    {
        HashBiMap<SourceLocation, String> biMapSourceAlias = HashBiMap.create();
        biMapSourceAlias.put(new SourceLocation(1, 1), "valuesA");
        biMapSourceAlias.put(new SourceLocation(2, 1), "valuesB");
        biMapSourceAlias.put(new SourceLocation(3, 1), "valuesC");
        biMapSourceAlias.put(new SourceLocation(4, 1), "valuesD");

        PlanBuilder p = new PlanBuilder(session, idAllocator, metadata);
        PlanNode toCompare = p.join(
                INNER,
                p.join(
                        INNER,
                        p.values(biMapSourceAlias.inverse().get("valuesA")),
                        p.values(biMapSourceAlias.inverse().get("valuesB"))),
                p.project(Assignments.of(), p.join(
                        INNER,
                        p.values(biMapSourceAlias.inverse().get("valuesC")),
                        p.values(biMapSourceAlias.inverse().get("valuesD")))));

        JoinConstraint constraint = new JoinConstraint(INNER,
                Optional.empty(),
                ImmutableList.of(
                        new JoinConstraint(INNER,
                                Optional.empty(),
                                ImmutableList.of(new RelationConstraint("valuesA"), new RelationConstraint("valuesB"))),
                        new JoinConstraint(INNER,
                                Optional.empty(),
                                ImmutableList.of(new RelationConstraint("valuesC"), new RelationConstraint("valuesD")))));
        assertTrue(matches(lookup, constraint, toCompare, new TreeMap<>(biMapSourceAlias)));
    }
}
