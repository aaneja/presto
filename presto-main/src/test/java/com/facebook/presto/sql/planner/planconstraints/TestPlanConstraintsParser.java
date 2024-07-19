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

import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.spi.plan.JoinDistributionType.PARTITIONED;
import static com.facebook.presto.spi.plan.JoinDistributionType.REPLICATED;
import static com.facebook.presto.spi.plan.JoinType.INNER;
import static com.facebook.presto.sql.planner.planconstraints.PlanConstraintsParser.constraintsMarkerEnd;
import static com.facebook.presto.sql.planner.planconstraints.PlanConstraintsParser.constraintsMarkerStart;
import static com.facebook.presto.sql.planner.planconstraints.PlanConstraintsParser.parse;
import static org.testng.Assert.assertEquals;

public class TestPlanConstraintsParser
{
    @Test
    public void testTableNamesAsTokenKeywordPrefixes()
    {
        // All of these fail to parse
        parse(constraintsMarkerStart + " join (replicated supplier) " + constraintsMarkerEnd);
        parse(constraintsMarkerStart + " join (part supplier) " + constraintsMarkerEnd);

        // This will be a documented restriction on the plan constraints framework (P and R are restricted keywords for now)
        // parse(constraintsMarkerStart + " join (p supplier) " + constraintsMarkerEnd);
    }

    @Test
    public void testSimplePlanConstraints()
    {
        JoinConstraint expectedJoinConstraint = new JoinConstraint(INNER,
                Optional.empty(),
                ImmutableList.of(new RelationConstraint("A"), new RelationConstraint("B")));
        JoinConstraint expectedJoinConstraintWithReplicatedDistribution = new JoinConstraint(INNER,
                Optional.of(REPLICATED),
                ImmutableList.of(new RelationConstraint("A"), new RelationConstraint("B")));
        JoinConstraint expectedJoinConstraintWithPartitionedDistribution = new JoinConstraint(INNER,
                Optional.of(PARTITIONED),
                ImmutableList.of(new RelationConstraint("A"), new RelationConstraint("B")));
        CardinalityConstraint expectedCardinalityConstraint = new CardinalityConstraint(new RelationConstraint("FOO"), new CardinalityEstimate(1000));

        String planConstraintString = constraintsMarkerStart + " join (a b) " + constraintsMarkerEnd;
        List<PlanConstraint> planConstraints = parse(planConstraintString);
        assertEquals(planConstraints.size(), 1);
        assertEquals(planConstraints.get(0), expectedJoinConstraint);

        planConstraintString = constraintsMarkerStart + " join (a b) [R] " + constraintsMarkerEnd;
        planConstraints = parse(planConstraintString);
        assertEquals(planConstraints.size(), 1);
        assertEquals(planConstraints.get(0), expectedJoinConstraintWithReplicatedDistribution);

        planConstraintString = constraintsMarkerStart + " join (a b) [P] " + constraintsMarkerEnd;
        planConstraints = parse(planConstraintString);
        assertEquals(planConstraints.size(), 1);
        assertEquals(planConstraints.get(0), expectedJoinConstraintWithPartitionedDistribution);

        planConstraintString = constraintsMarkerStart + " card (foo 1000) " + constraintsMarkerEnd;
        planConstraints = parse(planConstraintString);
        assertEquals(planConstraints.size(), 1);
        assertEquals(planConstraints.get(0), expectedCardinalityConstraint);

        planConstraintString = constraintsMarkerStart + " join (a b) card (foo 1000) " + constraintsMarkerEnd;
        planConstraints = parse(planConstraintString);
        assertEquals(planConstraints.size(), 2);
        assertEquals(planConstraints.get(0), expectedJoinConstraint);
        assertEquals(planConstraints.get(1), expectedCardinalityConstraint);

        planConstraintString = constraintsMarkerStart + "  card (foo 1000) join (a b) [P] " + constraintsMarkerEnd;
        planConstraints = parse(planConstraintString);
        assertEquals(planConstraints.size(), 2);
        assertEquals(planConstraints.get(0), expectedCardinalityConstraint);
        assertEquals(planConstraints.get(1), expectedJoinConstraintWithPartitionedDistribution);

        planConstraintString = constraintsMarkerStart + "  join (a b) [R] card (foo 1000) card (foo 1000) join (a b) " + constraintsMarkerEnd;
        planConstraints = parse(planConstraintString);
        assertEquals(planConstraints.size(), 4);
        assertEquals(planConstraints.get(0), expectedJoinConstraintWithReplicatedDistribution);
        assertEquals(planConstraints.get(1), expectedCardinalityConstraint);
        assertEquals(planConstraints.get(2), expectedCardinalityConstraint);
        assertEquals(planConstraints.get(3), expectedJoinConstraint);

        planConstraintString = constraintsMarkerStart + "  join (a(b c)d) " + constraintsMarkerEnd;
        planConstraints = parse(planConstraintString);
        assertEquals(planConstraints.size(), 1);
        assertEquals(planConstraints.get(0), new JoinConstraint(INNER,
                Optional.empty(),
                ImmutableList.of(new RelationConstraint("A"),
                        new JoinConstraint(INNER,
                                Optional.empty(),
                                ImmutableList.of(new RelationConstraint("B"), new RelationConstraint("C"))),
                        new RelationConstraint("D"))));

        planConstraintString = constraintsMarkerStart + "  join (a(b c)[P]d)[R] " + constraintsMarkerEnd;
        planConstraints = parse(planConstraintString);
        assertEquals(planConstraints.size(), 1);
        assertEquals(planConstraints.get(0), new JoinConstraint(INNER,
                Optional.of(REPLICATED),
                ImmutableList.of(new RelationConstraint("A"),
                        new JoinConstraint(INNER,
                                Optional.of(PARTITIONED),
                                ImmutableList.of(new RelationConstraint("B"), new RelationConstraint("C"))),
                        new RelationConstraint("D"))));

        List<PlanConstraint> constraint = parse(constraintsMarkerStart + " join (aka_name (cast_info (company_name (movie_companies ((title role_type) name))))) " + constraintsMarkerEnd);
        assertEquals(constraint.size(), 1);
        assertEquals(constraint.get(0), new JoinConstraint(INNER,
                Optional.empty(),
                ImmutableList.of(new RelationConstraint("AKA_NAME"),
                        new JoinConstraint(INNER,
                                Optional.empty(),
                                ImmutableList.of(new RelationConstraint("CAST_INFO"),
                                        new JoinConstraint(INNER,
                                                Optional.empty(),
                                                ImmutableList.of(new RelationConstraint("COMPANY_NAME"),
                                                        new JoinConstraint(INNER,
                                                                Optional.empty(),
                                                                ImmutableList.of(new RelationConstraint("MOVIE_COMPANIES"),
                                                                        new JoinConstraint(INNER,
                                                                                Optional.empty(),
                                                                                ImmutableList.of(new JoinConstraint(INNER, Optional.empty(),
                                                                                                ImmutableList.of(new RelationConstraint("TITLE"), new RelationConstraint("ROLE_TYPE"))),
                                                                                        new RelationConstraint("NAME"))))))))))));
    }

    @Test
    public void testNestedPlanConstraints()
    {
        String planConstraintString = constraintsMarkerStart + " join (a (b c) d) " + constraintsMarkerEnd;
        List<PlanConstraint> planConstraints = parse(planConstraintString);
        assertEquals(planConstraints.size(), 1);
        assertEquals(planConstraints.get(0), new JoinConstraint(INNER,
                Optional.empty(),
                ImmutableList.of(new RelationConstraint("A"),
                        new JoinConstraint(INNER,
                                Optional.empty(),
                                ImmutableList.of(new RelationConstraint("B"), new RelationConstraint("C"))),
                        new RelationConstraint("D"))));

        planConstraintString = constraintsMarkerStart + " join (a (b c) [R] d) " + constraintsMarkerEnd;
        planConstraints = parse(planConstraintString);
        assertEquals(planConstraints.size(), 1);
        assertEquals(planConstraints.get(0), new JoinConstraint(INNER,
                Optional.empty(),
                ImmutableList.of(new RelationConstraint("A"),
                        new JoinConstraint(INNER,
                                Optional.of(REPLICATED),
                                ImmutableList.of(new RelationConstraint("B"), new RelationConstraint("C"))),
                        new RelationConstraint("D"))));

        planConstraintString = constraintsMarkerStart + " join ((a (b c) [R]) [P] d e) [P]" + constraintsMarkerEnd;
        planConstraints = parse(planConstraintString);
        assertEquals(planConstraints.size(), 1);
        assertEquals(planConstraints.get(0), new JoinConstraint(INNER,
                Optional.of(PARTITIONED),
                ImmutableList.of(new JoinConstraint(INNER,
                                Optional.of(PARTITIONED),
                                ImmutableList.of(new RelationConstraint("A"),
                                        new JoinConstraint(INNER,
                                                Optional.of(REPLICATED),
                                                ImmutableList.of(new RelationConstraint("B"), new RelationConstraint("C"))))),
                        new RelationConstraint("D"),
                        new RelationConstraint("E"))));

        planConstraintString = constraintsMarkerStart + " card ((a b c d) 10)" + constraintsMarkerEnd;
        planConstraints = parse(planConstraintString);
        assertEquals(planConstraints.size(), 1);
        assertEquals(planConstraints.get(0), new CardinalityConstraint(new JoinConstraint(INNER,
                Optional.empty(),
                ImmutableList.of(new RelationConstraint("A"), new RelationConstraint("B"), new RelationConstraint("C"), new RelationConstraint("D"))),
                new CardinalityEstimate(10)));

        planConstraintString = constraintsMarkerStart + " card ((a (b c) d) 10)" + constraintsMarkerEnd;
        planConstraints = parse(planConstraintString);
        assertEquals(planConstraints.size(), 1);
        assertEquals(planConstraints.get(0), new CardinalityConstraint(new JoinConstraint(INNER,
                Optional.empty(),
                ImmutableList.of(new RelationConstraint("A"),
                        new JoinConstraint(INNER,
                                Optional.empty(),
                                ImmutableList.of(new RelationConstraint("B"), new RelationConstraint("C"))),
                        new RelationConstraint("D"))),
                new CardinalityEstimate(10)));

        planConstraintString = constraintsMarkerStart + " card ((a (b c d)) 10)" + constraintsMarkerEnd;
        planConstraints = parse(planConstraintString);
        assertEquals(planConstraints.size(), 1);
        assertEquals(planConstraints.get(0), new CardinalityConstraint(new JoinConstraint(INNER,
                Optional.empty(),
                ImmutableList.of(new RelationConstraint("A"),
                        new JoinConstraint(INNER,
                                Optional.empty(),
                                ImmutableList.of(new RelationConstraint("B"), new RelationConstraint("C"), new RelationConstraint("D"))))),
                new CardinalityEstimate(10)));

        planConstraintString = constraintsMarkerStart + " join ((a (b c) [R]) [P] d e) [P] card ((a ((b c) d e)) 10)" + constraintsMarkerEnd;
        planConstraints = parse(planConstraintString);
        assertEquals(planConstraints.size(), 2);
        assertEquals(planConstraints.get(0), new JoinConstraint(INNER,
                Optional.of(PARTITIONED),
                ImmutableList.of(new JoinConstraint(INNER,
                                Optional.of(PARTITIONED),
                                ImmutableList.of(new RelationConstraint("A"),
                                        new JoinConstraint(INNER,
                                                Optional.of(REPLICATED),
                                                ImmutableList.of(new RelationConstraint("B"), new RelationConstraint("C"))))),
                        new RelationConstraint("D"),
                        new RelationConstraint("E"))));
        assertEquals(planConstraints.get(1), new CardinalityConstraint(new JoinConstraint(INNER,
                Optional.empty(),
                ImmutableList.of(new RelationConstraint("A"),
                        new JoinConstraint(INNER,
                                Optional.empty(),
                                ImmutableList.of(new JoinConstraint(INNER,
                                                Optional.empty(),
                                                ImmutableList.of(new RelationConstraint("B"), new RelationConstraint("C"))),
                                        new RelationConstraint("D"),
                                        new RelationConstraint("E"))))),
                new CardinalityEstimate(10)));
    }
}
