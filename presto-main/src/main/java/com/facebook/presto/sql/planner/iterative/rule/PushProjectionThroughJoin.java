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

import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.DeterminismEvaluator;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.VariablesExtractor;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.sql.planner.VariablesExtractor.extractUnique;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;

/**
 * Utility class for pushing projections through inner join so that joins are not separated
 * by a project node and can participate in cross join elimination or join reordering.
 */
public final class PushProjectionThroughJoin
{
    public static Optional<PlanNode> pushProjectionThroughJoin(
            ProjectNode projectNode,
            Lookup lookup,
            PlanNodeIdAllocator planNodeIdAllocator,
            DeterminismEvaluator determinismEvaluator,
            FunctionResolution functionResolution,
            TypeProvider types)
    {
        if (!projectNode.getAssignments().getExpressions().stream().allMatch(determinismEvaluator::isDeterministic)) {
            return Optional.empty();
        }

        PlanNode child = lookup.resolve(projectNode.getSource());
        if (!(child instanceof JoinNode)) {
            return Optional.empty();
        }

        JoinNode joinNode = (JoinNode) child;
        PlanNode leftChild = joinNode.getLeft();
        PlanNode rightChild = joinNode.getRight();

        if (joinNode.getType() != JoinNode.Type.INNER) {
            return Optional.empty();
        }

        Assignments.Builder leftAssignmentsBuilder = Assignments.builder();
        Assignments.Builder rightAssignmentsBuilder = Assignments.builder();
        for (Map.Entry<VariableReferenceExpression, RowExpression> assignment :
        projectNode.getAssignments().entrySet()) {
            RowExpression expression = assignment.getValue();


            Set<VariableReferenceExpression> symbols = extractUnique(expression);
            if (leftChild.getOutputVariables().containsAll(symbols)) {
                // expression is satisfied with left child symbols
                leftAssignmentsBuilder.put(assignment.getKey(), expression);
            }
            else if (rightChild.getOutputVariables().containsAll(symbols)) {
                // expression is satisfied with right child symbols
                rightAssignmentsBuilder.put(assignment.getKey(), expression);
            }
            else {
                // expression is using symbols from both join sides
                return Optional.empty();
            }
        }

        // add projections for symbols required by the join itself
        Set<VariableReferenceExpression> joinRequiredSymbols = getJoinRequiredSymbols(joinNode);
        for (VariableReferenceExpression requiredSymbol : joinRequiredSymbols) {
            if (leftChild.getOutputVariables().contains(requiredSymbol)) {
                leftAssignmentsBuilder.put(requiredSymbol, requiredSymbol.canonicalize());
            }
            else {
                checkState(rightChild.getOutputVariables().contains(requiredSymbol));
                rightAssignmentsBuilder.put(requiredSymbol, requiredSymbol.canonicalize());
            }
        }

        Assignments leftAssignments = leftAssignmentsBuilder.build();
        Assignments rightAssignments = rightAssignmentsBuilder.build();
        List<VariableReferenceExpression> leftOutputSymbols = leftAssignments.getOutputs().stream()
                .filter(ImmutableSet.copyOf(projectNode.getOutputVariables())::contains)
                .collect(toImmutableList());
        List<VariableReferenceExpression> rightOutputSymbols = rightAssignments.getOutputs().stream()
                .filter(ImmutableSet.copyOf(projectNode.getOutputVariables())::contains)
                .collect(toImmutableList());

        return Optional.of(new JoinNode(
                joinNode.getSourceLocation(),
                joinNode.getId(),
                joinNode.getType(),
                inlineProjections(
                        new ProjectNode(planNodeIdAllocator.getNextId(), leftChild, leftAssignments),
                        lookup,
                        types, functionResolution
                ),
                inlineProjections(
                        new ProjectNode(planNodeIdAllocator.getNextId(), rightChild, rightAssignments),
                        lookup,
                        types, functionResolution
                ),
                joinNode.getCriteria(),
                joinNode.getOutputVariables(),
                joinNode.getFilter(),
                joinNode.getLeftHashVariable(),
                joinNode.getRightHashVariable(),
                joinNode.getDistributionType(),
                joinNode.getDynamicFilters()));
    }

    private static PlanNode inlineProjections(
            ProjectNode parentProjection,
            Lookup lookup,
            TypeProvider types, FunctionResolution functionResolution)
    {
        PlanNode child = lookup.resolve(parentProjection.getSource());
        if (!(child instanceof ProjectNode)) {
            return parentProjection;
        }
        ProjectNode childProjection = (ProjectNode) child;

        return InlineProjections.applyInlineProjection(parentProjection, childProjection, types, functionResolution)
                .map(node -> inlineProjections(node, lookup, types, functionResolution))
                .orElse(parentProjection);
    }

    private static Set<VariableReferenceExpression> getJoinRequiredSymbols(JoinNode node)
    {
        // extract symbols required by the join itself
        return Streams.concat(
                node.getCriteria().stream().map(JoinNode.EquiJoinClause::getLeft),
                node.getCriteria().stream().map(JoinNode.EquiJoinClause::getRight),
                node.getFilter().map(VariablesExtractor::extractUnique).orElse(ImmutableSet.of()).stream(),
                node.getLeftHashVariable().map(ImmutableSet::of).orElse(ImmutableSet.of()).stream(),
                node.getRightHashVariable().map(ImmutableSet::of).orElse(ImmutableSet.of()).stream())
                .collect(toImmutableSet());
    }

    private PushProjectionThroughJoin() {}
}
