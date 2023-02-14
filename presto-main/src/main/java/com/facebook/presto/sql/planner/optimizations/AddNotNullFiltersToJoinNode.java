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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.Session;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.expressions.DefaultRowExpressionTraversalVisitor;
import com.facebook.presto.expressions.LogicalRowExpressions;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.PlanVariableAllocator;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.AND;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.IS_NULL;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.google.common.collect.Sets.intersection;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Stream.concat;

public class AddNotNullFiltersToJoinNode
        implements PlanOptimizer
{
    private final Metadata metadata;

    public AddNotNullFiltersToJoinNode(Metadata metadata)
    {
        this.metadata = metadata;
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider types, PlanVariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        if (!SystemSessionProperties.isOptimizeNullsInJoin(session)) {
            return plan;
        }

        requireNonNull(plan, "plan is null");
        requireNonNull(session, "session is null");
        requireNonNull(types, "types is null");
        requireNonNull(idAllocator, "idAllocator is null");

        return SimplePlanRewriter.rewriteWith(new Rewriter(metadata.getFunctionAndTypeManager(), idAllocator), plan);
    }

    private static class Rewriter
            extends SimplePlanRewriter<Void>
    {
        private final PlanNodeIdAllocator idAllocator;
        private final FunctionAndTypeManager functionAndTypeManager;

        public Rewriter(FunctionAndTypeManager functionAndTypeManager, PlanNodeIdAllocator idAllocator)
        {
            this.functionAndTypeManager = functionAndTypeManager;
            this.idAllocator = idAllocator;
        }

        private PlanNode newJoinNode(JoinNode currentJoinNode, PlanNode newLeft, PlanNode newRight, RowExpression newFilterExpression)
        {
            return new JoinNode(
                    currentJoinNode.getSourceLocation(),
                    idAllocator.getNextId(),
                    currentJoinNode.getType(),
                    newLeft,
                    newRight,
                    currentJoinNode.getCriteria(),
                    currentJoinNode.getOutputVariables(),
                    Optional.ofNullable(newFilterExpression),
                    currentJoinNode.getLeftHashVariable(),
                    currentJoinNode.getRightHashVariable(),
                    currentJoinNode.getDistributionType(),
                    currentJoinNode.getDynamicFilters());
        }

        private RowExpression buildNotNullRowExpression(Collection<VariableReferenceExpression> expressions)
        {
            List<CallExpression> isNotNullExpressions = expressions.stream()
                    .map(x -> new CallExpression(x.getSourceLocation(), "not",
                            functionAndTypeManager.lookupFunction("not", fromTypes(BOOLEAN)),
                            BOOLEAN,
                            singletonList(new SpecialFormExpression(x.getSourceLocation(), IS_NULL, BOOLEAN, x))))
                    .collect(toList());

            return LogicalRowExpressions.and(isNotNullExpressions);
        }

        private RowExpression and(RowExpression first, RowExpression second)
        {
            requireNonNull(second);
            if (first != null) {
                return LogicalRowExpressions.and(first, second);
            }
            return second;
        }

        @Override
        public PlanNode visitJoin(JoinNode joinNode, RewriteContext<Void> context)
        {
            List<JoinNode.EquiJoinClause> joinCriteria = joinNode.getCriteria();
            RowExpression joinFilter = joinNode.getFilter().orElse(null);
            PlanNode rewrittenLeft = context.rewrite(joinNode.getLeft());
            PlanNode rewrittenRight = context.rewrite(joinNode.getRight());

            Collection<VariableReferenceExpression> inferredNotNullsVars = Collections.emptyList();
            switch (joinNode.getType()) {
                case LEFT:
                    //NOT NULL can be inferred for the right-side variables
                    inferredNotNullsVars = extractNotNullVariables(joinCriteria, joinFilter, rewrittenRight.getOutputVariables());
                    break;
                case RIGHT:
                    //NOT NULL can be inferred for the left-side variables
                    inferredNotNullsVars = extractNotNullVariables(joinCriteria, joinFilter, rewrittenLeft.getOutputVariables());
                    break;
                case INNER:
                    //NOT NULL can be inferred for variables from both sides of the join
                    inferredNotNullsVars = extractNotNullVariables(joinCriteria, joinFilter, concat(rewrittenLeft.getOutputVariables().stream(),
                            rewrittenRight.getOutputVariables().stream()).collect(toList()));
                    break;
                case FULL:
                    //NOT NULL cannot be inferred
                    return newJoinNode(joinNode, rewrittenLeft, rewrittenRight, joinFilter);
            }

            if (inferredNotNullsVars.size() > 0) {
                RowExpression updatedJoinFilter = and(joinFilter, buildNotNullRowExpression(inferredNotNullsVars));
                return newJoinNode(joinNode, rewrittenLeft, rewrittenRight, updatedJoinFilter);
            }

            return newJoinNode(joinNode, rewrittenLeft, rewrittenRight, joinFilter);
        }

        private Collection<VariableReferenceExpression> extractNotNullVariables(List<JoinNode.EquiJoinClause> joinCriteria,
                RowExpression joinFilter,
                List<VariableReferenceExpression> candidates)
        {
            RowExpression combinedFilter = joinFilter;
            for (JoinNode.EquiJoinClause criteria : joinCriteria) {
                combinedFilter = and(combinedFilter, criteria.getLeft());
                combinedFilter = and(combinedFilter, criteria.getRight());
            }

            return intersection(new HashSet<>(candidates), inferNotNullVariables(combinedFilter));
        }

        private ImmutableSet<VariableReferenceExpression> inferNotNullVariables(RowExpression expression)
        {
            ImmutableSet.Builder<VariableReferenceExpression> builder = ImmutableSet.builder();
            expression.accept(new ExtractInferredNotNullVariablesVisitor(functionAndTypeManager), builder);
            return builder.build();
        }
    }

    @VisibleForTesting
    public static class ExtractInferredNotNullVariablesVisitor
            extends DefaultRowExpressionTraversalVisitor<ImmutableSet.Builder<VariableReferenceExpression>>
    {
        private final FunctionAndTypeManager functionAndTypeManager;

        public ExtractInferredNotNullVariablesVisitor(FunctionAndTypeManager functionAndTypeManager)
        {
            this.functionAndTypeManager = functionAndTypeManager;
        }

        @Override
        public Void visitCall(CallExpression call, ImmutableSet.Builder<VariableReferenceExpression> context)
        {
            final FunctionHandle functionHandle = call.getFunctionHandle();
            final FunctionMetadata functionMetadata = functionAndTypeManager.getFunctionMetadata(functionHandle);
            if (functionMetadata.isCalledOnNullInput()) {
                //Since this operatorType can operate on NULL inputs and return a valid value, we can't make NOT NULL inference on it's arguments
                return null;
            }

            return super.visitCall(call, context);
        }

        @Override
        public Void visitSpecialForm(SpecialFormExpression specialForm, ImmutableSet.Builder<VariableReferenceExpression> context)
        {
            SpecialFormExpression.Form form = specialForm.getForm();
            if (form == AND) {
                //Since all arguments of an AND expression must be NOT NULL for the AND to evaluate to true
                //We can make NOT NULL inferences on this expression's arguments
                return super.visitSpecialForm(specialForm, context);
            }
            //For all other SpecialForms e.g. OR, COALESCE, IS_NULL, CASE we don't make NOT NULL inferences
            return null;
        }

        @Override
        public Void visitVariableReference(VariableReferenceExpression variableReferenceExpression, ImmutableSet.Builder<VariableReferenceExpression> context)
        {
            context.add(variableReferenceExpression);
            return super.visitVariableReference(variableReferenceExpression, context);
        }
    }
}
