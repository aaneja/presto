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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.Session;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.expressions.LogicalRowExpressions;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.google.api.client.util.Joiner;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.IS_NULL;
import static com.facebook.presto.sql.planner.plan.Patterns.join;
import static java.util.Collections.singletonList;

public class AddNotNullFiltersOnJoins
        implements Rule<JoinNode>
{
    private static final Logger log = Logger.get(AddNotNullFiltersOnJoins.class);
    private static final Pattern<JoinNode> PATTERN = join();
    private final FunctionResolution functionResolution;

    public AddNotNullFiltersOnJoins(FunctionAndTypeManager functionAndTypeManager)
    {
        this.functionResolution = new FunctionResolution(functionAndTypeManager);
    }

    private static Result newJoinNodeResult(Context context, JoinNode joinNode, PlanNode left, PlanNode right)
    {
        final PlanNodeId newJoinId = context.getIdAllocator().getNextId();
        final JoinNode transformedJoinNode = new JoinNode(
                joinNode.getSourceLocation(),
                newJoinId,
                Optional.empty(),
                joinNode.getType(),
                left,
                right,
                joinNode.getCriteria(),
                joinNode.getOutputVariables(),
                joinNode.getFilter(),
                joinNode.getLeftHashVariable(),
                joinNode.getRightHashVariable(),
                joinNode.getDistributionType(),
                joinNode.getDynamicFilters(),
                Optional.of(true)
        );
        return Result.ofPlanNode(transformedJoinNode);
    }

    private RowExpression buildNotNullRowExpression(List<VariableReferenceExpression> expressions)
    {
        RowExpression[] isNotNullExpressions = expressions.stream()
                .map(x -> new CallExpression(x.getSourceLocation(), "not", functionResolution.notFunction(), BOOLEAN,
                        singletonList(new SpecialFormExpression(x.getSourceLocation(), IS_NULL, BOOLEAN, x))))
                .toArray(RowExpression[]::new);

        return LogicalRowExpressions.and(isNotNullExpressions);
    }

    @Override
    public Pattern<JoinNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return SystemSessionProperties.isOptimizeNullsInJoinv2(session);
    }

    @Override
    public Result apply(JoinNode joinNode, Captures captures, Context context)
    {
        if (joinNode.notNullPredicatesGenerated().isPresent() && joinNode.notNullPredicatesGenerated().get()) {
            return Result.empty();
        }

        final List<JoinNode.EquiJoinClause> criteria = joinNode.getCriteria();
        Optional<RowExpression> filters = joinNode.getFilter();

        log.info("[%s], criteria = [%s], filters = [%s]", joinNode.getType(), Joiner.on(',').join(criteria), filters.toString());

        final ArrayList<VariableReferenceExpression> leftExpressions = new ArrayList<>();
        final ArrayList<VariableReferenceExpression> rightExpressions = new ArrayList<>();
        for (JoinNode.EquiJoinClause clause : criteria) {
            leftExpressions.add(clause.getLeft());
            rightExpressions.add(clause.getRight());
        }

        FilterNode leftFilterNode, rightFilterNode;
        switch (joinNode.getType()) {
            case INNER:
                //Not Null can be added to both sides
                leftFilterNode = new FilterNode(joinNode.getSourceLocation(),
                        context.getIdAllocator().getNextId(),
                        joinNode.getLeft(),
                        buildNotNullRowExpression(leftExpressions));
                rightFilterNode = new FilterNode(joinNode.getSourceLocation(),
                        context.getIdAllocator().getNextId(),
                        joinNode.getRight(),
                        buildNotNullRowExpression(rightExpressions));

                return newJoinNodeResult(context, joinNode, leftFilterNode, rightFilterNode);
            case LEFT:
                //Right side not null filters can be added
                rightFilterNode = new FilterNode(joinNode.getSourceLocation(),
                        context.getIdAllocator().getNextId(),
                        joinNode.getRight(),
                        buildNotNullRowExpression(rightExpressions));

                return newJoinNodeResult(context, joinNode, joinNode.getLeft(), rightFilterNode);
            case RIGHT:
                //Left side not null filters can be added
                leftFilterNode = new FilterNode(joinNode.getSourceLocation(),
                        context.getIdAllocator().getNextId(),
                        joinNode.getLeft(),
                        buildNotNullRowExpression(leftExpressions));

                return newJoinNodeResult(context, joinNode, leftFilterNode, joinNode.getRight());
            case FULL:
                return Result.empty();
        }

        return Result.empty();
    }
}
