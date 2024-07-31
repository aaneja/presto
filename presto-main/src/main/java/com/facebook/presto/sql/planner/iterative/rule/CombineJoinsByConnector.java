package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.expressions.DefaultRowExpressionTraversalVisitor;
import com.facebook.presto.expressions.LogicalRowExpressions;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.JoinType;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.relation.DeterminismEvaluator;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.CanonicalJoinNode;
import com.facebook.presto.sql.planner.EqualityInference;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static com.facebook.presto.expressions.LogicalRowExpressions.and;
import static com.facebook.presto.expressions.LogicalRowExpressions.extractConjuncts;
import static com.facebook.presto.expressions.RowExpressionNodeInliner.replaceExpression;
import static com.facebook.presto.spi.plan.JoinType.INNER;
import static com.facebook.presto.sql.planner.VariablesExtractor.extractUnique;
import static com.facebook.presto.sql.planner.optimizations.JoinNodeUtils.toRowExpression;
import static com.facebook.presto.sql.planner.plan.AssignmentUtils.getNonIdentityAssignments;
import static com.facebook.presto.sql.planner.plan.Patterns.join;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

public class CombineJoinsByConnector
        implements Rule<JoinNode>
{
    private final Pattern<JoinNode> joinNodePattern;
    private final FunctionResolution functionResolution;
    private final DeterminismEvaluator determinismEvaluator;
    private final Logger LOG = Logger.get(CombineJoinsByConnector.class);
    private final Metadata metadata;

    @Override
    public Pattern<JoinNode> getPattern()
    {
        return joinNodePattern;
    }

    public CombineJoinsByConnector(Metadata metadata)
    {
        this.metadata = metadata;
        this.functionResolution = new FunctionResolution(metadata.getFunctionAndTypeManager().getFunctionAndTypeResolver());
        this.determinismEvaluator = new RowExpressionDeterminismEvaluator(metadata.getFunctionAndTypeManager());
        this.joinNodePattern = join().matching(
                joinNode -> !joinNode.getDistributionType().isPresent()
                        && joinNode.getType() == INNER);
    }

    @Override
    public Result apply(JoinNode joinNode, Captures captures, Context context)
    {
        Lookup lookup = context.getLookup();
        MultiJoinNode multiJoinNode = new JoinNodeFlattener(joinNode, lookup,
                true, functionResolution, determinismEvaluator).toMultiJoinNode();

        // Rewrite the multi join node by combining sources from the same JDBC connector as one source
        // Any remaining predicates are join predicates *between* these new sources
        MultiJoinNode rewrittenMultiJoinNode = joinPushdownCombineSources(multiJoinNode, lookup);

        PlanNode result = createLeftDeepJoinTree(rewrittenMultiJoinNode, context.getIdAllocator());

        return Result.empty();
    }

    /**
     * Creates a left deep Join tree of CrossJoins, with a FilterNode at the top
     * The final result then needs a predicate pushdown / EliminateCrossJoins pass for the equality criteria to be set
     *
     * @param multiJoinNode
     * @param idAllocator
     * @return
     */
    public static PlanNode createLeftDeepJoinTree(MultiJoinNode multiJoinNode, PlanNodeIdAllocator idAllocator)
    {
        PlanNode joinNode = createJoin(0, ImmutableList.copyOf(multiJoinNode.getSources()), idAllocator);
        return new FilterNode(Optional.empty(), idAllocator.getNextId(), joinNode, multiJoinNode.getFilter());
    }

    private static PlanNode createJoin(int index, List<PlanNode> sources, PlanNodeIdAllocator idAllocator)
    {
        if (index == sources.size() - 1) {
            return sources.get(index);
        }

        PlanNode leftNode = createJoin(index + 1, sources, idAllocator);
        PlanNode rightNode = sources.get(index);
        return new JoinNode(
                Optional.empty(),
                idAllocator.getNextId(),
                JoinType.INNER,
                leftNode,
                rightNode,
                ImmutableList.of(),
                ImmutableList.<VariableReferenceExpression>builder()
                        .addAll(leftNode.getOutputVariables())
                        .addAll(rightNode.getOutputVariables())
                        .build(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of());
    }

    private MultiJoinNode joinPushdownCombineSources(MultiJoinNode multiJoinNode, Lookup lookup)
    {
        ImmutableList.Builder<PlanNode> rewrittenSources = ImmutableList.builder();
        List<RowExpression> overallPredicates = new ArrayList<>(LogicalRowExpressions.extractConjuncts(multiJoinNode.getFilter()));
        EqualityInference filterEqualityInference = new EqualityInference.Builder(metadata)
                .addEqualityInference(multiJoinNode.getFilter())
                .build();
        Map<ConnectorId, List<PlanNode>> sourcesByConnector = new HashMap<>();

        for (PlanNode source : multiJoinNode.getSources()) {
            Optional<ConnectorId> connectorId = getConnectorOfSource(source, lookup);
            if (connectorId.isPresent()) {
                // This source can be combined with other 'sources' of the same connector to produce a single TableScanNode
                sourcesByConnector.computeIfAbsent(connectorId.get(), k -> new ArrayList<>());
                sourcesByConnector.get(connectorId.get()).add(source);
            }
            else {
                rewrittenSources.add(source);
            }
        }

        sourcesByConnector.forEach(((connectorId, planNodes) -> {
            List<PlanNode> newSources = getNewTableScanNode(connectorId, planNodes, filterEqualityInference);
            rewrittenSources.addAll(newSources);
        }));

        return new MultiJoinNode(
                new LinkedHashSet<>(rewrittenSources.build()),
                LogicalRowExpressions.and(overallPredicates),
                multiJoinNode.getOutputVariables(),
                multiJoinNode.getAssignments());
    }

    /**
     * Builds a new TableScan node from sources that will benefit from a join pushdown
     * For any other sources, do not push down the join
     * @param connectorId
     * @param nodesToCombine
     * @param filterEqualityInference
     * @return
     */
    private List<PlanNode> getNewTableScanNode(ConnectorId connectorId,
            List<PlanNode> nodesToCombine,
            EqualityInference filterEqualityInference)
    {

        // Build combined output variables
        Set<VariableReferenceExpression> combinedOutputVariables = nodesToCombine.stream()
                .flatMap(o -> o.getOutputVariables().stream())
                .collect(Collectors.toSet());

        ImmutableList.Builder<PlanNode> joinPushdownSources = ImmutableList.builder();
        ImmutableList.Builder<PlanNode> finalResultSources = ImmutableList.builder();

        RowExpression equiJoinFiltersForSources = LogicalRowExpressions.and(
                filterEqualityInference.generateEqualitiesPartitionedBy(combinedOutputVariables::contains)
                        .getScopeEqualities());

        // We need to remove any sources that do not have any equi-join filters at all
        Set<VariableReferenceExpression> referredVariables = extractVariableExpressions(equiJoinFiltersForSources);
        nodesToCombine.forEach(node-> {
            if (node.getOutputVariables().stream().anyMatch(referredVariables::contains)) {
                // At least one of the output variables of this node was involved in an equi join with another source
                // So there is a valid JOIN with one of the other sources
                joinPushdownSources.add(node);
            }
            else {
                finalResultSources.add(node);
            }
        });

        // At this point we should have
        // 1. All the table references that belong to the same connector AND can have a JOIN pushed down - these need to be combined to a TableScanNode
        // 2. All the equi-join predicates that refer to these tables
        // We can now build our new TableScanNode which represents the join pushed down
        List<PlanNode> joinedSources = joinPushdownSources.build();
        LOG.info("For Connector [%s], we could build a new TableScanNode with sources : %s %n" +
                        "Output variables : [%s]%n" +
                        "Equi join Predicates : [%s]",
                connectorId,
                joinedSources,
                combinedOutputVariables,
                equiJoinFiltersForSources);

        // Returning a FAKE ValuesNode for now
        PlanNode fakeTableScanNode = new ValuesNode(Optional.empty(),
                new PlanNodeId(connectorId.toString()),
                Collections.emptyList(),
                Collections.emptyList(),
                Optional.empty());

        // Add the new combined-table-scan node
        finalResultSources.add(fakeTableScanNode);
        return finalResultSources.build();
    }

    /**
     * For a join source, see if we can resolve it to JDBC TableScanNode. This will only happen iff
     * the parent hierarchy only contains {Project, Filter, TableScanNode}'s as the parent's
     *
     * @param source
     * @param lookup
     * @return
     */
    private Optional<ConnectorId> getConnectorOfSource(PlanNode source, Lookup lookup)
    {
        PlanNode resolved = lookup.resolve(source);
        if (resolved instanceof ProjectNode) {
            return getConnectorOfSource(((ProjectNode) resolved).getSource(), lookup);
        }
        // In this PoC code, we do not handle intermediate Filter nodes
        if (resolved instanceof TableScanNode) {
            return Optional.of(((TableScanNode) resolved).getTable().getConnectorId());
        }
        return Optional.empty();
    }

    private static Set<VariableReferenceExpression> extractVariableExpressions(RowExpression expression)
    {
        ImmutableSet.Builder<VariableReferenceExpression> builder = ImmutableSet.builder();
        expression.accept(new VariableReferenceBuilderVisitor(), builder);
        return builder.build();
    }

    private static class VariableReferenceBuilderVisitor
            extends DefaultRowExpressionTraversalVisitor<ImmutableSet.Builder<VariableReferenceExpression>>
    {
        @Override
        public Void visitVariableReference(
                VariableReferenceExpression variable,
                ImmutableSet.Builder<VariableReferenceExpression> builder)
        {
            builder.add(variable);
            return null;
        }
    }

    private static class JoinNodeFlattener
    {
        private final LinkedHashSet<PlanNode> sources = new LinkedHashSet<>();
        private final Assignments intermediateAssignments;
        private final boolean handleComplexEquiJoins;
        private List<RowExpression> filters = new ArrayList<>();
        private final List<VariableReferenceExpression> outputVariables;
        private final FunctionResolution functionResolution;
        private final DeterminismEvaluator determinismEvaluator;
        private final Lookup lookup;

        JoinNodeFlattener(JoinNode node, Lookup lookup, boolean handleComplexEquiJoins, FunctionResolution functionResolution,
                DeterminismEvaluator determinismEvaluator)
        {
            requireNonNull(node, "node is null");
            checkState(node.getType() == INNER, "join type must be INNER");
            this.outputVariables = node.getOutputVariables();
            this.lookup = requireNonNull(lookup, "lookup is null");
            this.functionResolution = requireNonNull(functionResolution, "functionResolution is null");
            this.determinismEvaluator = requireNonNull(determinismEvaluator, "determinismEvaluator is null");
            this.handleComplexEquiJoins = handleComplexEquiJoins;

            Map<VariableReferenceExpression, RowExpression> intermediateAssignments = new HashMap<>();
            flattenNode(node, intermediateAssignments);

            // We resolve the intermediate assignments to only inputs of the flattened join node
            ImmutableSet<VariableReferenceExpression> inputVariables = sources.stream().flatMap(s -> s.getOutputVariables().stream()).collect(toImmutableSet());
            this.intermediateAssignments = resolveAssignments(intermediateAssignments, inputVariables);
            rewriteFilterWithInlinedAssignments(this.intermediateAssignments);
        }

        private Assignments resolveAssignments(Map<VariableReferenceExpression, RowExpression> assignments, Set<VariableReferenceExpression> availableVariables)
        {
            HashSet<VariableReferenceExpression> resolvedVariables = new HashSet<>();
            ImmutableList.copyOf(assignments.keySet()).forEach(variable -> resolveVariable(variable, resolvedVariables, assignments, availableVariables));

            return Assignments.builder().putAll(assignments).build();
        }

        private void resolveVariable(VariableReferenceExpression variable, HashSet<VariableReferenceExpression> resolvedVariables, Map<VariableReferenceExpression,
                RowExpression> assignments, Set<VariableReferenceExpression> availableVariables)
        {
            RowExpression expression = assignments.get(variable);
            Sets.SetView<VariableReferenceExpression> variablesToResolve = Sets.difference(Sets.difference(extractUnique(expression), availableVariables), resolvedVariables);

            // Recursively resolve any unresolved variables
            variablesToResolve.forEach(variableToResolve -> resolveVariable(variableToResolve, resolvedVariables, assignments, availableVariables));

            // Modify the assignment for the variable : Replace it with the now resolved constituent variables
            assignments.put(variable, replaceExpression(expression, assignments));
            // Mark this variable as resolved
            resolvedVariables.add(variable);
        }

        private void rewriteFilterWithInlinedAssignments(Assignments assignments)
        {
            ImmutableList.Builder<RowExpression> modifiedFilters = ImmutableList.builder();
            filters.forEach(filter -> modifiedFilters.add(replaceExpression(filter, assignments.getMap())));
            filters = modifiedFilters.build();
        }

        private void flattenNode(PlanNode node, Map<VariableReferenceExpression, RowExpression> assignmentsBuilder)
        {
            PlanNode resolved = lookup.resolve(node);

            if (resolved instanceof ProjectNode) {
                ProjectNode projectNode = (ProjectNode) resolved;
                // A ProjectNode could be 'hiding' a join source by building an assignment of a complex equi-join criteria like `left.key = right1.key1 + right1.key2`
                // We open up the join space by tracking the assignments from this Project node; these will be inlined into the overall filters once we finish
                // traversing the join graph
                // We only do this if the ProjectNode assignments are deterministic
                if (handleComplexEquiJoins && lookup.resolve(projectNode.getSource()) instanceof JoinNode &&
                        projectNode.getAssignments().getExpressions().stream().allMatch(determinismEvaluator::isDeterministic)) {
                    // We keep track of only the non-identity assignments since these are the ones that will be inlined into the overall filters
                    assignmentsBuilder.putAll(getNonIdentityAssignments(projectNode.getAssignments()));
                    flattenNode(projectNode.getSource(), assignmentsBuilder);
                }
                else {
                    sources.add(node);
                }
                return;
            }

            if (!(resolved instanceof JoinNode)) {
                sources.add(node);
                return;
            }

            JoinNode joinNode = (JoinNode) resolved;
            if (joinNode.getType() != INNER || !determinismEvaluator.isDeterministic(joinNode.getFilter().orElse(TRUE_CONSTANT))) {
                sources.add(node);
                return;
            }

            flattenNode(joinNode.getLeft(), assignmentsBuilder);
            flattenNode(joinNode.getRight(), assignmentsBuilder);
            joinNode.getCriteria().stream()
                    .map(criteria -> toRowExpression(criteria, functionResolution))
                    .forEach(filters::add);
            joinNode.getFilter().ifPresent(filters::add);
        }

        MultiJoinNode toMultiJoinNode()
        {
            ImmutableSet<VariableReferenceExpression> inputVariables = sources.stream().flatMap(source -> source.getOutputVariables().stream()).collect(toImmutableSet());

            // We could have some output variables that were possibly generated from intermediate assignments
            // For each of these variables, use the intermediate assignments to replace this variable with the set of input variables it uses

            // Additionally, we build an overall set of assignments for the reordered Join node - this is used to add a wrapper Project over the updated output variables
            // We do this to satisfy the invariant that the rewritten Join node must produce the same output variables as the input Join node
            ImmutableSet.Builder<VariableReferenceExpression> updatedOutputVariables = ImmutableSet.builder();
            Assignments.Builder overallAssignments = Assignments.builder();
            boolean nonIdentityAssignmentsFound = false;

            for (VariableReferenceExpression outputVariable : outputVariables) {
                if (inputVariables.contains(outputVariable)) {
                    overallAssignments.put(outputVariable, outputVariable);
                    updatedOutputVariables.add(outputVariable);
                    continue;
                }

                checkState(intermediateAssignments.getMap().containsKey(outputVariable),
                        "Output variable [%s] not found in input variables or in intermediate assignments", outputVariable);
                nonIdentityAssignmentsFound = true;
                overallAssignments.put(outputVariable, intermediateAssignments.get(outputVariable));
                updatedOutputVariables.addAll(extractUnique(intermediateAssignments.get(outputVariable)));
            }

            return new MultiJoinNode(sources,
                    and(filters),
                    updatedOutputVariables.build().asList(),
                    nonIdentityAssignmentsFound ? overallAssignments.build() : Assignments.of());
        }
    }

    static class MultiJoinNode
    {
        private final CanonicalJoinNode node;
        private final Assignments assignments;

        public MultiJoinNode(LinkedHashSet<PlanNode> sources, RowExpression filter, List<VariableReferenceExpression> outputVariables,
                Assignments assignments)
        {
            checkArgument(sources.size() > 1, "sources size is <= 1");

            requireNonNull(sources, "sources is null");
            requireNonNull(filter, "filter is null");
            requireNonNull(outputVariables, "outputVariables is null");
            requireNonNull(assignments, "assignments is null");

            this.assignments = assignments;
            // Plan node id doesn't matter here as we don't use this in planner
            this.node = new CanonicalJoinNode(
                    new PlanNodeId(""),
                    sources.stream().collect(toImmutableList()),
                    INNER,
                    ImmutableSet.of(),
                    ImmutableSet.of(filter),
                    outputVariables);
        }

        public RowExpression getFilter()
        {
            return node.getFilters().stream().findAny().get();
        }

        public LinkedHashSet<PlanNode> getSources()
        {
            return new LinkedHashSet<>(node.getSources());
        }

        public List<VariableReferenceExpression> getOutputVariables()
        {
            return node.getOutputVariables();
        }

        public Assignments getAssignments()
        {
            return assignments;
        }

        public static MultiJoinNode.Builder builder()
        {
            return new MultiJoinNode.Builder();
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(getSources(), ImmutableSet.copyOf(extractConjuncts(getFilter())), getOutputVariables());
        }

        @Override
        public boolean equals(Object obj)
        {
            if (!(obj instanceof MultiJoinNode)) {
                return false;
            }

            MultiJoinNode other = (MultiJoinNode) obj;
            return getSources().equals(other.getSources())
                    && ImmutableSet.copyOf(extractConjuncts(getFilter())).equals(ImmutableSet.copyOf(extractConjuncts(other.getFilter())))
                    && getOutputVariables().equals(other.getOutputVariables())
                    && getAssignments().equals(other.getAssignments());
        }

        @Override
        public String toString()
        {
            return "MultiJoinNode{" +
                    "node=" + node +
                    ", assignments=" + assignments +
                    '}';
        }

        static class Builder
        {
            private List<PlanNode> sources;
            private RowExpression filter;
            private List<VariableReferenceExpression> outputVariables;
            private Assignments assignments = Assignments.of();

            public MultiJoinNode.Builder setSources(PlanNode... sources)
            {
                this.sources = ImmutableList.copyOf(sources);
                return this;
            }

            public MultiJoinNode.Builder setFilter(RowExpression filter)
            {
                this.filter = filter;
                return this;
            }

            public MultiJoinNode.Builder setAssignments(Assignments assignments)
            {
                this.assignments = assignments;
                return this;
            }

            public MultiJoinNode.Builder setOutputVariables(VariableReferenceExpression... outputVariables)
            {
                this.outputVariables = ImmutableList.copyOf(outputVariables);
                return this;
            }

            public MultiJoinNode build()
            {
                return new MultiJoinNode(new LinkedHashSet<>(sources), filter, outputVariables, assignments);
            }
        }
    }
}
