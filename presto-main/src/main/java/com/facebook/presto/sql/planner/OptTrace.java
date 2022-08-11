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
package com.facebook.presto.sql.planner;

import com.facebook.presto.cost.CostProvider;
import com.facebook.presto.cost.PlanCostEstimate;
import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.cost.StatsProvider;
import com.facebook.presto.server.BasicQueryInfo;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SourceLocation;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.Ordering;
import com.facebook.presto.spi.plan.OrderingScheme;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.GroupReference;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Memo;
import com.facebook.presto.sql.planner.optimizations.JoinNodeUtils;
import com.facebook.presto.sql.planner.optimizations.PreferredProperties;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.InternalPlanVisitor;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.tree.AliasedRelation;
import com.facebook.presto.sql.tree.DefaultExpressionTraversalVisitor;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NodeLocation;
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.Table;
import com.google.common.base.Joiner;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import javafx.util.Pair;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.lang.String.format;
import static java.util.Arrays.stream;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class OptTrace
{
    File file;
    FileWriter fileWriter;
    BufferedWriter bufferedWriter;
    int indent;
    int incrIndent;
    long uid;
    Lookup lookUp;
    Memo memo;
    Stack<Long> uidStack;
    BasicQueryInfo queryInfo;
    boolean outputQueryInfo;
    HashMap<String, Integer> traceIdMap;
    HashMap<String, Integer> joinIdMap;
    Integer minusOne;
    HashMap<PlanNode, Pair<String, String>> joinStringMap;
    int currentTraceId;
    int currentJoinId;
    ArrayList<JoinConstraintNode> joinConstraints;
    ArrayList<JoinNode> enumeratedJoins;
    HashMap<Integer, PruneReason> prunedJoinIds;
    ArrayList<Integer> validUnderConstraintsJoinIds;
    boolean traceIdsAssigned;
    HashMap<PlanNode, PreferredProperties> planNodeToPreferredPropertiesMap;
    HashMap<PlanNode, String> planNodeToLookUpStringMap;
    BiMap<String, String> locationToTableOrAliasNameMap;
    ArrayList<String> duplicateTableOrAliasNames;
    HashMap<PlanNode, JoinConstraintNode> planNodeToJoinConstraintMap;

    public OptTrace(String dirPath)
    {
        initialize(dirPath, null, null);
    }

    public OptTrace(String dirPath, Lookup lookUpParam, Memo memoParam)
    {
        initialize(dirPath, lookUpParam, memoParam);
    }

    private void initialize(String dirPath, Lookup lookUpParam, Memo memoParam)
    {
        requireNonNull(dirPath, "dirPath is null");

        indent = 0;
        incrIndent = 2;
        lookUp = lookUpParam;
        memo = memoParam;
        uid = 0;
        uidStack = new Stack<Long>();
        queryInfo = null;
        outputQueryInfo = true;
        traceIdMap = new HashMap<String, Integer>();
        joinIdMap = new HashMap<String, Integer>();
        minusOne = Integer.valueOf(-1);
        joinStringMap = new HashMap<PlanNode, Pair<String, String>>();
        currentTraceId = 0;
        currentJoinId = 0;
        joinConstraints = null;
        enumeratedJoins = new ArrayList<JoinNode>();
        prunedJoinIds = new HashMap<Integer, PruneReason>();
        validUnderConstraintsJoinIds = new ArrayList<Integer>();
        traceIdsAssigned = false;
        planNodeToPreferredPropertiesMap = new HashMap<PlanNode, PreferredProperties>();
        planNodeToLookUpStringMap = new HashMap<PlanNode, String>();
        planNodeToJoinConstraintMap = new HashMap<PlanNode, JoinConstraintNode>();
        locationToTableOrAliasNameMap = HashBiMap.create();
        duplicateTableOrAliasNames = new ArrayList<String>();

        Path path = Paths.get(dirPath);

        if (Files.exists(path)) {
            if (!dirPath.endsWith("/")) {
                dirPath = dirPath + "/";
            }

            String tryFileName = dirPath + "optTrace_0.txt";
            path = Paths.get(tryFileName);

            for (int i = 1; Files.exists(path); ++i) {
                tryFileName = dirPath + "optTrace_" + i + ".txt";
                path = Paths.get(tryFileName);
            }

            file = new File(tryFileName);

            try {
                fileWriter = new FileWriter(file, true);
                bufferedWriter = new BufferedWriter(fileWriter);
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        else {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, format("Optimizer trace directory does not exist : %s", dirPath));
        }
    }

    public void addPlanNodeToPreferredPropertiesMapping(PlanNode planNode, PreferredProperties preferredProperties)
    {
        requireNonNull(planNode, "plan node is null");
        requireNonNull(preferredProperties, "preferred properties is null");
        planNodeToPreferredPropertiesMap.put(planNode, preferredProperties);
    }

    public void addPrunedJoinId(Integer joinId, PruneReason reason)
    {
        requireNonNull(joinId, "join id is null");
        if (joinId == -1) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "join id not set");
        }

        boolean add;
        if (reason == PruneReason.CONSTRAINT) {
            add = true;
        }
        else if (prunedJoinIds.get(joinId) == null) {
            add = true;
        }
        else {
            add = false;
        }

        if (add && prunedJoinIds.get(joinId) == null) {
            prunedJoinIds.put(joinId, reason);
        }
    }

    public PruneReason isPruned(Integer joinId)
    {
        requireNonNull(joinId, "join id is null");
        if (joinId == -1) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "trace id not set");
        }

        return prunedJoinIds.get(joinId);
    }

    public boolean isValidUnderConstraints(Integer joinId)
    {
        requireNonNull(joinId, "join id is null");
        if (joinId == -1) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "trace id not set");
        }

        return validUnderConstraintsJoinIds.contains(joinId);
    }

    public void setIsValidUnderConstraints(Integer joinId)
    {
        requireNonNull(joinId, "join id is null");
        if (joinId == -1) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "trace id not set");
        }

        validUnderConstraintsJoinIds.add(joinId);
    }

    public void addEnumeratedJoin(PlanNode planNode)
    {
        requireNonNull(planNode, "join node is null");

        if (planNode instanceof JoinNode) {
            JoinNode joinNode = (JoinNode) planNode;
            if (!enumeratedJoins.contains(planNode)) {
                enumeratedJoins.add(joinNode);
            }
        }
        else if (planNode instanceof GroupReference) {
            GroupReference groupReference = (GroupReference) planNode;
            Stream<PlanNode> planNodes = lookUp.resolveGroup(groupReference);
            Optional<PlanNode> groupPlanNode = planNodes.findFirst();
            if (groupPlanNode.isPresent()) {
                PlanNode tmpNode = groupPlanNode.get();
                if (tmpNode instanceof JoinNode) {
                    if (!enumeratedJoins.contains(tmpNode)) {
                        enumeratedJoins.add((JoinNode) tmpNode);
                    }
                }
            }
        }
    }

    public BasicQueryInfo getQueryInfo()
    {
        return queryInfo;
    }

    public void setQueryInfo(BasicQueryInfo queryInfoParam)
    {
        queryInfo = queryInfoParam;
    }

    public void reinitialize(Lookup lookUpParam, Memo memoParam)
    {
        lookUp = lookUpParam;
        memo = memoParam;
    }

    public LinkedHashSet<TableScanNode> getTableScans(PlanNode planNode)
    {
        OptTraceContext optTraceContext = new OptTraceContext(this, VisitType.FIND_TABLES);
        planNode.accept(new OptTrace.OptTraceVisitor(), optTraceContext);

        return optTraceContext.tableScans;
    }

    public int getTableScanCnt(PlanNode planNode)
    {
        OptTraceContext optTraceContext = new OptTraceContext(this, VisitType.CNT_TABLES);
        planNode.accept(new OptTrace.OptTraceVisitor(), optTraceContext);

        return optTraceContext.tableCnt;
    }

    public int getJoinCnt(PlanNode planNode)
    {
        OptTraceContext optTraceContext = new OptTraceContext(this, VisitType.CNT_JOINS);
        planNode.accept(new OptTrace.OptTraceVisitor(), optTraceContext);

        return optTraceContext.joinCnt;
    }

    public Pair<String, String> getJoinStrings(PlanNode planNode)
    {
        Pair<String, String> joinStrings = this.joinStringMap.get(planNode);

        if (joinStrings == null) {
            OptTraceContext optTraceContext = new OptTraceContext(this, VisitType.BUILD_JOIN_STRING);
            planNode.accept(new OptTrace.OptTraceVisitor(), optTraceContext);
            String joinString = new String(optTraceContext.tableCnt + "-way " + optTraceContext.joinString);

            String joinConstraintString = joinConstraintString(planNode);

            joinStrings = new Pair<String, String>(joinString, joinConstraintString);
            joinStringMap.put(planNode, joinStrings);
        }

        return joinStrings;
    }

    public Lookup lookUp()
    {
        return lookUp;
    }

    public Memo memo()
    {
        return memo;
    }

    public Long nextUid()
    {
        ++uid;
        return uid;
    }

    public void begin(String msgString, Object... args)
    {
        uidStack.push(Long.valueOf(uid + 1));
        if (msgString != null) {
            String beginString = new String("BEGIN : " + msgString);

            msg(beginString, true, args);
        }

        indent += incrIndent;
    }

    private void buildPlanConstraints(String queryString)
            throws IOException
    {
        joinConstraints = null;
        int beginConstraintPos = queryString.indexOf("/*!");
        if (beginConstraintPos != -1) {
            joinConstraints = new ArrayList<JoinConstraintNode>();
            int endConstraintPos = queryString.indexOf("*/", beginConstraintPos);
            String constraintString = queryString.substring(beginConstraintPos + 3, endConstraintPos - 1);
            JoinConstraintNode.parse(constraintString, joinConstraints);
        }
    }

    public void queryInfo()
    {
        if (outputQueryInfo) {
            this.msg("Query info :", true);
            this.incrIndent(1);
            if (queryInfo != null) {
                this.msg("Query string (id %s) :", true, queryInfo.getQueryId().getId());
                this.msg("  %s", true, queryInfo.getQuery());

                try {
                    buildPlanConstraints(queryInfo.getQuery());
                }
                catch (IOException e) {
                    throw new PrestoException(GENERIC_INTERNAL_ERROR, "Invalid join constraint");
                }
            }
            else {
                this.msg("<null>", true);
            }

            if (joinConstraints != null) {
                this.msg("Constraints :", true);
                incrIndent(1);
                int cnt = 0;
                for (JoinConstraintNode joinConstraintNode : joinConstraints) {
                    this.msg("%d : %s) :", true, cnt, joinConstraintNode.joinConstraintString());
                }
                decrIndent(1);
            }

            this.decrIndent(1);
            outputQueryInfo = false;
        }
    }

    public void end(String msgString, Object... args)
    {
        indent -= incrIndent;

        if (indent < 0) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, format("Optimizer trace indent < 0"));
        }

        if (uidStack.empty()) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, format("Optimizer trace UID stack empty"));
        }

        Long beginUid = uidStack.pop();

        if (msgString != null) {
            String endString = new String("END (for UID " + beginUid + ") : " + msgString);

            msg(endString, true, args);
        }
    }

    public void assignTraceIds(PlanNode planNode, Statement statement)
    {
        begin("assignTraceIds");
        if (!traceIdsAssigned) {
            buildLocationToTableOrAliasNameMap(statement);
            assignTableScanTraceIds(planNode);

            if (joinConstraints != null) {
                for (JoinConstraintNode joinConstraintNode : joinConstraints) {
                    joinConstraintNode.setJoinIdsFromTableOrAliasName(joinIdMap, locationToTableOrAliasNameMap);
                }
            }

            begin("location-to-table/alias map");
            for (Map.Entry<String, String> entry : locationToTableOrAliasNameMap.entrySet()) {
                msg(entry.getKey() + " <=> " + entry.getValue(), true);
            }
            end("location-to-table/alias map");

            traceIdsAssigned = true;
        }

        end("assignTraceIds");
    }

    private class TableScanComparator
            implements Comparator<TableScanNode>
    {
        public int compare(TableScanNode tableScan1, TableScanNode tableScan2)
        {
            String str1 = tableScan1.getTable().getConnectorHandle().toString();
            String str2 = tableScan2.getTable().getConnectorHandle().toString();

            int cmp = str1.compareTo(str2);

            return cmp;
        }
    }

    private class PlanNodeTableScanCntComparator
            implements Comparator<PlanNode>
    {
        private OptTrace optTrace;

        public PlanNodeTableScanCntComparator(OptTrace optTraceParam)
        {
            requireNonNull(optTraceParam, "opt trace is null");
            optTrace = optTraceParam;
        }

        public int compare(PlanNode planNode1, PlanNode planNode2)
        {
            int cnt1 = optTrace.getTableScanCnt(planNode1);
            int cnt2 = optTrace.getTableScanCnt(planNode2);

            int cmp;

            cmp = cnt1 - cnt2;

            if (cmp == 0) {
                Integer traceId1 = optTrace.getTraceId(planNode1);
                Integer traceId2 = optTrace.getTraceId(planNode2);

                cmp = traceId1.intValue() - traceId2.intValue();
            }

            return cmp;
        }
    }

    private class TableScanNameLocationComparator
            implements Comparator<TableScanNode>
    {
        private OptTrace optTrace;

        public TableScanNameLocationComparator(OptTrace optTraceParam)
        {
            requireNonNull(optTraceParam, "opt trace is null");
            optTrace = optTraceParam;
        }

        public int compare(TableScanNode tableScanNode1, TableScanNode tableScanNode2)
        {
            String name1 = tableName(tableScanNode1, optTrace);
            String name2 = tableName(tableScanNode2, optTrace);

            int cmp = name1.compareTo(name2);

            if (cmp == 0) {
                if (tableScanNode1.getSourceLocation().isPresent() && tableScanNode2.getSourceLocation().isPresent()) {
                    name1 = name1 + sourceLocationToString(tableScanNode1.getSourceLocation().get());
                    name2 = name2 + sourceLocationToString(tableScanNode2.getSourceLocation().get());

                    cmp = name1.compareTo(name2);
                }
            }

            return cmp;
        }
    }

    private String tableScanLookUpString(TableScanNode tableScanNode)
    {
        String lookUpString = planNodeToLookUpStringMap.get(tableScanNode);

        if (lookUpString == null) {
            StringBuilder builder = new StringBuilder();

            if (tableScanNode.getSourceLocation().isPresent()) {
                builder.append(sourceLocationToString(tableScanNode.getSourceLocation().get()));
            }
            else {
                builder.append(tableScanNode.getId().getId());
            }

            lookUpString = builder.toString();

            planNodeToLookUpStringMap.put(tableScanNode, lookUpString);
        }

        return lookUpString;
    }

    private String getLookUpString(PlanNode planNode)
    {
        String lookUpString = planNodeToLookUpStringMap.get(planNode);

        if (lookUpString == null) {
            if (planNode instanceof TableScanNode) {
                lookUpString = getJoinId(planNode).toString();
            }
            else if (planNode instanceof GroupReference) {
                GroupReference groupReference = (GroupReference) planNode;
                Stream<PlanNode> planNodes = lookUp.resolveGroup(groupReference);
                Optional<PlanNode> groupPlanNode = planNodes.findFirst();
                if (groupPlanNode.isPresent()) {
                    PlanNode tmpNode = groupPlanNode.get();
                    lookUpString = getLookUpString(tmpNode);
                }
                else {
                    lookUpString = new String("Group Reference " + groupReference.getGroupId());
                }
            }
            else if (planNode instanceof JoinNode || planNode instanceof SemiJoinNode) {
                Pair<String, String> joinStrings = getJoinStrings(planNode);
                lookUpString = joinStrings.getValue();
            }
            else {
                StringBuilder builder = new StringBuilder(planNode.getClass().getSimpleName());

                if (planNode.getSources().size() > 1) {
                    builder.append("(");
                }

                int cnt = 0;
                for (PlanNode source : planNode.getSources()) {
                    if (cnt > 0) {
                        builder.append(" ");
                    }

                    builder.append("(");
                    builder.append(getLookUpString(source));

                    builder.append(")");
                    ++cnt;
                }

                if (planNode.getSources().size() > 1) {
                    builder.append(")");
                }

                lookUpString = builder.toString();
            }
        }

        return lookUpString;
    }

    public void assignTableScanTraceIds(PlanNode node)
    {
        begin("assignTableScanTraceIds");
        LinkedHashSet<TableScanNode> tableScans = getTableScans(node);

        if (tableScans != null) {
            TableScanNameLocationComparator compare = new TableScanNameLocationComparator(this);

            ArrayList<TableScanNode> tableScansArray = new ArrayList<>(tableScans);
            tableScansArray.sort(compare);

            begin("location-to-id map");

            StringBuilder builder = new StringBuilder();
            for (TableScanNode tableScanNode : tableScansArray) {
                String lookUpString = tableScanLookUpString(tableScanNode);
                int traceId = newTraceId(lookUpString);
                newJoinId(lookUpString);

                builder.append(lookUpString);
                builder.append(" => ");
                builder.append(traceId);

                msg(builder.toString(), true);

                builder.setLength(0);
            }

            end("location-to-id map");
        }

        end("assignTableScanTraceIds");
    }

    public Pair<ArrayList<Integer>, ArrayList<Integer>> traceIds(JoinNode joinNode)
    {
        return null;
    }

    private Integer newTraceId(String lookUpString)
    {
        requireNonNull(lookUpString, "lookup string is null");

        Integer traceId = traceIdMap.get(lookUpString);

        if (traceId == null) {
            traceId = Integer.valueOf(currentTraceId);
            traceIdMap.put(lookUpString, traceId);
            traceIdMap.put(traceId.toString(), traceId);
            ++currentTraceId;
        }
        else {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, format("trace id already assigned"));
        }

        return traceId;
    }

    private Integer newJoinId(String lookUpString)
    {
        requireNonNull(lookUpString, "lookup string is null");

        Integer joinId = joinIdMap.get(lookUpString);

        if (joinId == null) {
            joinId = Integer.valueOf(currentJoinId);
            joinIdMap.put(lookUpString, joinId);
            ++currentJoinId;
        }
        else {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, format("join id already assigned"));
        }

        return joinId;
    }

    public Integer getTraceId(PlanNode planNode)
    {
        Integer traceId = minusOne;
        String lookUpString;

        if (planNode instanceof TableScanNode) {
            lookUpString = tableScanLookUpString((TableScanNode) planNode);
        }
        else {
            lookUpString = getLookUpString(planNode);
        }

        traceId = traceIdMap.get(lookUpString);

        if (traceId == null) {
            traceId = Integer.valueOf(currentTraceId);
            traceIdMap.put(lookUpString, traceId);
            ++currentTraceId;
        }

        return traceId;
    }

    public Integer getJoinId(PlanNode planNode)
    {
        Integer joinId = minusOne;

        String lookUpString;

        if (planNode instanceof TableScanNode) {
            lookUpString = tableScanLookUpString((TableScanNode) planNode);
        }
        else {
            lookUpString = getLookUpString(planNode);
        }

        requireNonNull(lookUpString, "lookup string is null");

        joinId = joinIdMap.get(lookUpString);

        if (joinId == null) {
            joinId = Integer.valueOf(currentJoinId);
            joinIdMap.put(lookUpString, joinId);
            ++currentJoinId;
        }

        return joinId;
    }

    Integer getJoinId(String lookUpString)
    {
        Integer joinId = joinIdMap.get(lookUpString);
        return joinId;
    }

    public static void begin(Optional<OptTrace> optTraceParam, String msgString, Object... args)
    {
        optTraceParam.ifPresent(optTrace -> optTrace.begin(msgString, args));
    }

    public static void addPrunedJoinId(Optional<OptTrace> optTraceParam, Integer joinId, PruneReason reason)
    {
        optTraceParam.ifPresent(optTrace -> optTrace.addPrunedJoinId(joinId, reason));
    }

    public static void addPlanNodeToPreferredPropertiesMapping(Optional<OptTrace> optTraceParam, PlanNode planNode, PreferredProperties preferredProperties)
    {
        optTraceParam.ifPresent(optTrace -> optTrace.addPlanNodeToPreferredPropertiesMapping(planNode, preferredProperties));
    }

    public static void traceVariableReferenceExpressionList(Optional<OptTrace> optTraceParam, List<VariableReferenceExpression> varList, int indentCnt,
            String msgString, Object... args)
    {
        optTraceParam.ifPresent(optTrace -> optTrace.traceVariableReferenceExpressionList(varList, indentCnt, msgString, args));
    }

    public static void tracePreferredProperties(Optional<OptTrace> optTraceParam, PreferredProperties preferredProperties, PlanNode planNode, int indentCnt,
            String msgString, Object... args)
    {
        if (preferredProperties != null) {
            optTraceParam.ifPresent(optTrace -> optTrace.tracePreferredProperties(preferredProperties, indentCnt, msgString, args));
        }
    }

    public static PruneReason isPruned(Optional<OptTrace> optTraceParam, Integer traceId)
    {
        PruneReason pruned;
        if (optTraceParam.isPresent()) {
            OptTrace optTrace = optTraceParam.get();
            pruned = optTrace.isPruned(traceId);
        }
        else {
            pruned = null;
        }

        return pruned;
    }

    public static void queryInfo(Optional<OptTrace> optTraceParam)
    {
        optTraceParam.ifPresent(optTrace -> optTrace.queryInfo());
    }

    public static void addEnumeratedJoin(Optional<OptTrace> optTraceParam, PlanNode planNode)
    {
        optTraceParam.ifPresent(optTrace -> optTrace.addEnumeratedJoin(planNode));
    }

    public static void trace(Optional<OptTrace> optTraceParam, PlanNode planNode, int indentCnt, String msgString, Object... args)
    {
        optTraceParam.ifPresent(optTrace -> optTrace.tracePlanNode(planNode, indentCnt, msgString, args));
    }

    public static void traceEnumeratedJoins(Optional<OptTrace> optTraceParam, PlanNode root, CostProvider costProvider, StatsProvider statsProvider,
            int indentCnt, String msgString, Object... args)
    {
        optTraceParam.ifPresent(optTrace -> optTrace.traceEnumeratedJoins(root, costProvider, statsProvider, indentCnt, msgString, args));
    }

    public static void traceJoinConstraint(Optional<OptTrace> optTraceParam, PlanNode planNode, int indentCnt, String msgString, Object... args)
    {
        optTraceParam.ifPresent(optTrace -> optTrace.traceJoinConstraint(planNode, indentCnt, msgString, args));
    }

    public static void trace(Optional<OptTrace> optTraceParam, PlanCostEstimate planCostEstimate, int indentCnt, String msgString, Object... args)
    {
        optTraceParam.ifPresent(optTrace -> optTrace.tracePlanCostEstimate(planCostEstimate, indentCnt, msgString, args));
    }

    public static void msg(Optional<OptTrace> optTraceParam, String msgString, boolean eol, Object... args)
    {
        optTraceParam.ifPresent(optTrace -> optTrace.msg(msgString, eol, args));
    }

    public static void end(Optional<OptTrace> optTraceParam, String msgString, Object... args)
    {
        optTraceParam.ifPresent(optTrace -> optTrace.end(msgString, args));
    }

    public static void incrIndent(Optional<OptTrace> optTraceParam, int indentCnt)
    {
        optTraceParam.ifPresent(optTrace -> optTrace.incrIndent(indentCnt));
    }

    public static void decrIndent(Optional<OptTrace> optTraceParam, int indentCnt)
    {
        optTraceParam.ifPresent(optTrace -> optTrace.decrIndent(indentCnt));
    }

    public static void trace(Optional<OptTrace> optTraceParam, Set<Integer> partition, String msgString, Object... args)
    {
        optTraceParam.ifPresent(optTrace -> optTrace.tracePartition(partition, msgString, args));
    }

    public static void trace(Optional<OptTrace> optTraceParam, Set<PlanNode> sources, int indentCnt, String msgString, Object... args)
    {
        optTraceParam.ifPresent(optTrace -> optTrace.traceJoinSources(sources, indentCnt, msgString, args));
    }

    public String joinConstraintString(PlanNode planNode)
    {
        JoinConstraintNode joinConstraintNode = joinConstraintNode(planNode);
        String joinConstraintString;
        if (joinConstraintNode != null) {
            joinConstraintString = joinConstraintNode.joinConstraintString();
        }
        else {
            joinConstraintString = null;
        }

        return joinConstraintString;
    }

    private JoinConstraintNode joinConstraintNode(PlanNode planNode)
    {
        JoinConstraintNode joinConstraintNode = planNodeToJoinConstraintMap.get(planNode);

        if (joinConstraintNode == null) {
            if (planNode instanceof GroupReference) {
                GroupReference groupReference = (GroupReference) planNode;
                Stream<PlanNode> planNodes = lookUp.resolveGroup(groupReference);
                Optional<PlanNode> groupPlanNode = planNodes.findFirst();
                if (groupPlanNode.isPresent()) {
                    PlanNode tmpNode = groupPlanNode.get();
                    joinConstraintNode = joinConstraintNode(tmpNode);
                }
                else {
                    joinConstraintNode = new JoinConstraintNode(getJoinId(groupReference));
                }
            }
            else {
                if (planNode instanceof TableScanNode) {
                    TableScanNode tableScanNode = (TableScanNode) planNode;
                    Integer joinId = getJoinId(planNode);

                    String tableOrAliasName;

                    if (tableScanNode.getSourceLocation().isPresent()) {
                        tableOrAliasName = locationToTableOrAliasNameMap.get(sourceLocationToString(tableScanNode.getSourceLocation().get()));
                    }
                    else {
                        tableOrAliasName = null;
                    }

                    joinConstraintNode = new JoinConstraintNode(tableScanNode, joinId, tableOrAliasName);
                }
                else if (planNode instanceof JoinNode) {
                    JoinNode joinNode = (JoinNode) planNode;
                    joinConstraintNode = new JoinConstraintNode(joinNode);
                    JoinConstraintNode childJoinConstraintNode = joinConstraintNode(joinNode.getLeft());
                    joinConstraintNode.appendChild(childJoinConstraintNode);
                    childJoinConstraintNode = joinConstraintNode(joinNode.getRight());
                    joinConstraintNode.appendChild(childJoinConstraintNode);

                    String joinConstraintString = joinConstraintNode.joinConstraintString();
                    Integer joinId = joinIdMap.get(joinConstraintString);

                    if (joinId == null) {
                        joinId = newJoinId(joinConstraintString);
                    }

                    joinConstraintNode.setJoinId(joinId);
                }
                else if (planNode instanceof SemiJoinNode) {
                    SemiJoinNode semiJoinNode = (SemiJoinNode) planNode;
                    joinConstraintNode = new JoinConstraintNode(semiJoinNode);
                    JoinConstraintNode childJoinConstraintNode = joinConstraintNode(semiJoinNode.getProbe());
                    joinConstraintNode.appendChild(childJoinConstraintNode);
                    childJoinConstraintNode = joinConstraintNode(semiJoinNode.getBuild());
                    joinConstraintNode.appendChild(childJoinConstraintNode);
                    String joinConstraintString = joinConstraintNode.joinConstraintString();
                    Integer joinId = joinIdMap.get(joinConstraintString);

                    if (joinId == null) {
                        joinId = newJoinId(joinConstraintString);
                    }

                    joinConstraintNode.setJoinId(joinId);
                }
                else if (planNode.getSources().size() > 1) {
                    joinConstraintNode = new JoinConstraintNode();
                    for (PlanNode childPlanNode : planNode.getSources()) {
                        JoinConstraintNode childJoinConstraintNode = joinConstraintNode(childPlanNode);
                        joinConstraintNode.appendChild(childJoinConstraintNode);
                    }
                }
                else {
                    joinConstraintNode = new JoinConstraintNode(getJoinId(planNode));
                }
            }

            planNodeToJoinConstraintMap.put(planNode, joinConstraintNode);
        }

        return joinConstraintNode;
    }

    public static JoinConstraintNode joinConstraintNode(Optional<OptTrace> optTraceParam, JoinNode joinNode)
    {
        JoinConstraintNode joinConstraintNode = null;
        if (optTraceParam.isPresent()) {
            joinConstraintNode = optTraceParam.get().joinConstraintNode(joinNode);
        }

        return joinConstraintNode;
    }

    public static JoinConstraintNode joinConstraintNode(Optional<OptTrace> optTraceParam, SemiJoinNode semiJoinNode)
    {
        JoinConstraintNode joinConstraintNode = null;
        if (optTraceParam.isPresent()) {
            joinConstraintNode = optTraceParam.get().joinConstraintNode(semiJoinNode);
        }

        return joinConstraintNode;
    }

    public static void assignTraceIds(Optional<OptTrace> optTraceParam, PlanNode node, Statement statement)
    {
        optTraceParam.ifPresent(optTrace -> optTrace.assignTraceIds(node, statement));
    }

    public boolean satisfiesAnyJoinConstraint(PlanNode planNode)
    {
        requireNonNull(planNode, "plan node is null");
        boolean satifiesAny = false;

        if (joinConstraints != null && joinConstraints.size() > 0 &&
                (planNode instanceof JoinNode || planNode instanceof SemiJoinNode || planNode instanceof GroupReference
                        || planNode instanceof TableScanNode)) {
            JoinConstraintNode joinConstraintNode = joinConstraintNode(planNode);
            requireNonNull(joinConstraintNode, "join constraint is null");

            satifiesAny = joinConstraintNode.satisfiesAnyConstraint(joinConstraints, this);
        }

        return satifiesAny;
    }

    public static boolean constraintsPresent(Optional<OptTrace> optTraceParam)
    {
        boolean constraintsPresent = false;
        if (optTraceParam.isPresent()) {
            constraintsPresent = (optTraceParam.get().joinConstraints != null && optTraceParam.get().joinConstraints.size() > 0);
        }

        return constraintsPresent;
    }

    public static boolean satisfiesAnyJoinConstraint(Optional<OptTrace> optTraceParam, PlanNode planNode)
    {
        boolean satifiesAny = false;
        if (optTraceParam.isPresent()) {
            satifiesAny = optTraceParam.get().satisfiesAnyJoinConstraint(planNode);
        }

        return satifiesAny;
    }

    public static boolean valid(Optional<OptTrace> optTraceParam, PlanNode planNode)
    {
        boolean valid;

        if (optTraceParam.isPresent()) {
            OptTrace optTrace = optTraceParam.get();

            Integer joinId = optTrace.getJoinId(planNode);

            PruneReason reason = optTrace.isPruned(joinId);
            if (reason != null) {
                valid = false;
            }
            else if (optTrace.isValidUnderConstraints(joinId)) {
                valid = true;
            }
            else if (optTrace.joinConstraints != null && optTrace.joinConstraints.size() > 0) {
                JoinConstraintNode joinConstraintNode = optTrace.joinConstraintNode(planNode);
                valid = joinConstraintNode.joinIsValid(optTrace.joinConstraints, optTrace);

                if (valid) {
                    optTrace.setIsValidUnderConstraints(joinId);
                }
                else {
                    reason = PruneReason.CONSTRAINT;
                    optTrace.addPrunedJoinId(joinId, reason);
                }
            }
            else {
                valid = true;
            }

            if (reason != null) {
                Pair<String, String> joinStrings = optTrace.getJoinStrings(planNode);
                requireNonNull(joinStrings, "join strings are null");
                requireNonNull(joinStrings.getKey(), "join string is null");
                optTrace.msg("Join not valid (** PRUNED BY %s **) : (%s , join id %d)",
                        true, reason.getString().toUpperCase(), joinStrings.getKey(), joinId.intValue());
            }
        }
        else {
            valid = true;
        }

        return valid;
    }

    public enum VisitType
    {
        NONE, PRINT, FIND_TABLES, BUILD_JOIN_STRING, CNT_TABLES, CNT_JOINS
    }

    public enum PruneReason
    {
        COST("cost"),
        CONSTRAINT("constraint");

        private String string;

        PruneReason(String stringRep)
        {
            this.string = stringRep;
        }

        public String getString()
        {
            return string;
        }
    }

    private class OptTraceContext
    {
        private OptTrace optTrace;
        LinkedHashSet<TableScanNode> tableScans;
        VisitType visitType;
        String joinString;
        int tableCnt;
        int joinCnt;

        public OptTraceContext(OptTrace optTraceParam, VisitType visitTypeParam)
        {
            requireNonNull(optTraceParam, "trace is null");
            optTrace = optTraceParam;
            visitType = visitTypeParam;
            joinString = null;
            tableCnt = 0;
            joinCnt = 0;
        }

        public void clearTableScans()
        {
            if (tableScans != null) {
                tableScans.clear();
            }
        }

        public void clearVisitType()
        {
            visitType = null;
        }

        public void clear()
        {
            clearVisitType();
            clearTableScans();
            joinString = null;
            tableCnt = 0;
        }

        public OptTrace optTrace()
        {
            return optTrace;
        }

        public void setVisitType(VisitType visitTypeParam)
        {
            visitType = visitTypeParam;
        }

        public void addTableScan(TableScanNode tableScanNode)
        {
            if (tableScans == null) {
                tableScans = new LinkedHashSet<TableScanNode>();
            }

            tableScans.add(tableScanNode);
        }

        LinkedHashSet<TableScanNode> tableScans()
        {
            return tableScans;
        }
    }

    public String tableScansToString(LinkedHashSet<TableScanNode> tableScans)
    {
        requireNonNull(tableScans, "tableScans is null");
        List<String> nameList = new ArrayList<String>();
        for (TableScanNode tableScan : tableScans) {
            String name = tableName(tableScan, this);
            nameList.add(name);
        }

        Collections.sort(nameList);

        StringBuilder builder = new StringBuilder("(");

        if (nameList.size() > 1) {
            builder = new StringBuilder("Join(");
        }
        else {
            builder = new StringBuilder("(");
        }

        boolean first = true;
        for (String name : nameList) {
            if (!first) {
                builder.append(" ");
            }

            builder.append(name);

            first = false;
        }

        builder.append(")");

        return builder.toString();
    }

    public void buildLocationToTableOrAliasNameMap(Node node)
    {
        if (node != null) {
            new OptTraceAstVisitor(this).process(node);
        }
    }

    private static String nodeLocation(Node node)
    {
        String locationString;
        if (node.getLocation().isPresent()) {
            locationString = nodeLocationToString(node.getLocation().get());
        }
        else {
            locationString = null;
        }

        return locationString;
    }

    private static String nodeLocationToString(NodeLocation location)
    {
        String locationString = new String(location.getLineNumber() + ":" + location.getColumnNumber());

        return locationString;
    }

    private static String sourceLocationToString(SourceLocation location)
    {
        String locationString = new String(location.getLine() + ":" + location.getColumn());

        return locationString;
    }

    private static class OptTraceAstVisitor
            extends DefaultExpressionTraversalVisitor<Node, Void>
    {
        private OptTrace optTrace;

        OptTraceAstVisitor(OptTrace optTraceParam)
        {
            requireNonNull(optTraceParam, "null opt trace");
            optTrace = optTraceParam;
        }

        @Override
        protected Node visitRelation(Relation node, Void context)
        {
            if (node instanceof Table) {
                Table table = (Table) node;
                String tableName = table.getName().toString();
                if (optTrace.locationToTableOrAliasNameMap.inverse().get(tableName) != null) {
                    optTrace.locationToTableOrAliasNameMap.inverse().remove(tableName);
                    optTrace.duplicateTableOrAliasNames.add(tableName);
                }
                else if (!optTrace.duplicateTableOrAliasNames.contains(tableName) && node.getLocation().isPresent()) {
                    optTrace.locationToTableOrAliasNameMap.put(nodeLocation(node), tableName);
                }
            }

            return visitNode(node, context);
        }

        @Override
        protected Node visitAliasedRelation(AliasedRelation node, Void context)
        {
            requireNonNull(node.getAlias(), "alias is null");

            if (node.getRelation() instanceof Table) {
                String aliasName = node.getAlias().getValue();
                if (optTrace.locationToTableOrAliasNameMap.inverse().containsValue(aliasName)) {
                    optTrace.locationToTableOrAliasNameMap.inverse().remove(aliasName);
                    optTrace.duplicateTableOrAliasNames.add(aliasName);
                }
                else if (!optTrace.duplicateTableOrAliasNames.contains(aliasName) && node.getLocation().isPresent()) {
                    optTrace.locationToTableOrAliasNameMap.put(nodeLocation(node), aliasName);
                }

                return null;
            }

            return process(node.getRelation(), context);
        }
    }

    private static class OptTraceVisitor
            extends InternalPlanVisitor<PlanNode, OptTraceContext>
    {
        public OptTraceVisitor()
        {
        }

        @Override
        public PlanNode visitPlan(PlanNode planNode, OptTraceContext optTraceContext)
        {
            OptTrace optTrace = optTraceContext.optTrace();

            switch (optTraceContext.visitType) {
                case PRINT: {
                    optTrace.msg(planNode.getClass().getSimpleName() + " " + "(node id " + planNode.getId()
                            + ")", true);

                    optTrace.traceVariableReferenceExpressionList(planNode.getOutputVariables(), 1, "Output variables :");
                    optTrace.tracePreferredPropertiesForPlanNode(planNode, 1, "Preferred properties :");

                    int childId = 0;
                    for (PlanNode child : planNode.getSources()) {
                        optTrace.incrIndent(1);
                        optTrace.msg("Child %d :", true, childId);
                        optTrace.incrIndent(1);
                        child.accept(this, optTraceContext);
                        optTrace.decrIndent(2);
                        ++childId;
                    }

                    break;
                }

                case BUILD_JOIN_STRING: {
                    if (planNode.getSources().size() > 1) {
                        int childId = 0;
                        for (PlanNode child : planNode.getSources()) {
                            if (optTraceContext.joinString == null) {
                                optTraceContext.joinString = new String();
                            }

                            if (childId == 0) {
                                optTraceContext.joinString = optTraceContext.joinString + "(";
                            }
                            else {
                                optTraceContext.joinString = optTraceContext.joinString + " ";
                            }

                            child.accept(this, optTraceContext);
                            ++childId;
                        }

                        if (childId > 0) {
                            optTraceContext.joinString = optTraceContext.joinString + ")";
                        }
                    }
                    else if (planNode.getSources().size() == 0) {
                        Integer traceId = optTrace.getTraceId(planNode);
                        if (optTraceContext.joinString == null) {
                            optTraceContext.joinString = new String();
                        }

                        optTraceContext.joinString = optTraceContext.joinString + traceId;
                    }
                    else {
                        for (PlanNode child : planNode.getSources()) {
                            child.accept(this, optTraceContext);
                        }
                    }

                    break;
                }

                default: {
                    for (PlanNode child : planNode.getSources()) {
                        child.accept(this, optTraceContext);
                    }
                }
            }

            return null;
        }

        @Override
        public PlanNode visitGroupReference(GroupReference groupReference, OptTraceContext optTraceContext)
        {
            OptTrace optTrace = optTraceContext.optTrace();

            requireNonNull(optTrace.lookUp(), "loopUp is null");
            Stream<PlanNode> planNodes = optTrace.lookUp().resolveGroup(groupReference);

            switch (optTraceContext.visitType) {
                case PRINT: {
                    int groupId = groupReference.getGroupId();

                    optTrace.msg("Group id " + groupId + " : ", true);

                    AtomicInteger count = new AtomicInteger(-1);
                    planNodes.forEach(member -> {
                        optTrace.incrIndent(1);
                        optTrace.msg("Member %d :", true, count.incrementAndGet());
                        optTrace.incrIndent(1);
                        member.accept(this, optTraceContext);
                        optTrace.decrIndent(2);
                    });

                    break;
                }

                default: {
                    planNodes.forEach(member -> {
                        member.accept(this, optTraceContext);
                    });

                    break;
                }
            }

            return null;
        }

        @Override
        public PlanNode visitProject(ProjectNode projectNode, OptTraceContext optTraceContext)
        {
            OptTrace optTrace = optTraceContext.optTrace();

            switch (optTraceContext.visitType) {
                case PRINT: {
                    optTrace.msg("Project (node id " + projectNode.getId()
                            + ")", true);

                    optTrace.incrIndent(1);
                    optTrace.traceVariableReferenceExpressionList(projectNode.getOutputVariables(), 0, "Output variables :");
                    optTrace.tracePreferredPropertiesForPlanNode(projectNode, 0, "Preferred properties :");

                    Assignments assignments = projectNode.getAssignments();
                    Map<VariableReferenceExpression, RowExpression> assignmentMap = assignments.getMap();
                    optTrace.msg("Assignments :", true);
                    optTrace.incrIndent(1);
                    int cnt = 0;
                    for (Map.Entry<VariableReferenceExpression, RowExpression> assignmentMapEntry : assignmentMap.entrySet()) {
                        optTrace.msg("%d : %s => %s", true, cnt, assignmentMapEntry.getKey().getName(),
                                assignmentMapEntry.getValue().toString());
                        ++cnt;
                    }
                    optTrace.decrIndent(1);

                    optTrace.msg("Locality : %s", true, projectNode.getLocality());

                    optTrace.decrIndent(1);

                    cnt = 0;
                    optTrace.incrIndent(1);
                    for (PlanNode child : projectNode.getSources()) {
                        optTrace.msg("Child %d :", true, cnt);
                        optTrace.incrIndent(1);
                        child.accept(this, optTraceContext);
                        optTrace.decrIndent(1);
                        ++cnt;
                    }
                    optTrace.decrIndent(1);

                    break;
                }

                default: {
                    for (PlanNode child : projectNode.getSources()) {
                        child.accept(this, optTraceContext);
                    }

                    break;
                }
            }

            return null;
        }

        @Override
        public PlanNode visitFilter(FilterNode filterNode, OptTraceContext optTraceContext)
        {
            OptTrace optTrace = optTraceContext.optTrace();

            switch (optTraceContext.visitType) {
                case PRINT: {
                    optTrace.msg("Filter (node id " + filterNode.getId()
                            + ")", true);

                    optTrace.traceVariableReferenceExpressionList(filterNode.getOutputVariables(), 1, "Output variables :");
                    optTrace.tracePreferredPropertiesForPlanNode(filterNode, 1, "Preferred properties :");

                    optTrace.incrIndent(1);
                    optTrace.msg("Predicate :", true);
                    optTrace.incrIndent(1);
                    optTrace.msg(filterNode.getPredicate().toString(), true, null);
                    optTrace.decrIndent(2);

                    int cnt = 0;
                    optTrace.incrIndent(1);
                    for (PlanNode child : filterNode.getSources()) {
                        optTrace.msg("Child %d :", true, cnt);
                        optTrace.incrIndent(1);
                        child.accept(this, optTraceContext);
                        optTrace.decrIndent(1);
                        ++cnt;
                    }
                    optTrace.decrIndent(1);

                    break;
                }

                default: {
                    for (PlanNode child : filterNode.getSources()) {
                        child.accept(this, optTraceContext);
                    }

                    break;
                }
            }

            return null;
        }

        @Override
        public PlanNode visitExchange(ExchangeNode exchange, OptTraceContext optTraceContext)
        {
            OptTrace optTrace = optTraceContext.optTrace();

            switch (optTraceContext.visitType) {
                case PRINT: {
                    ExchangeNode.Type type = exchange.getType();
                    ExchangeNode.Scope scope = exchange.getScope();
                    PartitioningScheme partitioningScheme = exchange.getPartitioningScheme();

                    optTrace.msg("ExchangeNode[%s] (node id %s)", true, exchange.getType(), exchange.getId());
                    optTrace.traceVariableReferenceExpressionList(exchange.getOutputVariables(), 1, "Output variables :");
                    optTrace.tracePreferredPropertiesForPlanNode(exchange, 1, "Preferred properties :");
                    optTrace.tracePartitioningScheme(exchange.getPartitioningScheme(), 1, "Partitioning scheme :");
                    optTrace.traceOrderingScheme(exchange.getOrderingScheme(), 1, "Ordering scheme :");
                    optTrace.incrIndent(1);

                    optTrace.msg("Inputs :", true);
                    optTrace.incrIndent(1);
                    int cnt = 0;
                    for (List<VariableReferenceExpression> input : exchange.getInputs()) {
                        optTrace.msg("%d : ", true, cnt);
                        optTrace.traceVariableReferenceExpressionList(input, 1, null);
                    }
                    optTrace.decrIndent(1);

                    optTrace.msg("Scope : %s", true, scope.toString());
                    optTrace.msg("Ensure source ordering? : %s", true, exchange.isEnsureSourceOrdering());
                    optTrace.decrIndent(1);

                    int childId = 0;
                    for (PlanNode child : exchange.getSources()) {
                        optTrace.incrIndent(1);
                        optTrace.msg("Child %d :", true, childId);
                        optTrace.incrIndent(1);
                        child.accept(this, optTraceContext);
                        optTrace.decrIndent(2);
                        ++childId;
                    }

                    break;
                }

                default: {
                    for (PlanNode child : exchange.getSources()) {
                        child.accept(this, optTraceContext);
                    }
                }
            }

            return null;
        }

        private static String formatHash(Optional<VariableReferenceExpression>... hashes)
        {
            List<VariableReferenceExpression> variables = stream(hashes)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .collect(toList());

            if (variables.isEmpty()) {
                return "<empty>";
            }

            return "[" + Joiner.on(", ").join(variables) + "]";
        }

        @Override
        public PlanNode visitJoin(JoinNode join, OptTraceContext optTraceContext)
        {
            OptTrace optTrace = optTraceContext.optTrace();

            switch (optTraceContext.visitType) {
                case PRINT: {
                    Integer traceId = optTrace.getTraceId(join);
                    Integer joinId = optTrace.getJoinId(join);

                    List<Expression> joinExpressions = new ArrayList<>();
                    for (JoinNode.EquiJoinClause clause : join.getCriteria()) {
                        joinExpressions.add(JoinNodeUtils.toExpression(clause));
                    }

                    String criteria = Joiner.on(" AND ").join(joinExpressions);

                    Pair<String, String> joinStrings = optTrace.getJoinStrings(join);
                    requireNonNull(joinStrings, "join strings are null");

                    optTrace.msg(join.getType().getJoinLabel() + " (node id " + join.getId() + ")", true);
                    optTrace.traceVariableReferenceExpressionList(join.getOutputVariables(), 1, "Output variables :");
                    optTrace.tracePreferredPropertiesForPlanNode(join, 1, "Preferred properties :");

                    optTrace.msg("Join string : " + joinStrings.getKey() + ")", true);

                    optTrace.msg("Constraint : " + joinStrings.getValue() + " (join id " + joinId + ")", true);

                    optTrace.msg(join.getType().getJoinLabel() + " (node id " + join.getId() +
                            " , trace id " + traceId + ")", true);

                    Optional<JoinNode.DistributionType> distType = join.getDistributionType();
                    optTrace.incrIndent(1);

                    optTrace.msg("Criteria : %s", true, criteria);

                    String formattedHash = formatHash(join.getLeftHashVariable());
                    optTrace.msg("Left hash var. : %s", true, formattedHash);
                    formattedHash = formatHash(join.getRightHashVariable());
                    optTrace.msg("Right hash var. : %s", true, formattedHash);
                    distType.ifPresent(dist -> optTrace.msg("Distribution type : %s", true, dist.name()));

                    optTrace.decrIndent(1);

                    optTrace.incrIndent(1);
                    optTrace.msg("Left input :", true);
                    optTrace.incrIndent(1);
                    join.getLeft().accept(this, optTraceContext);
                    optTrace.decrIndent(2);

                    optTrace.incrIndent(1);
                    optTrace.msg("Right input :", true);
                    optTrace.incrIndent(1);
                    join.getRight().accept(this, optTraceContext);
                    optTrace.decrIndent(2);

                    break;
                }

                case CNT_JOINS:
                    ++(optTraceContext.joinCnt);
                    break;

                case BUILD_JOIN_STRING: {
                    if (optTraceContext.joinString == null) {
                        optTraceContext.joinString = new String("(");
                    }
                    else {
                        optTraceContext.joinString = optTraceContext.joinString + "(";
                    }

                    join.getLeft().accept(this, optTraceContext);
                    optTraceContext.joinString = optTraceContext.joinString + " " + join.getType() + " ";
                    join.getRight().accept(this, optTraceContext);

                    Optional<JoinNode.DistributionType> distType = join.getDistributionType();

                    optTraceContext.joinString = optTraceContext.joinString + ")";

                    String distStr = null;
                    if (distType.isPresent()) {
                        JoinNode.DistributionType joinDistType = distType.get();
                        switch (joinDistType) {
                            case PARTITIONED:
                                distStr = new String("[P]");
                                break;
                            case REPLICATED:
                                distStr = new String("[R]");
                                break;
                            default:
                                distStr = new String("[?]");
                                break;
                        }

                        optTraceContext.joinString = optTraceContext.joinString + " " + distStr;
                    }

                    break;
                }

                default: {
                    for (PlanNode child : join.getSources()) {
                        child.accept(this, optTraceContext);
                    }
                }
            }

            return null;
        }

        @Override
        public PlanNode visitSemiJoin(SemiJoinNode join, OptTraceContext optTraceContext)
        {
            OptTrace optTrace = optTraceContext.optTrace();

            switch (optTraceContext.visitType) {
                case PRINT: {
                    Integer traceId = optTrace.getTraceId(join);
                    Integer joinId = optTrace.getJoinId(join);

                    optTrace.msg("SemiJoin (node id " + join.getId() +
                            " , trace id " + traceId + ")", true);

                    optTrace.traceVariableReferenceExpressionList(join.getOutputVariables(), 1, "Output variables :");
                    optTrace.tracePreferredPropertiesForPlanNode(join, 1, "Preferred properties :");

                    Optional<SemiJoinNode.DistributionType> distType = join.getDistributionType();
                    optTrace.incrIndent(1);

                    distType.ifPresent(dist -> optTrace.msg("Distribution type : %s", true, dist.name()));

                    optTrace.decrIndent(1);

                    optTrace.incrIndent(1);
                    optTrace.msg("Probe :", true);
                    optTrace.incrIndent(1);
                    join.getProbe().accept(this, optTraceContext);
                    optTrace.decrIndent(2);

                    optTrace.incrIndent(1);
                    optTrace.msg("Build :", true);
                    optTrace.incrIndent(1);
                    join.getBuild().accept(this, optTraceContext);
                    optTrace.decrIndent(2);

                    break;
                }

                case CNT_JOINS:
                    ++(optTraceContext.joinCnt);
                    break;

                case BUILD_JOIN_STRING: {
                    if (optTraceContext.joinString == null) {
                        optTraceContext.joinString = new String("(");
                    }
                    else {
                        optTraceContext.joinString = optTraceContext.joinString + "(";
                    }

                    join.getProbe().accept(this, optTraceContext);
                    optTraceContext.joinString = optTraceContext.joinString + " ";
                    join.getBuild().accept(this, optTraceContext);

                    optTraceContext.joinString = optTraceContext.joinString + ")";

                    break;
                }

                default: {
                    for (PlanNode child : join.getSources()) {
                        child.accept(this, optTraceContext);
                    }
                }
            }

            return null;
        }

        @Override
        public PlanNode visitTableScan(TableScanNode tableScan, OptTraceContext optTraceContext)
        {
            OptTrace optTrace = optTraceContext.optTrace();
            ++(optTraceContext.tableCnt);

            switch (optTraceContext.visitType) {
                case PRINT: {
                    Integer traceId = optTrace.getTraceId(tableScan);

                    if (traceId == null) {
                        throw new PrestoException(GENERIC_INTERNAL_ERROR, format("Could not find trace id : %s (node id ", tableName(tableScan, optTrace)) +
                                tableScan.getId() + ")");
                    }

                    optTrace.msg(tableScan.getClass().getSimpleName() + " (" + tableName(tableScan, optTrace) + " , node id " + tableScan.getId() + " , trace id " +
                            traceId + ")", true);

                    optTrace.traceVariableReferenceExpressionList(tableScan.getOutputVariables(), 1, "Output variables :");
                    optTrace.tracePreferredPropertiesForPlanNode(tableScan, 1, "Preferred properties :");

                    break;
                }

                case FIND_TABLES: {
                    optTraceContext.addTableScan(tableScan);
                    break;
                }

                case CNT_TABLES: {
                    break;
                }

                case BUILD_JOIN_STRING: {
                    if (optTraceContext.joinString == null) {
                        optTraceContext.joinString = new String(tableName(tableScan, optTrace));
                    }
                    else {
                        optTraceContext.joinString = optTraceContext.joinString + tableName(tableScan, optTrace);
                    }
                }

                default:
            }

            return null;
        }
    }

    private static String tableName(TableScanNode tableScanNode, OptTrace optTrace)
    {
        String str = tableScanNode.getTable().getConnectorHandle().toString();

        if (optTrace != null && optTrace.locationToTableOrAliasNameMap != null &&
                tableScanNode.getSourceLocation().isPresent()) {
            str = optTrace.locationToTableOrAliasNameMap.get(sourceLocationToString(tableScanNode.getSourceLocation().get()));
        }

        if (str == null) {
            str = tableScanNode.getTable().getConnectorHandle().toString();
            String token = new String("tableName=");
            int startPos = str.indexOf(token);
            if (startPos != -1) {
                startPos += token.length();
                int endPos = str.indexOf(",", startPos);

                if (endPos != -1) {
                    str = str.substring(startPos, endPos);
                }
            }
        }

        return str;
    }

    private void doIndent(int indentCnt)
    {
        try {
            for (int i = 0; i < indentCnt; ++i) {
                bufferedWriter.write(" ");
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String tableName(int groupId)
    {
        String name = null;
        if (memo != null) {
            PlanNode planNode = memo.getNode(groupId);

            if (planNode instanceof TableScanNode) {
                TableScanNode tableScan = (TableScanNode) planNode;
                name = tableName(tableScan, this);
            }
        }

        return name;
    }

    public void tracePartition(Set<Integer> partition, String msgString, Object... args)
    {
        requireNonNull(partition, "partition is null");

        StringBuilder partMsgBuilderIds = new StringBuilder();

        if (msgString != null) {
            partMsgBuilderIds.append(String.format(msgString, args));
        }

        partMsgBuilderIds.append("[");

        boolean first = true;
        boolean foundName = false;
        for (int group : partition) {
            if (!first) {
                partMsgBuilderIds.append(" , ");
            }

            partMsgBuilderIds.append(String.format("%s", group));

            first = false;
        }

        partMsgBuilderIds.append("]");

        msg(partMsgBuilderIds.toString(), true);
    }

    public void traceJoinSources(Set<PlanNode> sources, int indentCnt, String msgString, Object... args)
    {
        incrIndent(indentCnt);
        if (msgString != null) {
            msg(msgString, true, args);
            incrIndent(1);
        }

        int cnt = 0;
        for (PlanNode source : sources) {
            int joinId = getJoinId(source);
            String sourceStr = null;

            String nodeStr;

            if (source instanceof TableScanNode || source instanceof JoinNode || source instanceof SemiJoinNode) {
                Pair<String, String> joinStrings = getJoinStrings(source);
                requireNonNull(joinStrings, "join strings are null");

                nodeStr = joinStrings.getKey();
            }
            else {
                LinkedHashSet<TableScanNode> tableScans = getTableScans(source);

                if (tableScans != null && tableScans.size() > 0) {
                    nodeStr = tableScansToString(tableScans);
                }
                else {
                    nodeStr = new String(source.getClass().getSimpleName() + " " + source.getId().getId());
                }
            }

            if (source instanceof GroupReference) {
                GroupReference groupReference = (GroupReference) source;
                Stream<PlanNode> planNodes = lookUp.resolveGroup(groupReference);
                Optional<PlanNode> groupPlanNode = planNodes.findFirst();
                if (groupPlanNode.isPresent()) {
                    source = groupPlanNode.get();
                }
            }

            String sourceSimpleName;
            if (source instanceof JoinNode) {
                JoinNode joinNode = (JoinNode) source;
                sourceSimpleName = joinNode.getType().name();
            }
            else {
                sourceSimpleName = source.getClass().getSimpleName();
            }

            if (joinId != -1) {
                sourceStr = "Source %d : " + sourceSimpleName + " " + nodeStr + format(" (node id %s , join id %d)",
                        source.getId(), joinId);
            }
            else {
                throw new PrestoException(GENERIC_INTERNAL_ERROR, format("join id lookup failed"));
            }

            msg(sourceStr, true, cnt);
            ++cnt;
        }

        if (msgString != null) {
            decrIndent(1);
        }

        decrIndent(indentCnt);
    }

    public void tracePlanCostEstimate(PlanCostEstimate planCostEstimate, int indentCnt, String msgString, Object... args)
    {
        requireNonNull(planCostEstimate, "node is null");

        incrIndent(indentCnt);
        if (msgString != null) {
            msg(msgString, true, args);
            incrIndent(1);
        }

        if (planCostEstimate == PlanCostEstimate.infinite()) {
            msg("<infinite>", true);
        }
        else if (planCostEstimate == PlanCostEstimate.unknown()) {
            msg("<unknown>", true);
        }
        else if (planCostEstimate == PlanCostEstimate.zero()) {
            msg("<zero>", true);
        }
        else {
            msg("Cpu : %.3f , Network : %.3f , Max. mem. : %.3f", true, planCostEstimate.getCpuCost(),
                    planCostEstimate.getNetworkCost(),
                    planCostEstimate.getMaxMemory());
        }

        if (msgString != null) {
            decrIndent(1);
        }

        decrIndent(indentCnt);
    }

    public void tracePlanNodeStatsEstimate(PlanNodeStatsEstimate planNodeStatsEstimate, int indentCnt, String msgString, Object... args)
    {
        requireNonNull(planNodeStatsEstimate, "stats node estimate is null");

        if (msgString != null) {
            incrIndent(indentCnt);

            msg(msgString, true, args);
        }

        incrIndent(indentCnt + 1);

        if (planNodeStatsEstimate == PlanNodeStatsEstimate.unknown()) {
            msg("<unknown>", true);
        }
        else {
            msg("Row count : %.0f, confident? %s", true, planNodeStatsEstimate.getOutputRowCount(),
                    planNodeStatsEstimate.isConfident() ? "true" : "false");
        }

        decrIndent(indentCnt + 1);

        if (msgString != null) {
            decrIndent(indentCnt);
        }
    }

    public void traceJoinConstraint(PlanNode planNode, int indentCnt, String msgString, Object... args)
    {
        requireNonNull(planNode, "node is null");

        if (msgString != null) {
            incrIndent(indentCnt);

            msg(msgString, true, args);
        }

        incrIndent(1);
        JoinConstraintNode joinConstraint = joinConstraintNode(planNode);

        if (joinConstraint != null) {
            String joinConstraintString = null;
            Integer joinId = joinConstraint.joinId();
            if (joinId == null) {
                PlanNode source = joinConstraint.getSourceNode();
                requireNonNull(source, "source of join constraint is null");
                joinId = getJoinId(source);
                requireNonNull(joinId, "join id is null");
            }

            joinConstraintString = joinConstraint.joinConstraintString();
            msg(joinConstraintString + " (join id %d)", true, joinId.intValue());
        }
        else {
            msg("<no join>", true);
        }

        decrIndent(1);

        if (msgString != null) {
            decrIndent(indentCnt);
        }
    }

    public void traceEnumeratedJoins(PlanNode root, CostProvider costProvider, StatsProvider statsProvider, int indentCnt,
            String msgString, Object... args)
    {
        requireNonNull(root, "root is null");
        incrIndent(indentCnt);
        if (msgString != null) {
            msg(msgString, true, args);
            incrIndent(1);
        }

        PlanNodeTableScanCntComparator compare = new PlanNodeTableScanCntComparator(this);
        LinkedHashSet<TableScanNode> tableScans = getTableScans(root);

        if (tableScans != null) {
            ArrayList<TableScanNode> tableScansArray = new ArrayList<>(tableScans);
            tableScansArray.sort(compare);

            msg("Table scans :", true);
            incrIndent(1);
            int cnt = 0;
            for (TableScanNode tableScanNode : tableScansArray) {
                int joinId = getJoinId(tableScanNode);
                String sourceStr = null;
                Pair<String, String> joinStrings = getJoinStrings(tableScanNode);
                requireNonNull(joinStrings, "join strings are null");

                if (joinId != -1) {
                    sourceStr = "Table scan %d : " + tableScanNode.getClass().getSimpleName() + " " + joinStrings.getKey() + format(" (node id %s , join id %d)",
                            tableScanNode.getId(), joinId);
                }
                else {
                    throw new PrestoException(GENERIC_INTERNAL_ERROR, format("join id lookup failed"));
                }

                msg(sourceStr, true, cnt);

                if (costProvider != null) {
                    PlanCostEstimate planCostEstimate = costProvider.getCost(tableScanNode);
                    tracePlanCostEstimate(planCostEstimate, 1, "Estimated cost :");
                }

                if (statsProvider != null) {
                    PlanNodeStatsEstimate planNodeStatsEstimate = statsProvider.getStats(tableScanNode);
                    tracePlanNodeStatsEstimate(planNodeStatsEstimate, 1, "Estimated stats :");
                }

                ++cnt;
            }

            decrIndent(1);
        }
        else {
            msg("No table scans.", true);
        }

        Collections.sort(enumeratedJoins, compare);

        msg("Joins :", true);
        incrIndent(1);
        int cnt = 0;
        for (JoinNode joinNode : enumeratedJoins) {
            Integer joinId = getJoinId(joinNode);
            requireNonNull(joinId, "join id is null");
            Pair<String, String> joinStrings = getJoinStrings(joinNode);
            PruneReason pruneReason = isPruned(joinId);

            if (pruneReason != null) {
                msg("%d : %s , join id %d (** PRUNED BY %s **)", true, cnt,
                        joinStrings.getValue(), joinId.intValue(), pruneReason.getString().toUpperCase());
            }
            else {
                msg("%d : %s , join id %d (** ACCEPTED **)", true, cnt, joinStrings.getValue(), joinId.intValue());
            }

            msg("    %s", true, joinStrings.getKey());

            if (costProvider != null) {
                PlanCostEstimate planCostEstimate = costProvider.getCost(joinNode);
                tracePlanCostEstimate(planCostEstimate, 1, "Estimated cost :");
            }

            if (statsProvider != null) {
                PlanNodeStatsEstimate planNodeStatsEstimate = statsProvider.getStats(joinNode);
                tracePlanNodeStatsEstimate(planNodeStatsEstimate, 1, "Estimated stats :");
            }

            ++cnt;
        }

        decrIndent(1);

        if (msgString != null) {
            decrIndent(1);
        }

        decrIndent(indentCnt);
    }

    public void tracePartitioning(Partitioning partitioning, int indentCnt, String msgString, Object... args)
    {
        requireNonNull(partitioning, "partitioning is null");

        incrIndent(indentCnt);
        if (msgString != null) {
            msg(msgString, true);
            incrIndent(1);
        }

        PartitioningHandle handle = partitioning.getHandle();

        msg("Single node? %s", true, handle.getConnectorHandle().isSingleNode() ? "true" : "false");
        msg("Coordinator only? %s", true, handle.getConnectorHandle().isCoordinatorOnly() ? "true" : "false");

        msg("Arguments :", true);
        incrIndent(1);
        List<RowExpression> arguments = partitioning.getArguments();
        int cnt = 0;
        for (RowExpression arg : arguments) {
            msg("%d : %s", true, cnt, arg.toString());
            ++cnt;
        }
        decrIndent(1);

        Set<VariableReferenceExpression> varRefs = partitioning.getVariableReferences();
        traceVariableReferenceExpressionList(varRefs.stream().collect(toList()), 1, "Variable references :");

        if (msgString != null) {
            decrIndent(1);
        }

        decrIndent(indentCnt);
    }

    public void traceOrderingScheme(Optional<OrderingScheme> orderingSchemeParam, int indentCnt, String msgString, Object... args)
    {
        requireNonNull(orderingSchemeParam, "ordering scheme is null");
        incrIndent(indentCnt);
        if (msgString != null) {
            msg(msgString, true);
            incrIndent(1);
        }

        msg("Orderings :", true);

        incrIndent(1);
        if (orderingSchemeParam.isPresent()) {
            OrderingScheme orderingScheme = orderingSchemeParam.get();

            int cnt = 0;
            for (Ordering ordering : orderingScheme.getOrderBy()) {
                msg("%d : %s %s", true, cnt, ordering.getVariable().getName(), ordering.getSortOrder());
                ++cnt;
            }
        }
        else {
            msg("<not present>", true);
        }
        decrIndent(1);

        if (msgString != null) {
            decrIndent(1);
        }

        decrIndent(indentCnt);
    }

    public void tracePartitioningScheme(PartitioningScheme partitioningScheme, int indentCnt, String msgString, Object... args)
    {
        requireNonNull(partitioningScheme, "partitioning scheme is null");

        incrIndent(indentCnt);
        if (msgString != null) {
            msg(msgString, true);
            incrIndent(1);
        }

        tracePartitioning(partitioningScheme.getPartitioning(), 0, "Partitioning :");

        traceVariableReferenceExpressionList(partitioningScheme.getOutputLayout(), 0, "Output layout :");

        Optional<VariableReferenceExpression> hashColumn = partitioningScheme.getHashColumn();

        if (hashColumn.isPresent()) {
            msg("Hash column : %s", true, hashColumn.get().getName());
        }
        else {
            msg("Hash column : <not present>", true);
        }

        msg("Replicate nulls and any : %s", true, partitioningScheme.isReplicateNullsAndAny() ? "true" : "false");

        if (partitioningScheme.getBucketToPartition().isPresent()) {
            msg("Bucket to partition : %s", true, partitioningScheme.getBucketToPartition().get().toString());
        }

        if (msgString != null) {
            decrIndent(1);
        }

        decrIndent(indentCnt);
    }

    public void traceVariableReferenceExpressionList(List<VariableReferenceExpression> varList, int indentCnt, String msgString, Object... args)
    {
        int cnt = 0;

        incrIndent(indentCnt);
        if (msgString != null) {
            msg(msgString, true);
            incrIndent(1);
        }

        for (VariableReferenceExpression var : varList) {
            msg("%d : %s (%s)", true, cnt, var.getName(), var.getType().getDisplayName());
            ++cnt;
        }

        if (msgString != null) {
            decrIndent(1);
        }

        decrIndent(indentCnt);
    }

    public void tracePartitioningProperties(PreferredProperties.PartitioningProperties partitioningProperties, int indentCnt, String msgString, Object... args)
    {
        requireNonNull(partitioningProperties, "partitioning properties is null");

        incrIndent(indentCnt);
        if (msgString != null) {
            msg(msgString, true, args);
            incrIndent(1);
        }

        traceVariableReferenceExpressionList(partitioningProperties.getPartitioningColumns().stream().collect(toList()), 0,
                "Partitioning columns :");

        msg("Partitioning :", true);
        incrIndent(1);

        if (partitioningProperties.getPartitioning().isPresent()) {
            tracePartitioning(partitioningProperties.getPartitioning().get(), 0, null);
        }
        else {
            msg("<not present>", true);
        }

        msg("Replicate nulls and any : %s", true, partitioningProperties.isNullsAndAnyReplicated() ? "true" : "false");
        decrIndent(1);

        if (msgString != null) {
            decrIndent(1);
        }

        decrIndent(indentCnt);
    }

    public void tracePreferredProperties(PreferredProperties preferredProperties, int indentCnt, String msgString, Object... args)
    {
        requireNonNull(preferredProperties, "plan node is null");

        incrIndent(indentCnt);
        if (msgString != null) {
            msg(msgString, true, args);
            incrIndent(1);
        }

        msg("Global properties :", true);
        incrIndent(1);
        Optional<PreferredProperties.Global> globalPropertiesOpt = preferredProperties.getGlobalProperties();
        if (globalPropertiesOpt.isPresent()) {
            PreferredProperties.Global globalProperties = globalPropertiesOpt.get();
            msg("Distributed? : %s", true, globalProperties.isDistributed() ? "true" : "false");
            msg("Partitioning properties? :", true);
            incrIndent(1);
            if (globalProperties.getPartitioningProperties().isPresent()) {
                tracePartitioningProperties(globalProperties.getPartitioningProperties().get(), 0, null);
            }
            else {
                msg("<not present>", true);
            }
            decrIndent(1);
        }
        else {
            msg("<not present>", true);
        }

        decrIndent(1);

        if (msgString != null) {
            decrIndent(1);
        }

        decrIndent(indentCnt);
    }

    public void tracePreferredPropertiesForPlanNode(PlanNode planNode, int indentCnt, String msgString, Object... args)
    {
        requireNonNull(planNode, "plan node is null");

        PreferredProperties preferredProperties = planNodeToPreferredPropertiesMap.get(planNode);

        if (preferredProperties != null) {
            tracePreferredProperties(preferredProperties, indentCnt, msgString, args);
        }
    }

    public void tracePlanNode(PlanNode node, int indentCnt, String msgString, Object... args)
    {
        requireNonNull(node, "node is null");

        incrIndent(indentCnt);
        if (msgString != null) {
            msg(msgString, true, args);
            incrIndent(1);
        }

        OptTraceContext optTraceContext = new OptTraceContext(this, VisitType.PRINT);
        node.accept(new OptTrace.OptTraceVisitor(), optTraceContext);

        if (msgString != null) {
            decrIndent(1);
        }

        decrIndent(indentCnt);
    }

    public void incrIndent(int indentCnt)
    {
        indent += incrIndent * indentCnt;
    }

    public void decrIndent(int indentCnt)
    {
        indent -= incrIndent * indentCnt;
    }

    public void msg(String msgString, boolean eol, Object... args)
    {
        try {
            doIndent(indent);

            if (args != null) {
                bufferedWriter.write(String.format(msgString, args));
            }
            else {
                bufferedWriter.write(msgString);
            }

            bufferedWriter.write(" (UID " + nextUid() + ")");

            if (eol) {
                bufferedWriter.newLine();
            }
            bufferedWriter.flush();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected void finalize()
    {
        if (this.bufferedWriter != null) {
            try {
                bufferedWriter.flush();
                bufferedWriter.close();
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
