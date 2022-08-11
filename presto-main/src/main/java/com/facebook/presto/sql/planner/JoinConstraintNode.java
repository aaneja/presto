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

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.google.common.collect.BiMap;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

class JoinConstraintNode
{
    private Integer joinId;
    private ArrayList<JoinConstraintNode> children;
    private String joinConstraintString;
    ArrayList<Integer> joinIdSet;
    PlanNode sourceNode;
    JoinNode.DistributionType joinDistType;
    boolean isRoot;
    String tableOrAliasName;

    public enum ConstraintCompare
    {
        NOT_APPLICABLE("not applicable"),
        TRUE("true"),
        FALSE("false");

        private String string;

        ConstraintCompare(String stringRep)
        {
            this.string = stringRep;
        }

        public String getString()
        {
            return string;
        }
    }

    public JoinConstraintNode(TableScanNode tableScan, Integer joinIdParam, String tableOrAliasNameParam)
    {
        joinId = joinIdParam;
        children = null;
        joinConstraintString = null;
        joinIdSet = new ArrayList<Integer>();

        if (joinId != null) {
            joinIdSet.add(joinId);
        }

        sourceNode = tableScan;
        joinDistType = null;
        isRoot = true;
        tableOrAliasName = tableOrAliasNameParam;
    }

    public JoinConstraintNode(Integer joinIdParam)
    {
        joinId = joinIdParam;
        children = null;
        joinConstraintString = null;
        joinIdSet = new ArrayList<Integer>();
        joinIdSet.add(joinId);
        sourceNode = null;
        joinDistType = null;
        isRoot = true;
        tableOrAliasName = null;
    }

    public JoinConstraintNode()
    {
        joinId = null;
        children = null;
        joinConstraintString = null;
        joinIdSet = new ArrayList<Integer>();
        sourceNode = null;
        joinDistType = null;
        isRoot = true;
        tableOrAliasName = null;
    }

    public JoinConstraintNode(ArrayList<JoinConstraintNode> children)
    {
        joinId = null;
        joinIdSet = new ArrayList<Integer>();
        for (JoinConstraintNode child : children) {
            appendChild(child);
        }

        joinConstraintString = null;
        sourceNode = null;
        joinDistType = null;
        isRoot = true;
        tableOrAliasName = null;
    }

    public void setDistributionType(JoinNode.DistributionType joinDistTypeParam)
    {
        joinDistType = joinDistTypeParam;
    }

    public JoinConstraintNode(JoinNode joinNode)
    {
        requireNonNull(joinNode, "join is null");
        joinId = null;
        children = null;
        joinConstraintString = null;
        joinIdSet = new ArrayList<Integer>();
        sourceNode = joinNode;
        tableOrAliasName = null;

        if (joinNode.getDistributionType().isPresent()) {
            setDistributionType(joinNode.getDistributionType().get());
        }
        else {
            setDistributionType(null);
        }

        isRoot = true;
    }

    public JoinConstraintNode(SemiJoinNode semiJoinNodeParam)
    {
        joinId = null;
        children = null;
        joinConstraintString = null;
        joinIdSet = new ArrayList<Integer>();
        sourceNode = semiJoinNodeParam;
        joinDistType = null;
        isRoot = true;
        tableOrAliasName = null;
    }

    public PlanNode getSourceNode()
    {
        return sourceNode;
    }

    public Integer joinId()
    {
        return joinId;
    }

    public void setJoinId(Integer joinIdParam)
    {
        joinId = joinIdParam;
        joinIdSet.add(joinId);
    }

    public void setJoinIdsFromTableOrAliasName(HashMap<String, Integer> joinIdMap, BiMap<String, String> locationToTableOrAliasNameMap)
    {
        requireNonNull(joinIdMap, "join id map is null");
        requireNonNull(locationToTableOrAliasNameMap, "location-to-name map is null");
        if (this.joinId == null && tableOrAliasName != null) {
            String lookUpString = locationToTableOrAliasNameMap.inverse().get(tableOrAliasName);

            if (lookUpString == null) {
                throw new PrestoException(GENERIC_INTERNAL_ERROR, format("no lookup string for table %s", tableOrAliasName));
            }
            Integer joinId = joinIdMap.get(lookUpString);
            if (joinId == null || joinId == -1) {
                throw new PrestoException(GENERIC_INTERNAL_ERROR, format("Join id not found : %s", tableOrAliasName));
            }

            this.joinId = joinId;
            joinIdSet.add(joinId);
        }
        else if (children != null) {
            joinIdSet.clear();
            for (JoinConstraintNode child : children) {
                child.setJoinIdsFromTableOrAliasName(joinIdMap, locationToTableOrAliasNameMap);
                joinIdSet.addAll(child.joinIdSet);

                if (joinIdSet.stream().distinct().count() != joinIdSet.size()) {
                    throw new PrestoException(GENERIC_INTERNAL_ERROR, "Duplicate join id");
                }
            }
        }
    }

    public String tableOrAliasName()
    {
        return tableOrAliasName;
    }

    public boolean joinIsValid(ArrayList<JoinConstraintNode> joinConstraints, OptTrace optTrace)
    {
        Integer joinId = null;

        if (optTrace != null) {
            String joinConstraintString = this.joinConstraintString();
            optTrace.begin("joinIsValid");
            optTrace.msg("Join : %s", true, joinConstraintString);
            joinId = optTrace.getJoinId(joinConstraintString);
        }

        boolean valid;

        if (joinConstraints == null || joinConstraints.isEmpty()) {
            valid = true;
        }
        else {
            valid = false;
            boolean oneApplicable = false;

            for (JoinConstraintNode constraint : joinConstraints) {
                if (optTrace != null) {
                    optTrace.msg("Next constraint : %s", true, constraint.joinConstraintString());
                }
                ConstraintCompare cmp = satisfies(constraint);
                if (optTrace != null) {
                    optTrace.incrIndent(1);
                    optTrace.msg("Result : %s", true, cmp.getString());
                    optTrace.decrIndent(1);
                }
                if (cmp == ConstraintCompare.TRUE) {
                    valid = true;
                    break;
                }
                else if (cmp == ConstraintCompare.FALSE) {
                    oneApplicable = true;
                }
            }

            if (!valid && !oneApplicable) {
                valid = true;
            }
        }

        if (optTrace != null) {
            optTrace.msg("** JOIN VALID ? %s **", true, valid ? "TRUE" : "FALSE");
            optTrace.end("joinIsValid");
        }

        return valid;
    }

    public boolean satisfiesAnyConstraint(ArrayList<JoinConstraintNode> joinConstraints, OptTrace optTrace)
    {
        boolean satisfiesAny = false;
        if (joinConstraints != null && !joinConstraints.isEmpty()) {
            for (JoinConstraintNode constraint : joinConstraints) {
                ConstraintCompare cmp = satisfies(constraint);

                if (cmp == ConstraintCompare.TRUE) {
                    satisfiesAny = true;
                    break;
                }
            }
        }

        return satisfiesAny;
    }

    private JoinConstraintNode find(Integer joinId, String tableOrAliasName, ArrayList<JoinConstraintNode> childMappings,
                                    JoinNode.DistributionType distType)
    {
        JoinConstraintNode foundJoinConstraintNode = null;
        requireNonNull(childMappings, "child mappings is null");

        boolean distTypeOk = true;
        if (distType != null) {
            if (this.joinDistType != null && !distType.equals(this.joinDistType)) {
                distTypeOk = false;
            }
        }
        else {
            if (this.joinDistType != null) {
                distTypeOk = false;
            }
        }

        if (distTypeOk) {
            if (joinId != null && this.joinId != null && joinId == this.joinId) {
                foundJoinConstraintNode = this;
            }
            else if (tableOrAliasName != null && this.tableOrAliasName != null && tableOrAliasName == this.tableOrAliasName) {
                foundJoinConstraintNode = this;
            }
            else if (childMappings.size() > 0) {
                boolean matchedChildren = false;
                if (this.children != null && this.children.size() == childMappings.size()) {
                    matchedChildren = true;
                    for (int i = 0; i < childMappings.size() && matchedChildren; ++i) {
                        matchedChildren = this.children.get(i).equals(childMappings.get(i));
                    }
                }

                if (matchedChildren) {
                    foundJoinConstraintNode = this;
                }
            }
        }

        for (int i = 0; foundJoinConstraintNode == null && this.children != null && i < this.children.size(); ++i) {
            foundJoinConstraintNode = this.children.get(i).find(joinId, tableOrAliasName, childMappings, distType);
        }

        return foundJoinConstraintNode;
    }

    private JoinConstraintNode match(JoinConstraintNode otherJoinConstraintNode)
    {
        JoinConstraintNode match = null;

        ArrayList<JoinConstraintNode> childMappings = new ArrayList<>();
        boolean allChildrenMatch = true;
        for (int i = 0; children != null && i < children.size() && match == null; ++i) {
            JoinConstraintNode matchedChild = children.get(i).match(otherJoinConstraintNode);
            if (matchedChild != null) {
                if (matchedChild == otherJoinConstraintNode) {
                    match = matchedChild;
                }
                else {
                    childMappings.add(matchedChild);
                }
            }
            else {
                allChildrenMatch = false;
            }
        }

        if (match == null && allChildrenMatch) {
            match = otherJoinConstraintNode.find(joinId, tableOrAliasName, childMappings, joinDistType);
        }

        return match;
    }

    public ConstraintCompare satisfies(JoinConstraintNode otherJoinConstraintNode)
    {
        ConstraintCompare satisfiesConstraint;

        if (otherJoinConstraintNode != null) {
            if (!joinIdsIntersect(otherJoinConstraintNode)) {
                satisfiesConstraint = ConstraintCompare.NOT_APPLICABLE;
            }
            else if (match(otherJoinConstraintNode) != null) {
                satisfiesConstraint = ConstraintCompare.TRUE;
            }
            else {
                satisfiesConstraint = ConstraintCompare.FALSE;
            }
        }
        else {
            satisfiesConstraint = ConstraintCompare.TRUE;
        }

        return satisfiesConstraint;
    }

    public boolean joinIdsIntersect(JoinConstraintNode otherJoinConstraintNode)
    {
        requireNonNull(otherJoinConstraintNode, "unexpected null join constraint node");
        boolean intersects = false;
        if (!joinIdSet.isEmpty() && !otherJoinConstraintNode.joinIdSet.isEmpty()) {
            for (Integer joinId : joinIdSet) {
                if (otherJoinConstraintNode.joinIdSet.contains(joinId)) {
                    intersects = true;
                    break;
                }
            }
        }

        return intersects;
    }

    public boolean joinIdsSubset(JoinConstraintNode otherJoinConstraintNode, boolean proper)
    {
        requireNonNull(otherJoinConstraintNode, "unexpected null join constraint node");
        boolean subset;

        int joinIdSetSize = joinIdSet.size();
        int otherjoinIdSetSize = otherJoinConstraintNode.joinIdSet.size();

        if (joinIdSetSize > otherjoinIdSetSize || (proper && joinIdSetSize == otherjoinIdSetSize)) {
            subset = false;
        }
        else {
            subset = true;
            for (Integer joinId : joinIdSet) {
                if (!otherJoinConstraintNode.joinIdSet.contains(joinId)) {
                    subset = false;
                    break;
                }
            }
        }

        return subset;
    }

    public void appendChild(JoinConstraintNode child)
    {
        if (children == null) {
            children = new ArrayList<JoinConstraintNode>();
        }

        children.add(child);
        child.isRoot = false;
        ArrayList<Integer> childjoinIdSet = child.joinIdSet;
        for (Integer childjoinId : childjoinIdSet) {
            if (joinIdSet.contains(childjoinId)) {
                throw new PrestoException(GENERIC_INTERNAL_ERROR, format("duplicate join id : %d", childjoinId));
            }
        }
        joinIdSet.addAll(childjoinIdSet);
        Collections.sort(joinIdSet);
    }

    private String buildJoinConstraintString()
    {
        StringBuilder builder = new StringBuilder();
        if (isRoot == true && children != null && children.size() > 1) {
            builder.append("JOIN");
        }

        if (children == null || children.size() == 0) {
            if (tableOrAliasName != null) {
                builder.append(tableOrAliasName);
            }
            else if (joinId != null) {
                builder.append(joinId);
            }
        }
        else {
            boolean first = true;
            builder.append("(");
            for (JoinConstraintNode child : children) {
                if (!first) {
                    builder.append(" ");
                }

                String childJoinConstraintString = child.joinConstraintString();

                int startPos = childJoinConstraintString.indexOf("JOIN", 0);

                if (startPos >= 0) {
                    childJoinConstraintString = childJoinConstraintString.substring(startPos + 4);
                }

                builder.append(childJoinConstraintString);
                first = false;
            }

            builder.append(")");
        }

        if (joinDistType != null) {
            switch (joinDistType) {
                case PARTITIONED:
                    builder.append(" [P]");
                    break;
                case REPLICATED:
                    builder.append(" [R]");
                    break;
                default:
                    builder.append(" [?]");
                    break;
            }
        }

        return builder.toString();
    }

    public String joinConstraintString()
    {
        if (joinConstraintString == null) {
            joinConstraintString = buildJoinConstraintString();
        }

        return joinConstraintString;
    }

    private static enum Token
    {LPAR, RPAR, NUMBER, NAME, EOF, SPACE, LBRACKET, RBRACKET, PARTITIONED, REPLICATED};

    private static class Scanner
    {
        private final Reader in;
        private int c;
        private Token token;
        private int number;
        private String name;

        public Scanner(Reader in)
                throws IOException
        {
            this.in = in;
            c = in.read();
        }

        public Token getToken()
        {
            return token;
        }

        public int getNumber()
        {
            return number;
        }

        public String getName()
        {
            return name;
        }

        public Token nextToken()
                throws IOException
        {
            while (c == ' ') {
                c = in.read();
            }

            if (c < 0) {
                token = Token.EOF;
                return token;
            }
            if (c >= '0' && c <= '9') {
                number = c - '0';
                c = in.read();
                while (c >= '0' && c <= '9') {
                    number = 10 * number + (c - '0');
                    c = in.read();
                }

                token = Token.NUMBER;
                return token;
            }

            switch (c) {
                case '(':
                    token = Token.LPAR;
                    break;
                case ')':
                    token = Token.RPAR;
                    break;
                case '[':
                    token = Token.LBRACKET;
                    break;
                case ']':
                    token = Token.RBRACKET;
                    break;
                case 'P':
                    token = Token.PARTITIONED;
                    break;
                case 'R':
                    token = Token.REPLICATED;
                    break;
                default:
                    StringBuilder builder = new StringBuilder();
                    while (c != ' ' && c != ')') {
                        builder.append((char) c);
                        c = in.read();
                    }
                    name = builder.toString();
                    if (!name.isEmpty()) {
                        token = Token.NAME;
                        return token;
                    }
                    //throw new RuntimeException("Unknown character " + c);
            }

            c = in.read();
            return token;
        }
    }

    private static ArrayList<JoinConstraintNode> parseList(Scanner scanner)
            throws IOException
    {
        ArrayList<JoinConstraintNode> nodes = new ArrayList<JoinConstraintNode>();
        if (scanner.getToken() != Token.RPAR) {
            nodes.add(parse(scanner));

            while (scanner.getToken() != Token.RPAR) {
                if (scanner.getToken() == Token.EOF) {
                    throw new RuntimeException("expected EOF when parsing constraint string");
                }

                JoinConstraintNode newJoinConstraintNode = parse(scanner);

                nodes.add(newJoinConstraintNode);
            }
        }

        return nodes;
    }

    public static void parse(String constraintString, ArrayList<JoinConstraintNode> joinConstraints)
            throws IOException
    {
        requireNonNull(constraintString, "constraint string is null");
        requireNonNull(joinConstraints, "constraint list is null");

        constraintString = constraintString.trim();
        String upperCaseConstraintString = constraintString.toUpperCase();
        int startPos = upperCaseConstraintString.indexOf("JOIN", 0);
        int endPos = constraintString.length();

        while (startPos >= 0 && startPos < endPos) {
            startPos += 4;
            String nextConstraintString = null;
            int nextPos = upperCaseConstraintString.indexOf("JOIN", startPos);
            if (nextPos > 0 && nextPos < endPos) {
                nextConstraintString = constraintString.substring(startPos, nextPos).trim();
            }
            else {
                nextConstraintString = constraintString.substring(startPos, endPos);
            }

            StringReader in = new StringReader(nextConstraintString);
            Scanner scanner = new Scanner(in);
            scanner.nextToken();
            JoinConstraintNode joinConstraintNode = null;

            try {
                joinConstraintNode = parse(scanner);
            }
            catch (IOException e) {
                System.err.println("Invalid join constraint : " + nextConstraintString);
                joinConstraintNode = null;
            }

            if (joinConstraintNode != null) {
                joinConstraints.add(joinConstraintNode);
            }

            startPos = nextPos;
        }

        return;
    }

    private static JoinConstraintNode parse(Scanner scanner)
            throws IOException
    {
        switch (scanner.getToken()) {
            case NUMBER:
                int value = scanner.getNumber();
                scanner.nextToken();
                return new JoinConstraintNode((TableScanNode) null, value, null);
            case NAME:
                String name = scanner.getName();
                scanner.nextToken();
                return new JoinConstraintNode((TableScanNode) null, null, name);
            case LPAR:
                scanner.nextToken();
                ArrayList<JoinConstraintNode> nodes = parseList(scanner);

                if (scanner.getToken() != Token.RPAR) {
                    throw new RuntimeException(") expected");
                }

                scanner.nextToken();

                JoinNode.DistributionType distType = null;
                if (scanner.getToken() == Token.LBRACKET) {
                    scanner.nextToken();
                    if (scanner.getToken() == Token.PARTITIONED) {
                        distType = JoinNode.DistributionType.PARTITIONED;
                    }
                    else {
                        distType = JoinNode.DistributionType.REPLICATED;
                    }
                    scanner.nextToken();
                    if (scanner.getToken() != Token.RBRACKET) {
                        throw new RuntimeException("] expected");
                    }
                    scanner.nextToken();
                }

                JoinConstraintNode newNode = new JoinConstraintNode(nodes);
                newNode.setDistributionType(distType);
                return newNode;
            case EOF:
                return null;
            default:
                throw new RuntimeException("Number or ( expected");
        }
    }
}
