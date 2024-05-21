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

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.plan.JoinDistributionType;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.SYNTAX_ERROR;
import static com.facebook.presto.spi.plan.JoinDistributionType.PARTITIONED;
import static com.facebook.presto.spi.plan.JoinDistributionType.REPLICATED;
import static com.facebook.presto.spi.plan.JoinType.INNER;
import static com.facebook.presto.sql.planner.planconstraints.PlanConstraintsParser.ConstraintType.CARD;
import static com.facebook.presto.sql.planner.planconstraints.PlanConstraintsParser.ConstraintType.JOIN;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;

public final class PlanConstraintsParser
{
    static final String constraintsMarkerStart = "/*!";
    static final String constraintsMarkerEnd = "*/";

    private PlanConstraintsParser() {}

    @VisibleForTesting
    public static List<PlanConstraint> parse(Optional<String> query)
    {
        if (!query.isPresent() || query.get().isEmpty()) {
            return ImmutableList.of();
        }

        String queryString = query.get();

        if ((queryString.length() - queryString.replace(constraintsMarkerStart, "").length()) / constraintsMarkerStart.length() > 1) {
            throw new PrestoException(SYNTAX_ERROR, "Query cannot have more than one plan hint clause");
        }
        int constraintsStartIndex = queryString.indexOf(constraintsMarkerStart);
        if (constraintsStartIndex == -1) {
            return ImmutableList.of();
        }

        int constraintsEndIndex = queryString.indexOf(constraintsMarkerEnd, constraintsStartIndex);
        String constraintsString = queryString.substring(constraintsStartIndex + constraintsMarkerStart.length(), constraintsEndIndex);

        try {
            return parse(constraintsString.trim().toUpperCase());
        }
        catch (IOException e) {
            throw new PrestoException(GENERIC_USER_ERROR, "Invalid join constraint");
        }
    }

    private static List<PlanConstraint> parse(String constraintsString)
            throws IOException
    {
        if (constraintsString.indexOf(JOIN.getString()) == -1 &&
                constraintsString.indexOf(CARD.getString()) == -1) {
            throw new PrestoException(SYNTAX_ERROR, "Invalid hint format");
        }

        List<PlanConstraint> planConstraints = new ArrayList<>();
        int startIndex = 0;
        Optional<HintString> hintStringOptional = getNextHint(constraintsString, startIndex);
        while (hintStringOptional .isPresent()) {
            HintString hintString = hintStringOptional.get();
            Scanner scanner = new Scanner(new StringReader(hintString.getHint()));
            scanner.nextToken();
            startIndex = hintString.getEndIndex() - 1;
            planConstraints.add(parseHint(scanner, hintString.getConstraintType()));
            hintStringOptional = getNextHint(constraintsString, startIndex);
        }

        return planConstraints.stream()
                .filter(Objects::nonNull)
                .collect(toImmutableList());
    }

    private static Optional<HintString> getNextHint(String constraintsString, int startIndex)
    {
        if (startIndex >= constraintsString.length()) {
            return Optional.empty();
        }

        int joinHintStartIndex = constraintsString.indexOf(JOIN.getString(), startIndex);
        int cardinalityHintStartIndex = constraintsString.indexOf(CARD.getString(), startIndex);
        if (joinHintStartIndex == -1 && cardinalityHintStartIndex == -1) {
            return Optional.empty();
        }

        int hintStartIndex;
        ConstraintType constraintType = JOIN;
        if (cardinalityHintStartIndex == -1) {
            hintStartIndex = joinHintStartIndex;
            constraintType = JOIN;
        }
        else if (joinHintStartIndex == -1) {
            hintStartIndex = cardinalityHintStartIndex;
            constraintType = CARD;
        }
        else if (joinHintStartIndex < cardinalityHintStartIndex) {
            hintStartIndex = joinHintStartIndex;
            constraintType = JOIN;
        }
        else {
            hintStartIndex = cardinalityHintStartIndex;
            constraintType = CARD;
        }

        int offset = JOIN.getString().length();
        int nextJoinHintIndex = constraintsString.indexOf(JOIN.getString(), hintStartIndex + offset);
        int nextCardinalityHintIndex = constraintsString.indexOf(CARD.getString(), hintStartIndex + offset);
        nextJoinHintIndex = nextJoinHintIndex == -1 ? constraintsString.length() : nextJoinHintIndex;
        nextCardinalityHintIndex = nextCardinalityHintIndex == -1 ? constraintsString.length() : nextCardinalityHintIndex;
        int nextHintStartIndex = Math.min(nextJoinHintIndex, nextCardinalityHintIndex);
        String hintString = constraintsString.substring(hintStartIndex + offset, nextHintStartIndex).trim();
        return Optional.of(new HintString(hintString, constraintType, hintStartIndex, nextHintStartIndex));
    }

    private static PlanConstraint parseHint(Scanner scanner, ConstraintType constraintType)
            throws IOException
    {
        switch (scanner.getToken()) {
            case NUMBER:
                long value = scanner.getNumber();
                scanner.nextToken();
                if (constraintType != CARD) {
                    throw new IOException(format("unexpected number (%d) in constraint string", value));
                }
                return new CardinalityEstimate(value);
            case NAME:
                String name = scanner.getName();
                scanner.nextToken();
                return new RelationConstraint(name);
            case LPAREN:
                scanner.nextToken();
                ArrayList<PlanConstraint> nodes = parseList(scanner, constraintType);

                if (scanner.getToken() != Token.RPAREN) {
                    throw new IOException(") expected");
                }

                scanner.nextToken();

                if (constraintType == CARD) {
                    if (nodes.size() != 2 || !(nodes.get(1) instanceof CardinalityEstimate)) {
                        //!(nodes.get(nodes.size() - 1) instanceof CardinalityEstimate)) {
                        throw new IOException("malformed cardinality constraint");
                    }
                    //return new CardinalityConstraint(nodes.subList(0, nodes.size() - 1), (CardinalityEstimate) nodes.get(nodes.size() - 1));
                    return new CardinalityConstraint(nodes.get(0), (CardinalityEstimate) nodes.get(1));
                }

                Optional<JoinDistributionType> distributionType = Optional.empty();
                if (scanner.getToken() == Token.LBRACKET) {
                    scanner.nextToken();

                    if (scanner.getToken() == Token.PARTITIONED) {
                        distributionType = Optional.of(PARTITIONED);
                    }
                    else {
                        distributionType = Optional.of(REPLICATED);
                    }

                    scanner.nextToken();

                    if (scanner.getToken() != Token.RBRACKET) {
                        throw new RuntimeException("] expected");
                    }

                    scanner.nextToken();
                }

                // TODO: add other join types
                return new JoinConstraint(INNER, distributionType, nodes);
            case EOF:
                return null;
            default:
                throw new RuntimeException("Number or ( expected");
        }
    }

    private static ArrayList<PlanConstraint> parseList(Scanner scanner, ConstraintType constraintType)
            throws IOException
    {
        ArrayList<PlanConstraint> nodes = new ArrayList<>();
        if (scanner.getToken() != Token.RPAREN) {
            nodes.add(parseHint(scanner, JOIN));

            while (scanner.getToken() != Token.RPAREN) {
                if (scanner.getToken() == Token.EOF) {
                    throw new RuntimeException("unexpected EOF when parsing constraint string");
                }
                nodes.add(parseHint(scanner, constraintType));
            }
        }

        return nodes;
    }

    public enum ConstraintType
    {
        JOIN("JOIN"),
        CARD("CARD");

        private String string;

        ConstraintType(String stringRep)
        {
            this.string = stringRep;
        }

        public String getString()
        {
            return string;
        }
    }

    private static enum Token
    {
        LPAREN,
        RPAREN,
        NUMBER,
        NAME,
        EOF,
        SPACE,
        LBRACKET,
        RBRACKET,
        PARTITIONED,
        REPLICATED
    }

    private static class Scanner
    {
        private final Reader inputReader;
        private int c;
        private Token token;
        private long number;
        private String name;

        public Scanner(Reader inputReader)
                throws IOException
        {
            this.inputReader = inputReader;
            c = inputReader.read();
        }

        public Token getToken()
        {
            return token;
        }

        public long getNumber()
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
                c = inputReader.read();
            }

            if (c < 0) {
                token = Token.EOF;
                return token;
            }
            // TODO - estimates can also be floating point
            if (c >= '0' && c <= '9') {
                number = c - '0';
                c = inputReader.read();
                while (c >= '0' && c <= '9') {
                    number = 10 * number + (c - '0');
                    c = inputReader.read();
                }

                token = Token.NUMBER;
                return token;
            }

            switch (c) {
                case '(':
                    token = Token.LPAREN;
                    break;
                case ')':
                    token = Token.RPAREN;
                    break;
                case '[':
                    token = Token.LBRACKET;
                    break;
                case ']':
                    token = Token.RBRACKET;
                    break;
                default:
                    StringBuilder builder = new StringBuilder();
                    while (c != ' ' && c != ')' && c != '(' && c != '[' && c != ']') {
                        builder.append((char) c);
                        c = inputReader.read();
                    }
                    name = builder.toString();
                    if (!name.isEmpty()) {
                        if (name.equals("P")) {
                            token = Token.PARTITIONED;
                        }
                        else if (name.equals("R")) {
                            token = Token.REPLICATED;
                        }
                        else {
                            token = Token.NAME;
                        }
                        return token;
                    }
            }

            c = inputReader.read();
            return token;
        }
    }

    private static class HintString
    {
        private final String hint;
        private final ConstraintType constraintType;
        private final int startIndex;
        private final int endIndex;

        public HintString(String hint, ConstraintType constraintType, int startIndex, int endIndex)
        {
            this.hint = hint;
            this.constraintType = constraintType;
            this.startIndex = startIndex;
            this.endIndex = endIndex;
        }

        public String getHint()
        {
            return hint;
        }

        public int getStartIndex()
        {
            return startIndex;
        }

        public int getEndIndex()
        {
            return endIndex;
        }

        public ConstraintType getConstraintType()
        {
            return constraintType;
        }
    }
}
