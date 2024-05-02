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
import com.facebook.presto.spi.plan.TableScanNode;
import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Scanner;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.SYNTAX_ERROR;
import static com.facebook.presto.sql.planner.planconstraints.PlanConstraintsParser.ConstraintType.CARD;
import static com.facebook.presto.sql.planner.planconstraints.PlanConstraintsParser.ConstraintType.JOIN;

public final class PlanConstraintsParser
{
    private static final String constraintsMarkerStart = "/*!";
    private static final String constraintMarkerEnd = "*/";

    private PlanConstraintsParser() {}

    public static List<PlanConstraint> parse(Optional<String> query)
    {
        if (!query.isPresent() || query.get() == "") {
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

        int constraintsEndIndex = queryString.indexOf(constraintMarkerEnd, constraintsStartIndex);
        String constraintsString = queryString.substring(constraintsStartIndex + constraintsMarkerStart.length(), constraintsEndIndex);
//        return ImmutableList.of(new JoinConstraint(INNER,
//                Optional.empty(),
//                ImmutableList.of(new RelationConstraint("valuesA"), new RelationConstraint("valuesB"))));
        try {
            return parse(constraintsString.trim().toUpperCase());
        }
        catch (IOException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Invalid join constraint");
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
        Optional<HintString> hintStringOptional;
        while ((hintStringOptional = getNextHintString(constraintsString, startIndex)).isPresent()) {
            HintString hintString = hintStringOptional.get();
            Scanner scanner = new Scanner(new StringReader(hintString.getHint()));
            scanner.nextToken();
            startIndex = hintString.getEndIndex() - 1;
            // TODO - create PlanConstraints from the scanner and constrainttype
        }

        return planConstraints;
    }

    private static Optional<HintString> getNextHintString(String constraintsString, int startIndex)
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
        int nextHintStartIndex = Math.max(constraintsString.indexOf(JOIN.getString(), hintStartIndex + offset),
                constraintsString.indexOf(CARD.getString(), hintStartIndex + offset));

        nextHintStartIndex = nextHintStartIndex == -1 ?
                constraintsString.length() :
                nextHintStartIndex;
        String hintString = constraintsString.substring(hintStartIndex + offset, nextHintStartIndex).trim();

        return Optional.of(new HintString(hintString, constraintType, hintStartIndex, nextHintStartIndex));
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
                        c = inputReader.read();
                    }
                    name = builder.toString();
                    if (!name.isEmpty()) {
                        token = Token.NAME;
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
