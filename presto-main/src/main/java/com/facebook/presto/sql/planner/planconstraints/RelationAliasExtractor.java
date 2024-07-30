package com.facebook.presto.sql.planner.planconstraints;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.SourceLocation;
import com.facebook.presto.sql.tree.AliasedRelation;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NodeLocation;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.Table;
import com.facebook.presto.sql.tree.WithQuery;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import java.util.NavigableMap;
import java.util.TreeMap;

public class RelationAliasExtractor
{
    public static Logger log = Logger.get(RelationAliasExtractor.class);

    public static NavigableMap<SourceLocation, String> extractRelationAliases(Statement statement)
    {
        AliasLocationVisitor aliasExtractor = new AliasLocationVisitor();
        aliasExtractor.process(statement);
        TreeMap<SourceLocation, String> sourceLocationAliasMap = aliasExtractor.getSourceLocationAliasMap();
        log.info("Extracted relational aliases : [%s]", sourceLocationAliasMap);
        return sourceLocationAliasMap;
    }

    @VisibleForTesting
    public static class AliasLocationVisitor
            extends DefaultTraversalVisitor<Void, Void>
    {
        TreeMap<SourceLocation, String> sourceLocationAliasMap = new TreeMap<>();

        private void recordAlias(Node node, String alias)
        {
            Preconditions.checkState(node.getLocation().isPresent(), "Node location is missing for node %s", node);
            NodeLocation nodeLocation = node.getLocation().get();
            SourceLocation asSourceLocation = new SourceLocation(nodeLocation.getLineNumber(), nodeLocation.getColumnNumber());
            if (!sourceLocationAliasMap.containsKey(asSourceLocation)) {
                sourceLocationAliasMap.put(asSourceLocation, alias);
            }
        }

        public TreeMap<SourceLocation, String> getSourceLocationAliasMap()
        {
            return sourceLocationAliasMap;
        }

        @Override
        protected Void visitAliasedRelation(AliasedRelation node, Void context)
        {
            String alias = node.getAlias().toString();
            recordAlias(node, alias);

            return super.visitAliasedRelation(node, context);
        }

        @Override
        protected Void visitWithQuery(WithQuery node, Void context)
        {
            String alias = node.getName().toString();
            recordAlias(node, alias);
            return super.visitWithQuery(node, context);
        }

        @Override
        protected Void visitTable(Table node, Void context)
        {
            String alias = node.getName().toString();
            recordAlias(node, alias);
            return super.visitTable(node, context);
        }
    }
}
