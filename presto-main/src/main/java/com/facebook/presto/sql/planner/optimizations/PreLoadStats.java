package com.facebook.presto.sql.planner.optimizations;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.Session;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.split.SplitManager;
import com.facebook.presto.split.SplitSource;
import com.facebook.presto.sql.planner.PlanVariableAllocator;
import com.facebook.presto.sql.planner.TypeProvider;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_SPATIAL_PARTITIONING;
import static com.facebook.presto.spi.connector.ConnectorSplitManager.SplitSchedulingStrategy.UNGROUPED_SCHEDULING;
import static java.lang.String.format;

public class PreLoadStats
        implements PlanOptimizer
{
    private final SplitManager splitManager;
    private final Metadata metadata;
    private final Logger logger = Logger.get(PreLoadStats.class);

    public PreLoadStats(SplitManager splitManager, Metadata metadata)
    {
        this.splitManager = splitManager;
        this.metadata = metadata;
    }

    private static QualifiedObjectName toQualifiedObjectName(String name, String catalog, String schema)
    {
        ImmutableList<String> ids = ImmutableList.copyOf(Splitter.on('.').split(name));
        if (ids.size() == 3) {
            return new QualifiedObjectName(ids.get(0), ids.get(1), ids.get(2));
        }

        if (ids.size() == 2) {
            return new QualifiedObjectName(catalog, ids.get(0), ids.get(1));
        }

        if (ids.size() == 1) {
            return new QualifiedObjectName(catalog, schema, ids.get(0));
        }

        throw new PrestoException(INVALID_SPATIAL_PARTITIONING, format("Invalid name: %s", name));
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider types, PlanVariableAllocator variableAllocator,
            PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        String tableName = "t0"; //Hardcoding for now; tableHandle/ table name can be obtained from plan
        QualifiedObjectName name = toQualifiedObjectName(tableName, session.getCatalog().get(), session.getSchema().get());
        TableHandle tableHandle = metadata.getMetadataResolver(session).getTableHandle(name).get();

        //getSplits is calling BackgroundHiveSplitLoader which does file listing in the background
        try (SplitSource splitSource = splitManager.getSplits(session, tableHandle, UNGROUPED_SCHEDULING, WarningCollector.NOOP)) {
            logger.info("Initialized splits, background quick stats loading..");
        }

        return plan;
    }
}
