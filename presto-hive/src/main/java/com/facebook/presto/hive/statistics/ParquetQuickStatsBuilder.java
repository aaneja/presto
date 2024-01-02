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

package com.facebook.presto.hive.statistics;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.RuntimeUnit;
import com.facebook.presto.hive.FileFormatDataSourceStats;
import com.facebook.presto.hive.HdfsContext;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveFileContext;
import com.facebook.presto.hive.HiveFileInfo;
import com.facebook.presto.hive.metastore.MetastoreContext;
import com.facebook.presto.hive.metastore.Partition;
import com.facebook.presto.hive.metastore.SemiTransactionalHiveMetastore;
import com.facebook.presto.hive.metastore.StorageFormat;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.parquet.ParquetDataSource;
import com.facebook.presto.parquet.cache.ParquetFileMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.statistics.DoubleStatistics;
import org.apache.parquet.column.statistics.FloatStatistics;
import org.apache.parquet.column.statistics.IntStatistics;
import org.apache.parquet.column.statistics.LongStatistics;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.LogicalTypeAnnotation.DateLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.LogicalTypeAnnotationVisitor;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeLogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;

import java.time.LocalDate;
import java.time.chrono.ChronoLocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

import static com.facebook.presto.hive.CacheQuota.NO_CACHE_CONSTRAINTS;
import static com.facebook.presto.hive.HivePartition.UNPARTITIONED_ID;
import static com.facebook.presto.hive.HiveSessionProperties.getReadNullMaskedParquetEncryptedValue;
import static com.facebook.presto.hive.parquet.HdfsParquetDataSource.buildHdfsParquetDataSource;
import static com.facebook.presto.hive.parquet.ParquetPageSourceFactory.createDecryptor;
import static com.facebook.presto.parquet.cache.MetadataReader.readFooter;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class ParquetQuickStatsBuilder
        implements QuickStatsBuilderInterface
{
    public static final Logger log = Logger.get(ParquetQuickStatsBuilder.class);
    private static final ExecutorService footerFetchExecutor = Executors.newCachedThreadPool();

    private static final Set<String> PARQUET_SERDE_CLASS_NAMES = ImmutableSet.<String>builder()
            .add("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe")
            .add("parquet.hive.serde.ParquetHiveSerDe")
            .build();
    private final HdfsEnvironment hdfsEnvironment;
    private final FileFormatDataSourceStats stats;
    private final long footerFetchTimeoutMillis;

    public ParquetQuickStatsBuilder(FileFormatDataSourceStats stats, HdfsEnvironment hdfsEnvironment, HiveClientConfig hiveClientConfig)
    {
        this.stats = stats;
        this.hdfsEnvironment = hdfsEnvironment;
        this.footerFetchTimeoutMillis = hiveClientConfig.getParquetQuickStatsFooterFetchTimeout().roundTo(MILLISECONDS);
    }

    private static void processColumnMetadata(ParquetMetadata parquetMetadata, Map<ColumnPath, ColumnQuickStats<?>> rolledUpColStats)
    {
        List<BlockMetaData> rowGroups = parquetMetadata.getBlocks();
        for (BlockMetaData rowGroup : rowGroups) {
            long rowCount = rowGroup.getRowCount();

            for (ColumnChunkMetaData columnChunkMetaData : rowGroup.getColumns()) {
                ColumnPath columnKey = columnChunkMetaData.getPath();
                if (columnKey.size() > 1) {
                    // We do not support reading/using stats for nested columns at the moment. These columns have a HiveColumnHandle#ColumnType == SYNTHESIZED
                    // TODO : When we do add this support, map the column handles to the parquet path to build stats for these nested columns
                    continue;
                }
                String columnName = columnKey.toArray()[0];
                PrimitiveType columnPrimitiveType = columnChunkMetaData.getPrimitiveType();

                Statistics colStats = columnChunkMetaData.getStatistics();
                long nullsCount = colStats.getNumNulls();

                ColumnType mappedType = ColumnType.SLICE;
                switch (columnPrimitiveType.getPrimitiveTypeName()) {
                    case INT64:
                        mappedType = ColumnType.LONG;
                        break;
                    case INT32:
                        mappedType = ColumnType.INTEGER;
                        break;
                    case BOOLEAN:
                        mappedType = ColumnType.BOOLEAN;
                        break;
                    case BINARY:
                        mappedType = ColumnType.SLICE;
                        break;
                    case FLOAT:
                        mappedType = ColumnType.FLOAT;
                        break;
                    case DOUBLE:
                        mappedType = ColumnType.DOUBLE;
                        break;
                    default:
                    case INT96:
                    case FIXED_LEN_BYTE_ARRAY:
                        break;
                }

                if (columnPrimitiveType.getLogicalTypeAnnotation() != null) {
                    // Use logical information to decipher stats info for specific logical types
                    Optional<ColumnType> transformed = columnPrimitiveType.getLogicalTypeAnnotation().accept(new LogicalTypeAnnotationVisitor<ColumnType>()
                    {
                        @Override
                        public Optional<ColumnType> visit(DateLogicalTypeAnnotation dateLogicalType)
                        {
                            return Optional.of(ColumnType.DATE);
                        }

                        @Override
                        public Optional<ColumnType> visit(TimeLogicalTypeAnnotation timeLogicalType)
                        {
                            return Optional.of(ColumnType.TIME);
                        }
                    });

                    if (transformed.isPresent()) {
                        mappedType = transformed.get();
                    }
                }

                switch (mappedType) {
                    case INTEGER: {
                        ColumnQuickStats<Integer> toMerge = (ColumnQuickStats<Integer>) rolledUpColStats.getOrDefault(columnKey, new ColumnQuickStats<>(columnName, Integer.class));
                        IntStatistics asIntegerStats = ((IntStatistics) colStats);
                        toMerge.setMinValue(asIntegerStats.getMin());
                        toMerge.setMaxValue(asIntegerStats.getMax());
                        toMerge.addToNullsCount(nullsCount);
                        toMerge.addToRowCount(rowCount);
                        rolledUpColStats.put(columnKey, toMerge);
                        break;
                    }
                    case LONG: {
                        ColumnQuickStats<Long> toMerge = (ColumnQuickStats<Long>) rolledUpColStats.getOrDefault(columnKey, new ColumnQuickStats<>(columnName, Long.class));
                        LongStatistics asLongStats = ((LongStatistics) colStats);
                        toMerge.setMinValue(asLongStats.getMin());
                        toMerge.setMaxValue(asLongStats.getMax());
                        toMerge.addToNullsCount(nullsCount);
                        toMerge.addToRowCount(rowCount);
                        rolledUpColStats.put(columnKey, toMerge);
                        break;
                    }

                    case DOUBLE: {
                        ColumnQuickStats<Double> toMerge = (ColumnQuickStats<Double>) rolledUpColStats.getOrDefault(columnKey, new ColumnQuickStats<>(columnName, Double.class));
                        DoubleStatistics asDoubleStats = ((DoubleStatistics) colStats);
                        toMerge.setMinValue(asDoubleStats.getMin());
                        toMerge.setMaxValue(asDoubleStats.getMax());
                        toMerge.addToNullsCount(nullsCount);
                        toMerge.addToRowCount(rowCount);
                        rolledUpColStats.put(columnKey, toMerge);
                        break;
                    }
                    case FLOAT: {
                        ColumnQuickStats<Float> toMerge = (ColumnQuickStats<Float>) rolledUpColStats.getOrDefault(columnKey, new ColumnQuickStats<>(columnName, Float.class));
                        FloatStatistics asFloatStats = ((FloatStatistics) colStats);
                        toMerge.setMinValue(asFloatStats.getMin());
                        toMerge.setMaxValue(asFloatStats.getMax());
                        toMerge.addToNullsCount(nullsCount);
                        toMerge.addToRowCount(rowCount);
                        rolledUpColStats.put(columnKey, toMerge);
                        break;
                    }
                    case BOOLEAN: {
                        ColumnQuickStats<Boolean> toMerge = (ColumnQuickStats<Boolean>) rolledUpColStats.getOrDefault(columnKey, new ColumnQuickStats<>(columnName, Boolean.class));
                        toMerge.addToNullsCount(nullsCount);
                        toMerge.addToRowCount(rowCount);
                        // TODO : Boolean stats store trueCount and falseCount
                        rolledUpColStats.put(columnKey, toMerge);
                        break;
                    }
                    case DATE: {
                        ColumnQuickStats<ChronoLocalDate> toMerge = (ColumnQuickStats<ChronoLocalDate>) rolledUpColStats.getOrDefault(columnKey,
                                new ColumnQuickStats<>(columnName, ChronoLocalDate.class));
                        IntStatistics asIntStats = ((IntStatistics) colStats);
                        toMerge.setMinValue(LocalDate.ofEpochDay(asIntStats.getMin()));
                        toMerge.setMaxValue(LocalDate.ofEpochDay(asIntStats.getMax()));
                        toMerge.addToNullsCount(nullsCount);
                        toMerge.addToRowCount(rowCount);
                        rolledUpColStats.put(columnKey, toMerge);
                        break;
                    }
                    default:
                    case SLICE: {
                        ColumnQuickStats<Slice> toMerge = (ColumnQuickStats<Slice>) rolledUpColStats.getOrDefault(columnKey, new ColumnQuickStats<>(columnName, Slice.class));
                        toMerge.addToNullsCount(nullsCount);
                        toMerge.addToRowCount(rowCount);
                        rolledUpColStats.put(columnKey, toMerge);
                        break;
                    }
                }
            }
        }
    }

    @Override
    public PartitionQuickStats buildQuickStats(ConnectorSession session, SemiTransactionalHiveMetastore metastore,
            SchemaTableName table, MetastoreContext metastoreContext, String partitionId, List<HiveFileInfo> files)
    {
        requireNonNull(session);
        requireNonNull(metastore);
        requireNonNull(table);
        requireNonNull(metastoreContext);
        requireNonNull(partitionId);
        requireNonNull(files);

        if (files.isEmpty()) {
            return PartitionQuickStats.EMPTY;
        }
        // We want to keep the number of files we use to build quick stats bounded, so that
        // 1. We can control total file IO overhead in a measurable way
        // 2. Planning time remains bounded
        // Future work here is to sample the file list, read stats and extrapolate the overall stats (TODO)
        // For now, we use all the files and report a metric on the file count
        session.getRuntimeStats().addMetricValue(String.format("ParquetQuickStatsBuilder/FileCount/%s/%s", table.getTableName(), partitionId), RuntimeUnit.NONE, files.size());

        StorageFormat storageFormat;
        if (UNPARTITIONED_ID.equals(partitionId)) {
            Table resolvedTable = metastore.getTable(metastoreContext, table.getSchemaName(), table.getTableName()).get();
            storageFormat = resolvedTable.getStorage().getStorageFormat();
        }
        else {
            Partition partition = metastore.getPartition(metastoreContext, table.getSchemaName(), table.getTableName(), ImmutableList.of(partitionId)).get();
            storageFormat = partition.getStorage().getStorageFormat();
        }

        if (!PARQUET_SERDE_CLASS_NAMES.contains(storageFormat.getSerDe())) {
            // Not a parquet table/partition
            return PartitionQuickStats.EMPTY;
        }

        CompletableFuture<ParquetMetadata>[] footerFetchCompletableFutures = new CompletableFuture[files.size()];

        for (int counter = 0; counter < files.size(); counter++) {
            HiveFileInfo file = files.get(counter);
            Path path = file.getPath();
            long fileSize = file.getLength();

            HiveFileContext hiveFileContext = new HiveFileContext(
                    true,
                    NO_CACHE_CONSTRAINTS,
                    Optional.empty(),
                    OptionalLong.of(fileSize),
                    OptionalLong.empty(),
                    OptionalLong.empty(),
                    file.getFileModifiedTime(),
                    false);

            HdfsContext hdfsContext = new HdfsContext(session, table.getSchemaName(), table.getTableName());
            Configuration configuration = hdfsEnvironment.getConfiguration(hdfsContext, path);

            footerFetchCompletableFutures[counter] = supplyAsync(() -> {
                try (FSDataInputStream inputStream = hdfsEnvironment.getFileSystem(hdfsContext, path).openFile(path, hiveFileContext);
                        ParquetDataSource parquetDataSource = buildHdfsParquetDataSource(inputStream, path, stats)) {
                    ParquetFileMetadata parquetFileMetadata = readFooter(parquetDataSource,
                            fileSize,
                            createDecryptor(configuration, path),
                            getReadNullMaskedParquetEncryptedValue(session));

                    return parquetFileMetadata.getParquetMetadata();
                }
                catch (Exception e) {
                    log.error(e);
                    throw new RuntimeException(e);
                }
            }, footerFetchExecutor);
        }

        HashMap<ColumnPath, ColumnQuickStats<?>> rolledUpColStats = new HashMap<>();
        try {
            // Wait for footer reads to finish
            CompletableFuture<Void> overallCompletableFuture = CompletableFuture.allOf(footerFetchCompletableFutures);
            overallCompletableFuture.get(footerFetchTimeoutMillis, MILLISECONDS);

            for (CompletableFuture<ParquetMetadata> future : footerFetchCompletableFutures) {
                ParquetMetadata parquetMetadata = future.get();
                processColumnMetadata(parquetMetadata, rolledUpColStats);
            }
        }
        catch (InterruptedException | ExecutionException | TimeoutException e) {
            log.error(e, "Failed to read/build stats from parquet footer");
            throw new RuntimeException(e);
        }

        if (rolledUpColStats.isEmpty()) {
            return PartitionQuickStats.EMPTY;
        }
        return new PartitionQuickStats(partitionId, rolledUpColStats.values(), files.size());
    }

    enum ColumnType
    {
        INTEGER,
        LONG,
        FLOAT,
        DOUBLE,
        SLICE,
        DATE,
        TIME,
        BOOLEAN
    }
}
