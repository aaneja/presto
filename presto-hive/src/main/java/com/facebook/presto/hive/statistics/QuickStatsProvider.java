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
import com.facebook.presto.hive.DirectoryLister;
import com.facebook.presto.hive.HdfsContext;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveDirectoryContext;
import com.facebook.presto.hive.HiveFileInfo;
import com.facebook.presto.hive.NamenodeStats;
import com.facebook.presto.hive.filesystem.ExtendedFileSystem;
import com.facebook.presto.hive.metastore.MetastoreContext;
import com.facebook.presto.hive.metastore.Partition;
import com.facebook.presto.hive.metastore.PartitionStatistics;
import com.facebook.presto.hive.metastore.SemiTransactionalHiveMetastore;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.base.Stopwatch;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.apache.hadoop.fs.Path;
import org.weakref.jmx.Managed;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.hive.HivePartition.UNPARTITIONED_ID;
import static com.facebook.presto.hive.HiveSessionProperties.isQuickStatsEnabled;
import static com.facebook.presto.hive.HiveSessionProperties.isUseListDirectoryCache;
import static com.facebook.presto.hive.HiveUtil.buildDirectoryContextProperties;
import static com.facebook.presto.hive.NestedDirectoryPolicy.IGNORED;
import static com.facebook.presto.hive.NestedDirectoryPolicy.RECURSE;
import static com.facebook.presto.hive.metastore.PartitionStatistics.empty;
import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static java.util.concurrent.ForkJoinPool.commonPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toMap;

public class QuickStatsProvider
{
    public static final Logger log = Logger.get(QuickStatsProvider.class);
    public static final long MAX_CACHE_ENTRIES = 1_000_000L;
    private static final ExecutorService backgroundFetchExecutor = Executors.newCachedThreadPool();
    private final ScheduledExecutorService inProgressReaperExecutor = Executors.newScheduledThreadPool(100);
    private final HdfsEnvironment hdfsEnvironment;
    private final DirectoryLister directoryLister;
    private final List<QuickStatsBuilderInterface> statsBuilderStrategies;
    private final boolean recursiveDirWalkerEnabled;
    private final ConcurrentHashMap<String, Instant> inProgressBuilds = new ConcurrentHashMap<>();
    private final AtomicLong requestCount = new AtomicLong(0L);
    private final AtomicLong succesfulResolveFromCacheCount = new AtomicLong(0L);
    private final AtomicLong succesfulResolveFromProviderCount = new AtomicLong(0L);
    private final long buildTimeoutMillis;
    private final Duration cacheExpiryDuration;
    private final long reaperExpiryMillis;
    private final Cache<String, PartitionStatistics> partitionToStatsCache;
    private final NamenodeStats nameNodeStats;

    @Managed
    public long getRequestCount()
    {
        return requestCount.get();
    }

    @Managed
    public long getSuccesfulResolveFromCacheCount()
    {
        return succesfulResolveFromCacheCount.get();
    }

    @Managed
    public long getSuccesfulResolveFromProviderCount()
    {
        return succesfulResolveFromProviderCount.get();
    }

    @Managed
    public Map<String, Instant> getInProgressBuildsSnapshot()
    {
        return ImmutableMap.copyOf(inProgressBuilds);
    }

    public QuickStatsProvider(HdfsEnvironment hdfsEnvironment, DirectoryLister directoryLister, HiveClientConfig hiveClientConfig, NamenodeStats nameNodeStats,
            List<QuickStatsBuilderInterface> statsBuilderStrategies)
    {
        this.hdfsEnvironment = hdfsEnvironment;
        this.directoryLister = directoryLister;
        this.recursiveDirWalkerEnabled = hiveClientConfig.getRecursiveDirWalkerEnabled();
        this.buildTimeoutMillis = hiveClientConfig.getQuickStatsBuildTimeout().roundTo(MILLISECONDS);
        this.cacheExpiryDuration = hiveClientConfig.getQuickStatsCacheExpiry();
        this.partitionToStatsCache = CacheBuilder.newBuilder()
                .maximumSize(MAX_CACHE_ENTRIES)
                .expireAfterWrite(cacheExpiryDuration.roundTo(SECONDS), SECONDS)
                .build();
        this.reaperExpiryMillis = hiveClientConfig.getQuickStatsReaperExpiry().roundTo(MILLISECONDS);
        this.nameNodeStats = nameNodeStats;
        this.statsBuilderStrategies = statsBuilderStrategies;
    }

    public Map<String, PartitionStatistics> getQuickStats(ConnectorSession session, SemiTransactionalHiveMetastore metastore, SchemaTableName table,
            MetastoreContext metastoreContext, List<String> partitionIds)
    {
        if (!isQuickStatsEnabled(session)) {
            return partitionIds.stream().collect(toMap(k -> k, v -> empty()));
        }

        CompletableFuture<PartitionStatistics>[] partitionQuickStatCompletableFutures = new CompletableFuture[partitionIds.size()];
        for (int counter = 0; counter < partitionIds.size(); counter++) {
            String partitionId = partitionIds.get(counter);
            partitionQuickStatCompletableFutures[counter] = supplyAsync(() -> getQuickStats(session, metastore, table, metastoreContext, partitionId), backgroundFetchExecutor);
        }

        try {
            // Wait for all the partitions to get their quick stats
            // If this query is reading a partition for which we do not already have cached quick stats,
            // we will block the execution of the query until the stats are fetched for all such partitions,
            // or we time out waiting for the fetch
            // TODO : Make timeout value configurable
            allOf(partitionQuickStatCompletableFutures).get(buildTimeoutMillis, MILLISECONDS);
        }
        catch (InterruptedException | ExecutionException e) {
            log.error(e);
            throw new RuntimeException(e);
        }
        catch (TimeoutException e) {
            log.warn(e, "Timeout while building quick stats");
            // TODO : Log a metric that we experienced a timeout
        }

        ImmutableMap.Builder<String, PartitionStatistics> result = ImmutableMap.builder();
        for (int counter = 0; counter < partitionQuickStatCompletableFutures.length; counter++) {
            String partitionId = partitionIds.get(counter);
            CompletableFuture<PartitionStatistics> future = partitionQuickStatCompletableFutures[counter];
            if (future.isDone() && !future.isCancelled() && !future.isCompletedExceptionally()) {
                try {
                    result.put(partitionId, future.get());
                }
                catch (InterruptedException | ExecutionException e) {
                    // This should not happen because we checked that the future was completed successfully
                    log.error(e, "Failed to get value for a quick stats future which was completed successfully");
                    throw new RuntimeException(e);
                }
            }
            else {
                // If a future did not finish, or finished exceptionally, we do not add it to the results
                // A new query for the same partition could trigger a successful quick stats fetch for this partition
                result.put(partitionId, empty());
            }
        }

        return result.build();
    }

    public PartitionStatistics getQuickStats(ConnectorSession session, SemiTransactionalHiveMetastore metastore, SchemaTableName table,
            MetastoreContext metastoreContext, String partitionId)
    {
        if (!isQuickStatsEnabled(session)) {
            return empty();
        }
        requestCount.incrementAndGet(); // New request was made to resolve quick stats for a partition

        // Check if we already have stats cached in partitionIdToQuickStatsCache. If so return from cache
        String partitionKey = String.join("/", table.toSchemaTablePrefix().toString(), partitionId);

        PartitionStatistics cachedValue = partitionToStatsCache.getIfPresent(partitionKey);
        if (cachedValue != null) {
            succesfulResolveFromCacheCount.incrementAndGet();
            return cachedValue;
        }

        // Check if we already have a quick stats build in progress for this partition key
        if (inProgressBuilds.containsKey(partitionKey)) {
            return empty();
        }

        // If not, atomically initiate a call to build quick stats in a background thread
        AtomicReference<CompletableFuture<PartitionStatistics>> partitionStatisticsCompletableFuture = new AtomicReference<>();
        inProgressBuilds.computeIfAbsent(partitionKey, (key) -> {
            partitionStatisticsCompletableFuture.set(
                    supplyAsync(() -> buildQuickStats(partitionKey, partitionId, session, metastore, table, metastoreContext), backgroundFetchExecutor));

            // Also add a hook to reap this in-progress thread if it doesn't finish in reaperExpiry seconds
            inProgressReaperExecutor.schedule(() -> {
                inProgressBuilds.remove(partitionKey);
                partitionStatisticsCompletableFuture.get().cancel(true);
            }, reaperExpiryMillis, MILLISECONDS);
            return Instant.now();
        });

        CompletableFuture<PartitionStatistics> future = partitionStatisticsCompletableFuture.get();
        // If a background call to build quick stats was started, wait for it to complete
        // This way, the first query that initiated the quick stats call for this partition will wait for the stats to be built (unless it times out)
        if (future != null) {
            try {
                PartitionStatistics partitionStatistics = future.get(buildTimeoutMillis, MILLISECONDS);
                succesfulResolveFromProviderCount.incrementAndGet(); // successfully resolved quick stats for the partition
                return partitionStatistics;
            }
            catch (InterruptedException | ExecutionException e) {
                log.error(e, "Error while building quick stats for partition : %s", partitionId);
                // Return empty PartitionStats for this partition
                return empty();
            }
            catch (TimeoutException e) {
                log.warn(e, "Timeout while building quick stats for partition : %s", partitionId);
                // Return empty PartitionStats for this partition
                return empty();
            }
            finally {
                if (future.isDone()) {
                    inProgressBuilds.remove(partitionKey);
                }
                else {
                    // Remove the in-progress flag when the background fetch finishes
                    future.whenCompleteAsync((r, e) -> inProgressBuilds.remove(partitionKey), commonPool());
                }
            }
        }

        // No quick stats calls were initiated, but we do have a quick stats build in progress
        // Return empty PartitionStats for this partition, for now
        return empty();
    }

    private PartitionStatistics buildQuickStats(String partitionKey, String partitionId, ConnectorSession session, SemiTransactionalHiveMetastore metastore, SchemaTableName table,
            MetastoreContext metastoreContext)
    {
        Table resolvedTable = metastore.getTable(metastoreContext, table.getSchemaName(), table.getTableName()).get();
        Optional<Partition> partition;
        Path path;
        if (UNPARTITIONED_ID.equals(partitionId)) {
            partition = Optional.empty();
            path = new Path(resolvedTable.getStorage().getLocation());
        }
        else {
            partition = metastore.getPartition(metastoreContext, table.getSchemaName(), table.getTableName(), ImmutableList.of(partitionId));
            path = new Path(partition.get().getStorage().getLocation());
        }

        HdfsContext hdfsContext = new HdfsContext(session, table.getSchemaName(), table.getTableName(), partitionId, false);
        HiveDirectoryContext hiveDirectoryContext = new HiveDirectoryContext(recursiveDirWalkerEnabled ? RECURSE : IGNORED, isUseListDirectoryCache(session),
                hdfsContext.getIdentity(), buildDirectoryContextProperties(session));
        ExtendedFileSystem fs;
        try {
            fs = hdfsEnvironment.getFileSystem(hdfsContext, path);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }

        // Resolve the list of files using the directory lister.
        // If the directoryLister is a CachingDirectoryLister, this listing call will get/put the call from cache
        ImmutableList<HiveFileInfo> fileList = ImmutableList.copyOf(directoryLister.list(fs, resolvedTable, path, partition, nameNodeStats, hiveDirectoryContext));

        PartitionQuickStats partitionQuickStats = PartitionQuickStats.EMPTY;
        Stopwatch buildStopwatch = Stopwatch.createStarted();
        // Build quick stats one by one from statsBuilderStrategies. Do this until we get a non-empty PartitionQuickStats
        for (QuickStatsBuilderInterface strategy : statsBuilderStrategies) {
            partitionQuickStats = strategy.buildQuickStats(session, metastore, table, metastoreContext, partitionId, fileList);

            if (partitionQuickStats != PartitionQuickStats.EMPTY) {
                // Strategy successfully resolved stats, don't explore other strategies
                // TODO : We can order the strategies based on table metadata, e.g Iceberg tables could use the IcebergQuickStatsBuilder first
                break;
            }
        }
        session.getRuntimeStats().addMetricValue("QuickStatsProvider/BuildTimeMS/" + partitionKey, RuntimeUnit.NONE, buildStopwatch.elapsed(MILLISECONDS));
        PartitionStatistics partitionStatistics = PartitionQuickStats.convertToPartitionStatistics(partitionQuickStats);

        // Update the cache with the computed partition stats
        partitionToStatsCache.put(partitionKey, partitionStatistics);

        return partitionStatistics;
    }
}
