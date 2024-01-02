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

import com.facebook.presto.hive.DirectoryLister;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveColumnConverterProvider;
import com.facebook.presto.hive.MetastoreClientConfig;
import com.facebook.presto.hive.NamenodeStats;
import com.facebook.presto.hive.metastore.MetastoreContext;
import com.facebook.presto.hive.metastore.Partition;
import com.facebook.presto.hive.metastore.PartitionStatistics;
import com.facebook.presto.hive.metastore.SemiTransactionalHiveMetastore;
import com.facebook.presto.hive.metastore.Storage;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.testing.TestingConnectorSession;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.Duration;
import org.mockito.stubbing.Answer;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.hive.HiveSessionProperties.QUICK_STATS_ENABLED;
import static com.facebook.presto.hive.HiveSessionProperties.USE_LIST_DIRECTORY_CACHE;
import static com.facebook.presto.hive.HiveStorageFormat.PARQUET;
import static com.facebook.presto.hive.HiveTestUtils.createTestHdfsEnvironment;
import static com.facebook.presto.hive.metastore.PartitionStatistics.empty;
import static com.facebook.presto.hive.metastore.PrestoTableType.MANAGED_TABLE;
import static com.facebook.presto.hive.metastore.StorageFormat.fromHiveStorageFormat;
import static com.facebook.presto.hive.statistics.PartitionQuickStats.convertToPartitionStatistics;
import static com.facebook.presto.spi.session.PropertyMetadata.booleanProperty;
import static java.util.Collections.emptyIterator;
import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static java.util.concurrent.ForkJoinPool.commonPool;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestQuickStatsProvider
{
    public static final ConnectorSession SESSION = new TestingConnectorSession(ImmutableList.of(booleanProperty(
                    QUICK_STATS_ENABLED,
                    "Use quick stats to resolve stats",
                    true,
                    false),
            booleanProperty(
                    USE_LIST_DIRECTORY_CACHE,
                    "Directory list caching",
                    false,
                    false)));

    public static final String TEST_TABLE = "test_table";
    public static final String TEST_SCHEMA = "test_schema";
    private final HiveClientConfig hiveClientConfig = new HiveClientConfig().setRecursiveDirWalkerEnabled(true);
    private HdfsEnvironment hdfsEnvironment;
    private DirectoryLister directoryListerMock;
    private SemiTransactionalHiveMetastore metastoreMock;
    private MetastoreContext metastoreContext;
    private PartitionQuickStats mockPartitionQuickStats;
    private PartitionStatistics expectedPartitionStats;

    @BeforeTest
    public void setUp()
    {
        metastoreMock = mock(SemiTransactionalHiveMetastore.class);
        metastoreContext = new MetastoreContext(SESSION.getUser(),
                SESSION.getQueryId(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                false,
                HiveColumnConverterProvider.DEFAULT_COLUMN_CONVERTER_PROVIDER);
        when(metastoreMock.getPartition(any(), eq(TEST_SCHEMA), eq(TEST_TABLE), any()))
                .thenReturn(Optional.of(new Partition(
                        TEST_SCHEMA,
                        TEST_TABLE,
                        ImmutableList.of(),
                        new Storage(
                                fromHiveStorageFormat(PARQUET),
                                "some/path",
                                Optional.empty(),
                                true,
                                ImmutableMap.of(),
                                ImmutableMap.of()),
                        ImmutableList.of(),
                        ImmutableMap.of(),
                        Optional.empty(),
                        false,
                        true,
                        0,
                        0)));

        when(metastoreMock.getTable(any(), eq(TEST_SCHEMA), eq(TEST_TABLE)))
                .thenReturn(Optional.of(new Table(
                        TEST_SCHEMA,
                        TEST_TABLE,
                        "user_name",
                        MANAGED_TABLE,
                        new Storage(fromHiveStorageFormat(PARQUET),
                                "some/location",
                                Optional.empty(),
                                false,
                                ImmutableMap.of(),
                                ImmutableMap.of()),
                        ImmutableList.of(),
                        ImmutableList.of(),
                        ImmutableMap.of(),
                        Optional.empty(),
                        Optional.empty())));

        directoryListerMock = mock(DirectoryLister.class);
        when(directoryListerMock.list(any(), any(), any(), any(), any(), any())).thenReturn(emptyIterator());

        MetastoreClientConfig metastoreClientConfig = new MetastoreClientConfig();
        hdfsEnvironment = createTestHdfsEnvironment(hiveClientConfig, metastoreClientConfig);

        ColumnQuickStats<Integer> mockIntegerColumnStats = new ColumnQuickStats<>("column", Integer.class);
        mockIntegerColumnStats.setMinValue(Integer.MIN_VALUE);
        mockIntegerColumnStats.setMaxValue(Integer.MAX_VALUE);
        mockIntegerColumnStats.addToRowCount(4242L);
        mockPartitionQuickStats = new PartitionQuickStats("partitionId", ImmutableList.of(mockIntegerColumnStats), 42);
        expectedPartitionStats = convertToPartitionStatistics(mockPartitionQuickStats);
    }

    @Test
    public void testReadThruCaching()
    {
        QuickStatsBuilderInterface quickStatsBuilderMock = mock(QuickStatsBuilderInterface.class);
        when(quickStatsBuilderMock.buildQuickStats(any(), any(), any(), any(), any(), any()))
                .thenReturn(mockPartitionQuickStats);

        QuickStatsProvider quickStatsProvider = new QuickStatsProvider(hdfsEnvironment, directoryListerMock, hiveClientConfig, new NamenodeStats(),
                ImmutableList.of(quickStatsBuilderMock));

        // Execute
        ImmutableList<String> testPartitions1 = ImmutableList.of("partition1", "partition2", "partition3");
        Map<String, PartitionStatistics> quickStats = quickStatsProvider.getQuickStats(SESSION, metastoreMock,
                new SchemaTableName(TEST_SCHEMA, TEST_TABLE), metastoreContext, testPartitions1);

        // Verify only one call was made for each test partition
        assertEquals(quickStats.entrySet().size(), testPartitions1.size());
        assertTrue(quickStats.keySet().containsAll(testPartitions1));
        quickStats.values().forEach(ps -> assertEquals(ps, expectedPartitionStats));
        for (String testPartition : testPartitions1) {
            verify(quickStatsBuilderMock).buildQuickStats(any(), any(), any(), any(), eq(testPartition), any());
        }
        verifyNoMoreInteractions(quickStatsBuilderMock);

        // For subsequent calls for the same partitions that are already cached, no new calls are mode to the quick stats builder
        quickStatsProvider.getQuickStats(SESSION, metastoreMock,
                new SchemaTableName(TEST_SCHEMA, TEST_TABLE), metastoreContext, testPartitions1);
        verifyNoMoreInteractions(quickStatsBuilderMock);

        // For subsequent calls with a mix of old and new partitions, we only see calls to the quick stats builder for the new partitions
        ImmutableList<String> testPartitions2 = ImmutableList.of("partition4", "partition5", "partition6");
        ImmutableList<String> testPartitionsMix = ImmutableList.<String>builder().addAll(testPartitions1).addAll(testPartitions2).build();

        quickStats = quickStatsProvider.getQuickStats(SESSION, metastoreMock,
                new SchemaTableName(TEST_SCHEMA, TEST_TABLE), metastoreContext, testPartitionsMix);
        assertEquals(quickStats.entrySet().size(), testPartitionsMix.size());
        assertTrue(quickStats.keySet().containsAll(testPartitionsMix));
        quickStats.values().forEach(ps -> assertEquals(ps, expectedPartitionStats));
        for (String testPartition : testPartitions2) {
            verify(quickStatsBuilderMock).buildQuickStats(any(), any(), any(), any(), eq(testPartition), any());
        }
        verifyNoMoreInteractions(quickStatsBuilderMock);
    }

    @Test(invocationCount = 3)
    public void testConcurrentFetchForSamePartition()
            throws ExecutionException, InterruptedException
    {
        QuickStatsBuilderInterface longRunningQuickStatsBuilderMock = mock(QuickStatsBuilderInterface.class);
        when(longRunningQuickStatsBuilderMock.buildQuickStats(any(), any(), any(), any(), any(), any()))
                .thenAnswer((Answer<PartitionQuickStats>) invocationOnMock -> {
                    // Sleep for 1.5s to simulate a long-running quick stats call
                    Thread.sleep(1500);
                    return mockPartitionQuickStats;
                });

        QuickStatsProvider quickStatsProvider = new QuickStatsProvider(hdfsEnvironment, directoryListerMock, hiveClientConfig, new NamenodeStats(),
                ImmutableList.of(longRunningQuickStatsBuilderMock));

        List<String> testPartitions = ImmutableList.of("partition1", "partition2", "partition3");

        // Execute two concurrent calls for the same partitions; wait for them to complete
        CompletableFuture<Map<String, PartitionStatistics>> future1 = supplyAsync(() -> quickStatsProvider.getQuickStats(SESSION, metastoreMock,
                new SchemaTableName(TEST_SCHEMA, TEST_TABLE), metastoreContext, testPartitions), commonPool());

        CompletableFuture<Map<String, PartitionStatistics>> future2 = supplyAsync(() -> quickStatsProvider.getQuickStats(SESSION, metastoreMock,
                new SchemaTableName(TEST_SCHEMA, TEST_TABLE), metastoreContext, testPartitions), commonPool());

        allOf(future1, future2).join();

        // Verify only one call was made for each test partition
        for (String testPartition : testPartitions) {
            verify(longRunningQuickStatsBuilderMock).buildQuickStats(any(), any(), any(), any(), eq(testPartition), any());
        }
        verifyNoMoreInteractions(longRunningQuickStatsBuilderMock);

        Map<String, PartitionStatistics> quickStats1 = future1.get();
        Map<String, PartitionStatistics> quickStats2 = future2.get();

        // Both PartitionStatistics have stats for all partitions
        assertEquals(quickStats1.entrySet().size(), testPartitions.size());
        assertTrue(quickStats1.keySet().containsAll(testPartitions));
        assertEquals(quickStats2.entrySet().size(), testPartitions.size());
        assertTrue(quickStats2.keySet().containsAll(testPartitions));

        // For the same partition, we will observe that one of them has the expected partition stats and the other one is empty
        for (String testPartition : testPartitions) {
            PartitionStatistics partitionStatistics1 = quickStats1.get(testPartition);
            PartitionStatistics partitionStatistics2 = quickStats2.get(testPartition);

            if (partitionStatistics1.equals(empty())) {
                assertEquals(partitionStatistics2, expectedPartitionStats);
            }
            else if (partitionStatistics2.equals(empty())) {
                assertEquals(partitionStatistics1, expectedPartitionStats);
            }
            else {
                fail(String.format("For [%s] partitionExpected one of the partitions stats to be empty. Actual partitionStatistics1 [%s], partitionStatistics2 [%s]",
                        testPartition, partitionStatistics1, partitionStatistics2));
            }
        }

        // Future calls for the same partitions will return from cached partition stats with valid values
        Map<String, PartitionStatistics> quickStats = quickStatsProvider.getQuickStats(SESSION, metastoreMock,
                new SchemaTableName(TEST_SCHEMA, TEST_TABLE), metastoreContext, testPartitions);

        // Verify only one call was made for each test partition
        assertEquals(quickStats.entrySet().size(), testPartitions.size());
        assertTrue(quickStats.keySet().containsAll(testPartitions));
        quickStats.values().forEach(ps -> assertEquals(ps, expectedPartitionStats));

        verifyNoMoreInteractions(longRunningQuickStatsBuilderMock);
    }

    @Test
    public void quickStatsBuildTimeIsBounded()
    {
        QuickStatsBuilderInterface longRunningQuickStatsBuilderMock = mock(QuickStatsBuilderInterface.class);
        ImmutableMap<String, Integer> mockPerPartitionStatsFetchTimes = ImmutableMap.of("p1", 10, "p2", 20, "p3", 500, "p4", 800);
        for (Map.Entry<String, Integer> partitionToSleepEntry : mockPerPartitionStatsFetchTimes.entrySet()) {
            when(longRunningQuickStatsBuilderMock.buildQuickStats(any(), any(), any(), any(), eq(partitionToSleepEntry.getKey()), any()))
                    .thenAnswer((Answer<PartitionQuickStats>) invocationOnMock -> {
                        Thread.sleep(partitionToSleepEntry.getValue());
                        return mockPartitionQuickStats;
                    });
        }

        HiveClientConfig customHiveClientConfig = new HiveClientConfig().setQuickStatsBuildTimeout(new Duration(300, TimeUnit.MILLISECONDS));
        QuickStatsProvider quickStatsProvider = new QuickStatsProvider(hdfsEnvironment, directoryListerMock, customHiveClientConfig, new NamenodeStats(),
                ImmutableList.of(longRunningQuickStatsBuilderMock));

        List<String> testPartitions = ImmutableList.copyOf(mockPerPartitionStatsFetchTimes.keySet());
        Map<String, PartitionStatistics> quickStats = quickStatsProvider.getQuickStats(SESSION, metastoreMock,
                new SchemaTableName(TEST_SCHEMA, TEST_TABLE), metastoreContext, testPartitions);
        Map<String, Instant> inProgressBuildsSnapshot = quickStatsProvider.getInProgressBuildsSnapshot();

        assertEquals(quickStats.size(), 4);
        assertEquals(quickStats.get("p1"), convertToPartitionStatistics(mockPartitionQuickStats));
        assertEquals(quickStats.get("p2"), convertToPartitionStatistics(mockPartitionQuickStats));
        // Since fetching quick stats for p3 and p4 would take > 300ms, we would return EMPTY partition stats for them
        assertEquals(quickStats.get("p3"), empty());
        assertEquals(quickStats.get("p4"), empty());

        // Snapshot of the in-progress builds confirms that quickstats build for these partitions is in progress
        String p3CacheKey = String.format("%s.%s/%s", TEST_SCHEMA, TEST_TABLE, "p3");
        String p4CacheKey = String.format("%s.%s/%s", TEST_SCHEMA, TEST_TABLE, "p4");
        assertEquals(inProgressBuildsSnapshot.keySet(), ImmutableSet.of(p3CacheKey, p4CacheKey));
    }
}
