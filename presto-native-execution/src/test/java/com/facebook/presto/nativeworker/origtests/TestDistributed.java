package com.facebook.presto.nativeworker.origtests;

import com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils;
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestAggregations;
import com.facebook.presto.tests.AbstractTestDistributedQueries;

public class TestDistributed
        extends AbstractTestDistributedQueries
{
    @Override
    protected QueryRunner createQueryRunner() throws Exception
    {
        return PrestoNativeQueryRunnerUtils.createNativeQueryRunner(true);
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner() throws Exception
    {
        return PrestoNativeQueryRunnerUtils.createJavaQueryRunner();
    }

}
