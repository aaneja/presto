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
package com.facebook.presto.hive.s3;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.github.monkeywie.proxyee.intercept.HttpProxyInterceptInitializer;
import com.github.monkeywie.proxyee.intercept.HttpProxyInterceptPipeline;
import com.github.monkeywie.proxyee.intercept.common.FullResponseIntercept;
import com.github.monkeywie.proxyee.server.HttpProxyServer;
import com.github.monkeywie.proxyee.server.HttpProxyServerConfig;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.net.URI;

import static com.facebook.airlift.testing.Assertions.assertInstanceOf;
import static com.facebook.presto.hive.s3.S3ConfigurationUpdater.S3_MAX_BACKOFF_TIME;
import static com.facebook.presto.hive.s3.S3ConfigurationUpdater.S3_MAX_CLIENT_RETRIES;
import static com.facebook.presto.hive.s3.S3ConfigurationUpdater.S3_MAX_RETRY_TIME;
import static org.testng.Assert.assertEquals;

public class TestS3Retries
{
    public static final int PROXY_PORT = 9999;

    private HttpProxyServer proxyServer;
    private ProxyInterceptor proxyInterceptor;

    class ProxyInterceptor
            extends HttpProxyInterceptInitializer
    {
        int requestCounter = 0;

        @Override
        public void init(HttpProxyInterceptPipeline pipeline)
        {
            pipeline.addLast(new FullResponseIntercept()
            {
                @Override
                public boolean match(HttpRequest httpRequest, HttpResponse httpResponse, HttpProxyInterceptPipeline pipeline)
                {
                    return true;
                }

                @Override
                public void handleResponse(HttpRequest httpRequest, FullHttpResponse httpResponse, HttpProxyInterceptPipeline pipeline)
                {
                    System.out.printf("[%d] HTTP %s for %s%n", ++requestCounter, httpRequest.method(), httpRequest.uri());
                    // Fake a 503 response to emulate throttling
                    httpResponse.setStatus(HttpResponseStatus.SERVICE_UNAVAILABLE);
                }
            });
        }
    }

    @BeforeClass
    public void startProxy()
    {
        HttpProxyServerConfig config = new HttpProxyServerConfig();
        config.setHandleSsl(false);
        proxyInterceptor = new ProxyInterceptor();

        proxyServer = new HttpProxyServer()
                .serverConfig(config)
                .proxyInterceptInitializer(proxyInterceptor);

        proxyServer.startAsync(PROXY_PORT);
    }

    @AfterClass
    public void stopProxy()
    {
        proxyServer.close();
    }

    @DataProvider
    public static Object[][] retryCountsProvider()
    {
        return new Object[][] {
                // Retries only from AWS SDK
                new Object[] {1, 0},
                new Object[] {2, 0},
                // Retries only from retryDriver
                new Object[] {0, 1},
                new Object[] {0, 2},
                // Retries from both - this is the current default behavior
                new Object[] {2, 2},
        };
    }

    @Test(dataProvider = "retryCountsProvider")
    public void testReadRetryCounters(int s3ClientConfigurationMaxRetries, int retryDriverRetries)
            throws Exception
    {
        System.out.printf("s3ClientConfigurationMaxRetries: %d, retryDriverRetries: %d%n", s3ClientConfigurationMaxRetries, retryDriverRetries);
        proxyInterceptor.requestCounter = 0;

        ClientConfiguration clientConfig = new ClientConfiguration()
                .withMaxErrorRetry(s3ClientConfigurationMaxRetries)
                .withProxyHost("localhost")
                // This is the port of the proxy server, not the MinIO server
                .withProxyPort(PROXY_PORT);

        AmazonS3 s3Client = AmazonS3ClientBuilder
                .standard()
                .withClientConfiguration(clientConfig)
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
                        "http://localhost:9000", // We have a MinIO container running on port 9000 for serving the actual object
                        "us-east-1"))
                .withPathStyleAccessEnabled(true)
                .withCredentials(new AWSStaticCredentialsProvider(
                        new BasicAWSCredentials("ACCESS_KEY", "SECRET_KEY")))
                .build();

        try (PrestoS3FileSystem fs = new PrestoS3FileSystem()) {
            int maxRetries = retryDriverRetries;
            Configuration configuration = new Configuration();
            configuration.set(S3_MAX_BACKOFF_TIME, "1ms");
            configuration.set(S3_MAX_RETRY_TIME, "5s");
            configuration.setInt(S3_MAX_CLIENT_RETRIES, maxRetries);
            fs.initialize(new URI("s3n://test-bucket/"), configuration);
            fs.setS3Client(s3Client);
            try (FSDataInputStream inputStream = fs.open(new Path("s3n://test-bucket/test"))) {
                inputStream.read();
            }
            catch (Throwable expected) {
                assertInstanceOf(expected, AmazonS3Exception.class);
                assertEquals(((AmazonS3Exception) expected).getStatusCode(), 503);
            }
        }

        System.out.println("Total GetObject requests made: " + proxyInterceptor.requestCounter);
    }
}
