package io.rakam.clickhouse.data;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.http.client.Request;
import io.airlift.http.client.StringResponseHandler;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.http.client.jetty.JettyIoPool;
import io.airlift.http.client.jetty.JettyIoPoolConfig;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.rakam.clickhouse.BasicMemoryBuffer;
import io.rakam.clickhouse.RetryDriver;
import io.rakam.clickhouse.StreamConfig;
import io.rakam.clickhouse.metastore.MetastoreWorkerManager;
import org.apache.bval.model.MetaBean;
import org.rakam.aws.AWSConfig;
import org.rakam.aws.dynamodb.metastore.DynamodbMetastoreConfig;
import org.rakam.clickhouse.ClickHouseConfig;
import org.rakam.clickhouse.analysis.ClickHouseQueryExecution;
import org.rakam.collection.SchemaField;
import org.rakam.util.ProjectCollection;
import org.rakam.util.ValidationUtil;

import javax.ws.rs.core.UriBuilder;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static io.airlift.http.client.StringResponseHandler.createStringResponseHandler;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.rakam.clickhouse.analysis.ClickHouseMetastore.toClickHouseType;
import static org.rakam.clickhouse.analysis.ClickHouseQueryExecution.getSystemSocksProxy;
import static org.rakam.util.ValidationUtil.checkCollection;
import static org.rakam.util.ValidationUtil.checkTableColumn;

public class KinesisRecordProcessor
        implements IRecordProcessor
{
    private static final Logger logger = Logger.get(KinesisRecordProcessor.class);
    protected static final JettyHttpClient HTTP_CLIENT = new JettyHttpClient(
            new HttpClientConfig()
                    .setConnectTimeout(new Duration(10, SECONDS))
                    .setSocksProxy(getSystemSocksProxy()), new JettyIoPool("rakam-clickhouse", new JettyIoPoolConfig()),
            ImmutableSet.of());

    private final BasicMemoryBuffer streamBuffer;
    private final MessageTransformer context;
    private final ClickHouseConfig config;

    public KinesisRecordProcessor(StreamConfig streamConfig, AWSConfig awsConfig, ClickHouseConfig config, DynamodbMetastoreConfig metastoreConfig)
    {
        this.streamBuffer = new BasicMemoryBuffer(streamConfig);
        context = new MessageTransformer(awsConfig, metastoreConfig);
        this.config = config;
    }

    @Override
    public void initialize(String shardId)
    {
        logger.info("Initialized processor for shard %s", shardId);
    }

    @Override
    public void processRecords(List<Record> records, IRecordProcessorCheckpointer checkpointer)
    {
        for (Record record : records) {
            streamBuffer.consumeRecord(record);
        }

        if (streamBuffer.shouldFlush()) {
            synchronized (this) {
                logger.info("Flushing %s records", streamBuffer.getRecords().size());

                Map<ProjectCollection, Map.Entry<List<SchemaField>, MessageTransformer.ZeroCopyByteArrayOutputStream>> pages;
                try {
                    pages = context.convert(streamBuffer.getRecords());
                }
                catch (IOException e) {
                    throw Throwables.propagate(e);
                }

                streamBuffer.clear();

                for (Map.Entry<ProjectCollection, Map.Entry<List<SchemaField>, MessageTransformer.ZeroCopyByteArrayOutputStream>> entry : pages.entrySet()) {
                    try {
                        RetryDriver.retry()
                                .run("insert", (Callable<Void>) () -> {
                                    CompletableFuture<Void> future = new CompletableFuture<>();
                                    executeRequest(entry.getKey(), entry.getValue().getKey(), entry.getValue().getValue(), future);
                                    future.join();
                                    return null;
                                });
                    }
                    catch (Exception e) {
                        logger.error(e);
                    }
                }

                try {
                    checkpointer.checkpoint();
                }
                catch (InvalidStateException | ShutdownException e) {
                    throw Throwables.propagate(e);
                }
            }
        }
    }

    private void executeRequest(ProjectCollection key, List<SchemaField> fields, MessageTransformer.ZeroCopyByteArrayOutputStream value, CompletableFuture<Void> future)
    {
        String columns = fields.stream().map(e -> checkTableColumn(e.getName(), '`')).collect(Collectors.joining(", "));

        URI uri = UriBuilder
                .fromPath("/").scheme("http").host("127.0.0.1").port(8123)
                .queryParam("query", format("INSERT INTO %s.%s (`$date`, %s) FORMAT RowBinary",
                        key.project, checkCollection(key.collection, '`'), columns)).build();

        HttpClient.HttpResponseFuture<StringResponseHandler.StringResponse> f = HTTP_CLIENT.executeAsync(Request.builder()
                .setUri(uri)
                .setMethod("POST")
                .setBodyGenerator(out -> out.write(value.getUnderlyingArray(), 0, value.size()))
                .build(), createStringResponseHandler());

        f.addListener(() -> {
            try {
                StringResponseHandler.StringResponse stringResponse = f.get(1L, MINUTES);
                if (stringResponse.getStatusCode() == 200) {
                    future.complete(null);
                }
                else {
                    boolean tableMissing = stringResponse.getBody().contains("Code: 60");
                    boolean columnMissing = stringResponse.getBody().contains("Code: 16");
                    if (tableMissing || columnMissing) {
                        List<SchemaField> missingColumns = new ClickHouseQueryExecution(config,
                                String.format("SELECT name FROM system.columns WHERE database = '%s' AND table = '%s'", key.project,
                                        ValidationUtil.checkLiteral(key.collection))).getResult().join().getResult().stream()
                                .map(e -> e.get(0).toString())
                                .filter(e -> fields.stream().noneMatch(a -> a.getName().equals(e)))
                                .map(e -> fields.stream().filter(a -> a.getName().equals(e)).findAny().get())
                                .collect(Collectors.toList());

                        MetastoreWorkerManager.alterTable(config, key, missingColumns);
                        executeRequest(key, fields, value, future);
                    }
                    else {
                        RuntimeException ex = new RuntimeException(stringResponse.getStatusMessage() + " : "
                                + stringResponse.getBody().split("\n", 2)[0]);
                        future.completeExceptionally(ex);
                    }
                }
            }
            catch (InterruptedException | ExecutionException | TimeoutException e) {
                future.completeExceptionally(e);
                logger.error(e);
            }
        }, Runnable::run);
    }

    @Override
    public void shutdown(IRecordProcessorCheckpointer checkpointer, ShutdownReason reason)
    {

    }
}

