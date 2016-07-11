package io.rakam.clickhouse.data;

import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import io.rakam.clickhouse.BackupConfig;
import io.rakam.clickhouse.StreamConfig;
import org.rakam.aws.AWSConfig;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.rmi.dgc.VMID;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;

import static com.amazonaws.services.kinesis.metrics.interfaces.MetricsLevel.NONE;
import static com.amazonaws.services.kinesis.metrics.interfaces.MetricsLevel.SUMMARY;

public class KinesisWorkerManager
{
    private final AWSConfig config;
    private final AmazonKinesisClient kinesisClient;
    private final List<Thread> threads;
    private final IRecordProcessorFactory recordProcessorFactory;
    private final BackupConfig backupConfig;

    @Inject
    public KinesisWorkerManager(AWSConfig config, BackupConfig backupConfig, StreamConfig streamConfig)
    {
        this.config = config;
        this.backupConfig = backupConfig;
        this.threads = new ArrayList<>();
        this.kinesisClient = new AmazonKinesisClient(config.getCredentials());
        // SEE: https://github.com/awslabs/amazon-kinesis-client/issues/34
        if (config.getDynamodbEndpoint() == null && config.getKinesisEndpoint() == null) {
            kinesisClient.setRegion(config.getAWSRegion());
        }
        if (config.getKinesisEndpoint() != null) {
            kinesisClient.setEndpoint(config.getKinesisEndpoint());
        }
        KinesisUtil.createAndWaitForStreamToBecomeAvailable(kinesisClient, config.getEventStoreStreamName(), 1);
        this.recordProcessorFactory = () -> new KinesisRecordProcessor(streamConfig);
    }

    @PostConstruct
    public void initializeWorker()
    {
        Thread middlewareWorker = createMiddlewareWorker();
        middlewareWorker.start();
        threads.add(middlewareWorker);
    }

    private Worker getWorker(IRecordProcessorFactory factory, KinesisClientLibConfiguration config)
    {
        AmazonKinesisClient amazonKinesisClient = new AmazonKinesisClient(config.getKinesisCredentialsProvider(),
                config.getKinesisClientConfiguration());
        AmazonDynamoDBClient dynamoDBClient = new AmazonDynamoDBClient(config.getDynamoDBCredentialsProvider(),
                config.getDynamoDBClientConfiguration());
        if (this.config.getDynamodbEndpoint() != null) {
            dynamoDBClient.setEndpoint(this.config.getDynamodbEndpoint());
        }

        config.withMetricsLevel(backupConfig.getEnableCloudWatch() ? SUMMARY : NONE);

        AmazonCloudWatchClient client = new AmazonCloudWatchClient(config.getCloudWatchCredentialsProvider(),
                config.getCloudWatchClientConfiguration());

        return new Worker(factory, config, amazonKinesisClient, dynamoDBClient, client, Executors.newCachedThreadPool());
    }

    private Thread createMiddlewareWorker()
    {
        KinesisClientLibConfiguration configuration = new KinesisClientLibConfiguration("clickhouse-kinesis-kinesis-consumer",
                config.getEventStoreStreamName(),
                this.config.getCredentials(),
                new VMID().toString())
                .withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON)
                .withUserAgent("rakam-middleware-consumer")
                .withCallProcessRecordsEvenForEmptyRecordList(true);
        if (this.config.getKinesisEndpoint() == null & this.config.getDynamodbEndpoint() == null) {
            configuration.withRegionName(this.config.getRegion());
        }

        if (config.getKinesisEndpoint() != null) {
            configuration.withKinesisEndpoint(config.getKinesisEndpoint());
        }

        Thread middlewareWorkerThread;
        try {
            Worker worker = getWorker(recordProcessorFactory, configuration);

            middlewareWorkerThread = new Thread(worker);
        }
        catch (Exception e) {
            throw new RuntimeException("Error creating Kinesis stream worker", e);
        }

        middlewareWorkerThread.setName("middleware-consumer-thread");
        return middlewareWorkerThread;
    }

    @PreDestroy
    public void destroyWorkers()
    {
        threads.forEach(Thread::interrupt);
    }
}