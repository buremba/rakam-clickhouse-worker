package io.rakam.clickhouse.metastore;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreamsClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.DescribeStreamRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeStreamResult;
import com.amazonaws.services.dynamodbv2.model.GetRecordsRequest;
import com.amazonaws.services.dynamodbv2.model.GetRecordsResult;
import com.amazonaws.services.dynamodbv2.model.GetShardIteratorRequest;
import com.amazonaws.services.dynamodbv2.model.GetShardIteratorResult;
import com.amazonaws.services.dynamodbv2.model.Record;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.Shard;
import com.amazonaws.services.dynamodbv2.model.ShardIteratorType;
import com.amazonaws.services.dynamodbv2.model.TrimmedDataAccessException;
import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.google.inject.Inject;
import io.airlift.http.client.RuntimeIOException;
import io.airlift.log.Logger;
import io.rakam.clickhouse.BackupConfig;
import io.rakam.clickhouse.data.KinesisRecordProcessor;
import org.rakam.aws.AWSConfig;
import org.rakam.aws.dynamodb.metastore.DynamodbMetastoreConfig;
import org.rakam.clickhouse.ClickHouseConfig;
import org.rakam.clickhouse.analysis.ClickHouseQueryExecution;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.util.ProjectCollection;
import org.rakam.util.RakamException;

import javax.annotation.PostConstruct;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.rakam.clickhouse.analysis.ClickHouseMetastore.toClickHouseType;
import static org.rakam.util.ValidationUtil.checkCollection;
import static org.rakam.util.ValidationUtil.checkTableColumn;

public class MetastoreWorkerManager
{
    private static final Logger logger = Logger.get(KinesisRecordProcessor.class);

    private final AmazonDynamoDBClient amazonDynamoDBClient;
    private final DynamodbMetastoreConfig metastoreConfig;
    private final ClickHouseConfig config;
    private AmazonDynamoDBStreamsClient streamsClient;
    private ScheduledExecutorService executor;
    private File checkpointFile;

    @Inject
    public MetastoreWorkerManager(AWSConfig awsConfig, ClickHouseConfig config, DynamodbMetastoreConfig metastoreConfig)
    {
        this.config = config;
        amazonDynamoDBClient = new AmazonDynamoDBClient(awsConfig.getCredentials());

        if (awsConfig.getDynamodbEndpoint() != null) {
            amazonDynamoDBClient.setEndpoint(awsConfig.getDynamodbEndpoint());
        }

        this.metastoreConfig = metastoreConfig;

        streamsClient =
                new AmazonDynamoDBStreamsClient(awsConfig.getCredentials());
        if (awsConfig.getDynamodbEndpoint() != null) {
            streamsClient.setEndpoint(awsConfig.getDynamodbEndpoint());
        }

        executor = Executors.newSingleThreadScheduledExecutor();
    }

    @PostConstruct
    public void run()
            throws IOException
    {
        String tableArn = amazonDynamoDBClient.describeTable(metastoreConfig.getTableName()).getTable().getLatestStreamArn();
        checkpointFile = new File("checkpoint");

        String sequenceNumber = null;
        if (checkpointFile.exists()) {
            try {
                sequenceNumber = Files.toString(checkpointFile, Charsets.UTF_8);
            }
            catch (IOException e) {
                checkpointFile.delete();
                checkpointFile.createNewFile();
            }
            // empty file
            if(sequenceNumber.isEmpty()) {
                sequenceNumber = null;
            }
        }
        else {
            checkpointFile.createNewFile();
            processAllBlocking();
        }

        DescribeStreamResult describeStreamResult =
                streamsClient.describeStream(new DescribeStreamRequest()
                        .withStreamArn(tableArn));
        List<Shard> shards =
                describeStreamResult.getStreamDescription().getShards();

        for (Shard shard : shards) {
            String shardId = shard.getShardId();

            GetShardIteratorResult getShardIteratorResult;
            try {
                getShardIteratorResult = streamsClient.getShardIterator(new GetShardIteratorRequest()
                        .withStreamArn(tableArn)
                        .withSequenceNumber(sequenceNumber)
                        .withShardId(shardId)
                        .withShardIteratorType(ShardIteratorType.AT_SEQUENCE_NUMBER));
            }
            catch (TrimmedDataAccessException e) {
                // the records are trimmed, process all and start from latest.
                getShardIteratorResult = streamsClient.getShardIterator(new GetShardIteratorRequest()
                        .withStreamArn(tableArn)
                        .withShardId(shardId)
                        .withShardIteratorType(ShardIteratorType.LATEST));

                processAllBlocking();
            }

            String iterator = getShardIteratorResult.getShardIterator();

            executor.schedule(() -> nextResults(iterator),
                    500, MILLISECONDS);
        }
    }

    private void processAllBlocking() {
        Map<String, AttributeValue> lastCheckpoint = null;
        List<Map<String, AttributeValue>> list = new ArrayList<>();
        do {
            ScanResult scan = amazonDynamoDBClient.scan(new ScanRequest()
                    .withTableName(metastoreConfig.getTableName())
                    .withConsistentRead(true)
                    .withExclusiveStartKey(lastCheckpoint));

            list.addAll(scan.getItems());

            lastCheckpoint = scan.getLastEvaluatedKey();
        } while(lastCheckpoint != null);

        process(list.stream());
    }

    private void nextResults(String iterator)
    {
        String nextIterator = processRecords(iterator);
        if (nextIterator != null) {
            executor.schedule(() -> {
                nextResults(nextIterator);
            }, 500, MILLISECONDS);
        }
    }

    private String processRecords(String nextItr)
    {
        GetRecordsResult getRecordsResult = streamsClient
                .getRecords(new GetRecordsRequest().withShardIterator(nextItr));

        List<Record> records = getRecordsResult.getRecords();
        if (!records.isEmpty()) {
            process(records.stream()
                    .map(e -> e.getDynamodb().getNewImage()));

            String sequenceNumber = records.get(records.size() - 1)
                    .getDynamodb()
                    .getSequenceNumber();

            FileWriter fileWriter = null;
            try {
                fileWriter = new FileWriter(checkpointFile, false);
                fileWriter.write(sequenceNumber);
            }
            catch (IOException e) {
                logger.error(e);
            } finally {
                if(fileWriter != null) {
                    try {
                        fileWriter.close();
                    }
                    catch (IOException e) {
                        logger.error(e);
                    }
                }

                logger.info("Final checkpoint sequence number: %s", sequenceNumber);
            }
        }

        return getRecordsResult.getNextShardIterator();
    }

    private void process(Stream<Map<String, AttributeValue>> records)
    {
        Map<ProjectCollection, List<SchemaField>> builder = new HashMap();
        records.forEach(record -> {
            String project = record.get("project").getS();
            // new project
            if (record.get("id").getS().equals("|")) {
                try {
                    ClickHouseQueryExecution.runStatement(config, format("CREATE DATABASE %s", project));
                }
                catch (RakamException e) {
                    if (!e.getMessage().contains("Code: 82")) {
                        throw e;
                    }
                }
                catch (RuntimeIOException e) {
                    logger.error(e);
                }
            }
            else {
                String collection = record.get("collection").getS();
                String name = record.get("name").getS();
                String type = record.get("type").getS();
                builder.computeIfAbsent(new ProjectCollection(project, collection),
                        k -> new ArrayList<>()).add(new SchemaField(name, FieldType.valueOf(type)));
            }
        });

        for (Map.Entry<ProjectCollection, List<SchemaField>> entry : builder.entrySet()) {
            String queryEnd = entry.getValue().stream()
                    .map(f -> format("%s %s", checkTableColumn(f.getName(), '`'), toClickHouseType(f.getType())))
                    .collect(Collectors.joining(", "));

            boolean timeActive = entry.getValue().stream().anyMatch(f -> f.getName().equals("_time") && f.getType() == FieldType.TIMESTAMP);
            if (!timeActive) {
                queryEnd += ", _time DateTime";
            }

            Optional<SchemaField> userColumn = entry.getValue().stream().filter(f -> f.getName().equals("_user")).findAny();

            String properties;
            if (userColumn.isPresent()) {
                String hashFunction = userColumn.get().getType().isNumeric() ? "intHash32" : "cityHash64";
                properties = format("ENGINE = MergeTree(`$date`, %s(_user), (`$date`, %s(_user)), 8192)", hashFunction, hashFunction);
            }
            else {
                properties = "ENGINE = MergeTree(`$date`, (`$date`), 8192)";
            }

            ProjectCollection collection = entry.getKey();
            String internalTableCreateQuery = format("CREATE TABLE %s.%s (`$date` Date, %s) %s ",
                    collection.project, checkCollection("$local_" + collection.collection, '`'), queryEnd, properties);

            String distributedTableCreateQuery = format("CREATE TABLE %s.%s AS %s.%s ENGINE = Distributed(servers, %s, %s, %s)",
                    collection.project, checkCollection(collection.collection, '`'),
                    collection.project, checkCollection("$local_" + collection.collection, '`'),
                    collection.project, checkCollection("$local_" + collection.collection, '`'),
                    userColumn.isPresent() ? userColumn.get().getName() : "rand()");

            try {
                ClickHouseQueryExecution.runStatement(config, internalTableCreateQuery);
                ClickHouseQueryExecution.runStatement(config, distributedTableCreateQuery);
            }
            catch (RakamException e) {
                if (e.getMessage().contains("Code: 44") || e.getMessage().contains("Code: 57")) {
                    for (SchemaField field : entry.getValue()) {
                        queryEnd = format("ADD COLUMN %s %s", checkTableColumn(field.getName(), '`'),
                                toClickHouseType(field.getType()));

                        String alterQueryInternalTable = format("ALTER TABLE %s.%s %s",
                                collection.project, checkCollection("$local_" + collection.collection, '`'), queryEnd);
                        String alterQueryDistributedTable = format("ALTER TABLE %s.%s %s",
                                collection.project, checkCollection(collection.collection, '`'), queryEnd);

                        try {
                            ClickHouseQueryExecution.runStatement(config, alterQueryInternalTable);
                            ClickHouseQueryExecution.runStatement(config, alterQueryDistributedTable);
                        }
                        catch (Exception e1) {
                            if (!e.getMessage().contains("Code: 44") && !e.getMessage().contains("Code: 57")) {
                                throw e1;
                            }
                        }
                    }
                }
                else {
                    throw e;
                }
            }
        }
    }
}
