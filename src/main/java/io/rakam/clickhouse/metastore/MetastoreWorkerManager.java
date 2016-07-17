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
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.http.client.RuntimeIOException;
import io.airlift.log.Logger;
import io.rakam.clickhouse.RetryDriver;
import org.rakam.aws.AWSConfig;
import org.rakam.aws.dynamodb.metastore.DynamodbMetastoreConfig;
import org.rakam.clickhouse.ClickHouseConfig;
import org.rakam.clickhouse.analysis.ClickHouseQueryExecution;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.util.ProjectCollection;
import org.rakam.util.RakamException;

import javax.annotation.PostConstruct;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.amazonaws.services.dynamodbv2.model.ShardIteratorType.LATEST;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.rakam.clickhouse.analysis.ClickHouseMetastore.toClickHouseType;
import static org.rakam.util.ValidationUtil.checkCollection;
import static org.rakam.util.ValidationUtil.checkTableColumn;

public class MetastoreWorkerManager
{
    private static final Logger logger = Logger.get(MetastoreWorkerManager.class);

    private final AmazonDynamoDBClient amazonDynamoDBClient;
    private final DynamodbMetastoreConfig metastoreConfig;
    private final ClickHouseConfig config;
    private final Set<String> activeShards;
    private String tableArn;
    private AmazonDynamoDBStreamsClient streamsClient;
    private ScheduledExecutorService executor;

    @Inject
    public MetastoreWorkerManager(AWSConfig awsConfig, ClickHouseConfig config, DynamodbMetastoreConfig metastoreConfig)
    {
        this.config = config;
        amazonDynamoDBClient = new AmazonDynamoDBClient(awsConfig.getCredentials());

        if (awsConfig.getDynamodbEndpoint() != null) {
            amazonDynamoDBClient.setEndpoint(awsConfig.getDynamodbEndpoint());
        }

        amazonDynamoDBClient.setRegion(awsConfig.getAWSRegion());

        this.metastoreConfig = metastoreConfig;

        streamsClient =
                new AmazonDynamoDBStreamsClient(awsConfig.getCredentials());
        if (awsConfig.getDynamodbEndpoint() != null) {
            streamsClient.setEndpoint(awsConfig.getDynamodbEndpoint());
        }
        streamsClient.setRegion(awsConfig.getAWSRegion());

        activeShards = new ConcurrentSkipListSet<>();
        executor = Executors.newSingleThreadScheduledExecutor();
    }

    @PostConstruct
    public void run()
            throws IOException
    {
        tableArn = amazonDynamoDBClient.describeTable(metastoreConfig.getTableName())
                .getTable().getLatestStreamArn();

        discoverShards();
        executor.scheduleAtFixedRate(this::discoverShards, 30, 30, SECONDS);

        processAllBlocking();
    }

    private void discoverShards()
    {
        DescribeStreamResult describeStreamResult =
                streamsClient.describeStream(new DescribeStreamRequest()
                        .withStreamArn(tableArn));
        List<Shard> shards =
                describeStreamResult.getStreamDescription().getShards();

        for (Shard shard : shards) {
            if (activeShards.contains(shard)) {
                continue;
            }
            String shardId = shard.getShardId();

            GetShardIteratorResult getShardIteratorResult = streamsClient.getShardIterator(new GetShardIteratorRequest()
                    .withStreamArn(tableArn)
                    .withShardId(shardId)
                    .withShardIteratorType(LATEST));

            processAllBlocking();

            String iterator = getShardIteratorResult.getShardIterator();

            executor.schedule(() -> nextResults(shard.getShardId(), iterator),
                    500, MILLISECONDS);
        }
    }

    private void processAllBlocking()
    {
        logger.info("Syncing table metadata from dynamodb");
        
        Map<String, AttributeValue> lastCheckpoint = null;
        List<Map<String, AttributeValue>> list = new ArrayList<>();
        do {
            ScanResult scan = amazonDynamoDBClient.scan(new ScanRequest()
                    .withTableName(metastoreConfig.getTableName())
                    .withConsistentRead(true)
                    .withExclusiveStartKey(lastCheckpoint));

            list.addAll(scan.getItems());

            lastCheckpoint = scan.getLastEvaluatedKey();
        }
        while (lastCheckpoint != null);

        List<String> databases = new ClickHouseQueryExecution(config, "SELECT name FROM system.databases").getResult().join()
                .getResult().stream().map(e -> e.get(0).toString())
                .collect(Collectors.toList());

        Map<ProjectCollection, List<SchemaField>> columns = new HashMap<>();
        for (Map<String, AttributeValue> record : list) {
            String project = record.get("project").getS();

            if (record.get("id").getS().equals("|")) {
                if (!databases.contains(project)) {
                    process(Stream.of(record));
                }
            }
            else {
                String collection = record.get("collection").getS();
                String name = record.get("name").getS();
                String type = record.get("type").getS();
                columns.computeIfAbsent(new ProjectCollection(project, collection),
                        k -> new ArrayList<>())
                        .add(new SchemaField(name, FieldType.valueOf(type)));
            }
        }

        for (List<Object> column : new ClickHouseQueryExecution(config, "SELECT database, table, name FROM system.columns").getResult().join().getResult()) {
            String database = column.get(0).toString();
            String table = column.get(1).toString();
            String columnName = column.get(2).toString();
            List<SchemaField> fields = columns.get(new ProjectCollection(database, table));

            if (fields != null) {
                fields.removeIf(field -> field.getName().equals(columnName));
            }
        }

        List<Map<String, AttributeValue>> objects = new ArrayList<>();
        for (Map.Entry<ProjectCollection, List<SchemaField>> entry : columns.entrySet()) {

            for (SchemaField field : entry.getValue()) {
                objects.add(ImmutableMap.of(
                        "project", new AttributeValue(entry.getKey().project),
                        "id", new AttributeValue(""),
                        "collection", new AttributeValue(entry.getKey().collection),
                        "name", new AttributeValue(field.getName()),
                        "type", new AttributeValue(field.getType().toString())));
            }
        }

        process(objects.stream());
    }

    private void nextResults(String shardId, String iterator)
    {
        String nextIterator = processRecords(iterator);
        if (nextIterator != null) {
            executor.schedule(() -> nextResults(shardId, nextIterator), 500, MILLISECONDS);
        }
        else {
            activeShards.remove(shardId);
        }
    }

    private String processRecords(String nextItr)
    {
        GetRecordsResult getRecordsResult = null;
        try {
            getRecordsResult = streamsClient
                    .getRecords(new GetRecordsRequest().withShardIterator(nextItr));

            List<Record> records = getRecordsResult.getRecords();
            if (!records.isEmpty()) {
                try {
                    RetryDriver.retry().run("metadata", () -> {
                        process(records.stream()
                                .map(e -> e.getDynamodb().getNewImage()));
                        return null;
                    });
                }
                catch (Exception e) {
                    logger.error(e);
                }
            }

            return getRecordsResult.getNextShardIterator();
        }
        catch (Exception e) {
            logger.error(e);

            if (getRecordsResult != null) {
                return getRecordsResult.getNextShardIterator();
            }
            else {
                return null;
            }
        }
    }

    public static void create(ClickHouseConfig config, String project)
    {
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

    public static void alterTable(ClickHouseConfig config, ProjectCollection collection, List<SchemaField> value)
    {
        String queryEnd = value.stream()
                .map(f -> format("%s %s", checkTableColumn(f.getName(), '`'), toClickHouseType(f.getType())))
                .collect(Collectors.joining(", "));

        boolean timeActive = value.stream().anyMatch(f -> f.getName().equals("_time") && f.getType() == FieldType.TIMESTAMP);
        if (!timeActive) {
            queryEnd += ", _time DateTime";
        }

        Optional<SchemaField> userColumn = value.stream().filter(f -> f.getName().equals("_user")).findAny();

        String properties;
        if (userColumn.isPresent()) {
            String hashFunction = userColumn.get().getType().isNumeric() ? "intHash32" : "cityHash64";
            properties = format("ENGINE = MergeTree(`$date`, %s(_user), (`$date`, %s(_user)), 8192)", hashFunction, hashFunction);
        }
        else {
            properties = "ENGINE = MergeTree(`$date`, (`$date`), 8192)";
        }

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
                for (SchemaField field : value) {
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

    private void process(Stream<Map<String, AttributeValue>> records)
    {
        Map<ProjectCollection, List<SchemaField>> builder = new HashMap();
        records.forEach(record -> {
            String project = record.get("project").getS();
            // new project
            if (record.get("id").getS().equals("|")) {
                create(config, project);
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
            alterTable(config, entry.getKey(), entry.getValue());
        }
    }
}
