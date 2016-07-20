package io.rakam.clickhouse.data;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.reflect.TypeToken;
import com.google.inject.Inject;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.http.client.JsonBodyGenerator;
import io.airlift.http.client.Request;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.http.client.jetty.JettyIoPool;
import io.airlift.http.client.jetty.JettyIoPoolConfig;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.rakam.clickhouse.BackupConfig;
import org.rakam.aws.AWSConfig;
import org.rakam.clickhouse.ClickHouseConfig;
import org.rakam.clickhouse.analysis.ClickHouseQueryExecution;
import org.rakam.report.QueryResult;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.annotations.BodyParam;
import org.rakam.server.http.annotations.JsonRequest;
import org.rakam.util.ProjectCollection;

import javax.ws.rs.Path;
import javax.ws.rs.core.UriBuilder;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.airlift.http.client.StringResponseHandler.createStringResponseHandler;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.rakam.clickhouse.analysis.ClickHouseQueryExecution.getSystemSocksProxy;
import static org.rakam.util.ValidationUtil.checkCollection;

@Path("/")
public class ClickhouseClusterShardManager
        extends HttpService
{
    protected static final JettyHttpClient HTTP_CLIENT = new JettyHttpClient(
            new HttpClientConfig()
                    .setConnectTimeout(new Duration(10, SECONDS))
                    .setSocksProxy(getSystemSocksProxy()), new JettyIoPool("rakam-clickhouse", new JettyIoPoolConfig().setMaxThreads(4)),
            ImmutableSet.of());
    private Logger logger = Logger.get(ClickhouseClusterShardManager.class);
    private final BackupConfig backupConfig;
    private final ClickHouseConfig config;
    private final AmazonS3Client amazonS3Client;

    @Inject
    public ClickhouseClusterShardManager(BackupConfig backupConfig, ClickHouseConfig config, AWSConfig awsConfig)
    {
        this.backupConfig = backupConfig;
        this.config = config;
        amazonS3Client = new AmazonS3Client();
        amazonS3Client.setRegion(awsConfig.getAWSRegion());

        if (awsConfig.getS3Endpoint() != null) {
            amazonS3Client.setEndpoint(awsConfig.getS3Endpoint());
        }
    }

    @Path("/move_parts")
    @JsonRequest
    public boolean move(@BodyParam List<Part> parts)
    {
        for (Part key : parts) {
            File file = Paths.get(backupConfig.getDirectory().getAbsolutePath(),
                    key.database,
                    key.table, "detached",
                    key.part).toFile();

            String s3Path = key.nodeIdentifier + "/" + key.database
                    + "/" + key.table + "/" + key.part;

            try {
                new TransferManager(amazonS3Client).download(
                        new GetObjectRequest(backupConfig.getBucket(), s3Path), file).waitForCompletion();

                amazonS3Client.deleteObject(backupConfig.getBucket(), s3Path);
            }
            catch (InterruptedException e) {
                logger.error(e);
            }
        }

        Map<ProjectCollection, List<Part>> partsByTable = parts.stream().collect(Collectors.groupingBy(new Function<Part, ProjectCollection>()
        {
            @Override
            public ProjectCollection apply(Part part)
            {
                return new ProjectCollection(part.database, part.table);
            }
        }));

        for (Map.Entry<ProjectCollection, List<Part>> entry : partsByTable.entrySet()) {
            String queryEnd = parts.stream()
                    .map(e -> format("ATTACH PART '%s'", e.part))
                    .collect(Collectors.joining(", "));

            new ClickHouseQueryExecution(config, format("ALTER TABLE %s.%s %s",
                    entry.getKey().project, checkCollection(entry.getKey().collection, '`'), queryEnd))
                    .getResult().thenAccept(result -> {
                if (result.isFailed()) {
                    logger.error(result.getError().message);
                }
            });
        }

        return true;
    }

    @Path("/master_reshard")
    @JsonRequest
    public CompletableFuture<Boolean> reshard(@BodyParam List<OpsWorksInstance> instances)
            throws URISyntaxException
    {
        if (!instances.get(0).id.equals(backupConfig.getIdentifier())) {
            return CompletableFuture.completedFuture(false);
        }

        for (String node : amazonS3Client.listObjects(new ListObjectsRequest().withBucketName(backupConfig.getBucket())
                .withPrefix("").withDelimiter("/")).getCommonPrefixes()) {
            if(!instances.stream().anyMatch(e -> e.status.equals("online") && e.id.equals(node))) {
                logger.info("Instance %s is not active. Moving partitions", node);

                Iterator<List<Part>> partitions = Iterables.partition(getPartsOfNode(node), instances.size()).iterator();
                int idx = 0;
                while(partitions.hasNext()) {
                    List<Part> next = partitions.next();
                    OpsWorksInstance opsWorksInstance = instances.get(idx++);
                    move(opsWorksInstance, next);
                }
            }
        }

        CompletableFuture<QueryResult>[] memberDataSize = instances.stream().map(member -> {
            URI address;
            try {
                address = new URI("http://" + member.private_ip + ":" + config.getAddress().getPort());
            }
            catch (URISyntaxException e) {
                throw Throwables.propagate(e);
            }
            ClickHouseConfig config = new ClickHouseConfig().setAddress(address);
            return new ClickHouseQueryExecution(config, "SELECT sum(bytes) FROM system.parts").getResult();
        }).toArray(CompletableFuture[]::new);

        return CompletableFuture.allOf(memberDataSize).handle((dummy, ex) -> {
            if (ex != null) {
                return false;
            }
            long[] sizes = Arrays.stream(memberDataSize).mapToLong(e -> {
                if (e.join().isFailed()) {
                    return -1;
                }

                return ((Number) e.join().getResult().get(0).get(0)).longValue();
            }).toArray();

            List<AbstractMap.SimpleImmutableEntry<OpsWorksInstance, Long>> list = IntStream.range(0, instances.size())
                    .mapToObj(e -> new AbstractMap.SimpleImmutableEntry<>(instances.get(e), sizes[e]))
                    .sorted((o1, o2) -> o1.getValue().compareTo(o2.getValue())).collect(Collectors.toList());

            double average = Arrays.stream(sizes).average().getAsDouble();
            double barrier = average / 2;

            for (int i = 0; i < list.size(); i++) {
                Map.Entry<OpsWorksInstance, Long> entry = list.get(i);
                long instanceSize = entry.getValue();

                if (instanceSize < barrier) {
                    // get average - sizes[i] parts from top node.

                    ArrayList<Part> parts = new ArrayList<>();
                    long totalBytes = 0;

                    int memberId = 0;
                    while (totalBytes < (average - instanceSize)) {
                        ClickHouseConfig config;
                        AbstractMap.SimpleImmutableEntry<OpsWorksInstance, Long> topMember = list.get(memberId++);

                        try {
                            config = new ClickHouseConfig().setAddress(new URI(topMember.getKey().private_ip));
                        }
                        catch (URISyntaxException e) {
                            throw com.google.common.base.Throwables.propagate(e);
                        }

                        Long targetTotal = topMember.getValue();
                        QueryResult result = new ClickHouseQueryExecution(config, format("select database, table, name, bytes from system.parts " +
                                "where active and modification_time < '%s' order by rand() limit 1000", Instant.now().minus(1, ChronoUnit.HOURS).toString()))
                                .getResult().join();

                        for (List<Object> objects : result.getResult()) {
                            totalBytes += (Long) objects.get(3);
                            parts.add(new Part(topMember.getKey().id, objects.get(0).toString(),
                                    objects.get(1).toString(), objects.get(2).toString()));
                            if ((totalBytes - instanceSize > average) || targetTotal < average) {
                                break;
                            }
                        }

                        topMember.setValue(topMember.getValue() - totalBytes);
                        entry.setValue(entry.getValue() + totalBytes);
                        Collections.sort(list, (o1, o2) -> o1.getValue().compareTo(o2.getValue()));
                    }

                    move(entry.getKey(), parts);
                }
            }

            return true;
        });
    }

    private void move(OpsWorksInstance instance, List<Part> parts) {
        JsonCodec<List<Part>> jsonCodec = JsonCodec.jsonCodec(new TypeToken<List<Part>>() {});
        try {
            HTTP_CLIENT.executeAsync(Request.builder()
                    .setUri(UriBuilder.fromPath("move_parts").host(instance.private_ip)
                            .port(8123).scheme("http").build())
                    .setMethod("POST")
                    .setBodyGenerator(JsonBodyGenerator.jsonBodyGenerator(jsonCodec, parts))
                    .build(), createStringResponseHandler()).get();
        }
        catch (InterruptedException | ExecutionException e) {
            throw Throwables.propagate(e);
        }
    }

    private List<Part> getPartsOfNode(String node)
    {
        List<Part> parts = new ArrayList<>();

        ObjectListing listing = amazonS3Client.listObjects(backupConfig.getBucket(), node);

        listing.getObjectSummaries().stream().map(e -> extractPart(backupConfig, e))
                .forEach(parts::add);

        while (listing.isTruncated()) {
            listing = amazonS3Client.listNextBatchOfObjects(listing);

            listing.getObjectSummaries().stream().map(e -> extractPart(backupConfig, e))
                    .forEach(parts::add);
        }

        return parts;
    }

    public static Part extractPart(BackupConfig backupConfig, S3ObjectSummary summary)
    {
        String[] parts = summary.getKey().split("/", 4);
        String database = parts[1];
        String table = new String(Base64.getDecoder().decode(parts[2]), UTF_8);
        String part = parts[3];
        return new Part(backupConfig.getIdentifier(), database, table, part);
    }

    public static class OpsWorksInstance
    {
        public final String public_dns_name;
        public final String private_dns_name;
        public final int backends;
        public final String ip;
        public final String private_ip;
        public final String instance_type;
        public final String status;
        public final String id;
        public final String aws_instance_id;
        public final String elastic_ip;
        public final String created_at;
        public final String booted_at;
        public final String region;
        public final String availability_zone;
        public final String infrastructure_class;

        @JsonCreator
        public OpsWorksInstance(@JsonProperty("public_dns_name") String public_dns_name,
                @JsonProperty("private_dns_name") String private_dns_name,
                @JsonProperty("backends") int backends,
                @JsonProperty("ip") String ip,
                @JsonProperty("private_ip") String private_ip,
                @JsonProperty("instance_type") String instance_type,
                @JsonProperty("status") String status,
                @JsonProperty("id") String id,
                @JsonProperty("aws_instance_id") String aws_instance_id,
                @JsonProperty("elastic_ip") String elastic_ip,
                @JsonProperty("created_at") String created_at,
                @JsonProperty("booted_at") String booted_at,
                @JsonProperty("region") String region,
                @JsonProperty("availability_zone") String availability_zone,
                @JsonProperty("infrastructure_class") String infrastructure_class)
        {
            this.public_dns_name = public_dns_name;
            this.private_dns_name = private_dns_name;
            this.backends = backends;
            this.ip = ip;
            this.private_ip = private_ip;
            this.instance_type = instance_type;
            this.status = status;
            this.id = id;
            this.aws_instance_id = aws_instance_id;
            this.elastic_ip = elastic_ip;
            this.created_at = created_at;
            this.booted_at = booted_at;
            this.region = region;
            this.availability_zone = availability_zone;
            this.infrastructure_class = infrastructure_class;
        }
    }

    public static class Part
    {
        public final String nodeIdentifier;
        public final String database;
        public final String table;
        public final String part;

        @JsonCreator
        public Part(@JsonProperty("nodeIdentifier") String nodeIdentifier,
                @JsonProperty("database") String database,
                @JsonProperty("table") String table,
                @JsonProperty("part") String part)
        {
            this.nodeIdentifier = nodeIdentifier;
            this.database = database;
            this.table = table;
            this.part = part;
        }

        @Override
        public String toString()
        {
            return "Part{" +
                    "nodeIdentifier='" + nodeIdentifier + '\'' +
                    ", database='" + database + '\'' +
                    ", table='" + table + '\'' +
                    ", part='" + part + '\'' +
                    '}';
        }
    }
}
