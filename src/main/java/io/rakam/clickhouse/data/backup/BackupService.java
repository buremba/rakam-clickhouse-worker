package io.rakam.clickhouse.data.backup;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.google.common.collect.Iterators;
import com.google.common.escape.Escaper;
import com.google.common.net.UrlEscapers;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.rakam.clickhouse.BackupConfig;
import io.rakam.clickhouse.RetryDriver;
import io.rakam.clickhouse.data.ClickhouseClusterShardManager.Part;
import org.rakam.aws.AWSConfig;
import org.rakam.clickhouse.ClickHouseConfig;
import org.rakam.clickhouse.analysis.ClickHouseQueryExecution;
import org.xerial.snappy.SnappyFramedOutputStream;

import javax.annotation.PostConstruct;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.ZoneOffset.UTC;

public class BackupService
{
    private Logger logger = Logger.get(BackupService.class);
    private static DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private final ClickHouseConfig config;
    private ScheduledExecutorService service;
    private final BackupConfig backupConfig;
    private final AmazonS3Client amazonS3Client;
    private Instant checkpoint;

    @Inject
    public BackupService(ClickHouseConfig config, BackupConfig backupConfig, AWSConfig awsConfig)
    {
        this.backupConfig = backupConfig;
        this.config = config;

        amazonS3Client = new AmazonS3Client();
        if (awsConfig.getS3Endpoint() != null) {
            amazonS3Client.setEndpoint(awsConfig.getS3Endpoint());
        }
        checkpoint = Instant.now();
    }

    @PostConstruct
    public synchronized void start()
    {
        if (service != null) {
            throw new IllegalStateException();
        }

        service = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
                .setUncaughtExceptionHandler((t, e) -> logger.error(e))
                .build());

        service.scheduleAtFixedRate(() -> {
            ClickHouseQueryExecution execution = null;
            try {
                String date = FORMATTER.format(checkpoint.atOffset(UTC));
                execution = new ClickHouseQueryExecution(config,
                        format("SELECT database, table, name, CAST(remove_time AS String) != '0000-00-00 00:00:00' as removed, now() " +
                                "FROM system.parts WHERE (CAST(remove_time AS String) != '0000-00-00 00:00:00' AND remove_time >= '%s') " +
                                "OR modification_time >= '%s'", date, date));
            }
            catch (Exception e) {
                logger.error(e, "Error while generating query");
            }

            execution.getResult().whenComplete((result, ex) -> {
                if (ex != null) {
                    logger.error(ex);
                    return;
                }
                if (result.isFailed()) {
                    logger.error(result.getError().message);
                    return;
                }
                if (result.getResult().isEmpty()) {
                    return;
                }

                try {
                    synchronized (this) {
                        checkpoint = Instant.parse(result.getResult().get(0).get(4).toString());
                    }
                    Map<Boolean, List<List<Object>>> collect = result.getResult().stream()
                            .collect(Collectors.groupingBy(new Function<List<Object>, Boolean>()
                            {
                                @Override
                                public Boolean apply(List<Object> objects)
                                {
                                    return objects.get(3).equals(TRUE);
                                }
                            }));

                    if (collect.containsKey(FALSE)) {
                        createNewParts(Iterators.transform(collect.get(FALSE).iterator(), objects -> {
                            String database = objects.get(0).toString();
                            String table = objects.get(1).toString();
                            String part = objects.get(2).toString();
                            return new Part(backupConfig.getIdentifier(), database, table, part);
                        }));
                    }

                    if (collect.containsKey(TRUE)) {
                        deleteRemovedParts(collect.get(TRUE));
                    }

                    if (collect.size() > 0) {
                        logger.debug("Added %d new backups and removed %d expired backup files",
                                collect.get(TRUE), collect.get(FALSE));
                    }
                }
                catch (Exception e) {
                    logger.error(e);
                }
            });
        }, 5, 5, TimeUnit.SECONDS);
    }

    public void createNewParts(Iterator<Part> results)
    {
        while(results.hasNext()) {
            Part next = results.next();

            Escaper urlEncoder = UrlEscapers.urlFormParameterEscaper();;
            File path = Paths.get(backupConfig.getDirectory().getAbsolutePath(),
                    urlEncoder.escape(next.database),
                    urlEncoder.escape(next.table),
                    next.part).toFile();

            if (path.exists() && path.isDirectory()) {
                try {
                    RetryDriver.retry().run("backup", () -> {
                        ByteArrayInOutStream out = new ByteArrayInOutStream();

                        SnappyFramedOutputStream output = new SnappyFramedOutputStream(out);

                        File[] files = path.listFiles();
                        output.write(Ints.toByteArray(files.length));
                        for (File file : files) {
                            // write name
                            byte[] bytes = file.getName().getBytes(UTF_8);
                            output.write(Ints.toByteArray(bytes.length));
                            output.write(bytes);

                            output.write(Longs.toByteArray(file.length()));

                            // write file content
                            output.transferFrom(new FileInputStream(file));
                        }

                        output.flush();
                        ObjectMetadata objectMetadata = new ObjectMetadata();
                        objectMetadata.setContentLength(out.size());

                        String s3Path = backupConfig.getIdentifier() +
                                "/" + next.database + "/" +
                                Base64.getEncoder().encodeToString(next.table.getBytes(UTF_8)) +
                                "/" + next.part;

                        amazonS3Client.putObject(backupConfig.getBucket(), s3Path,
                                out.getInputStream(), objectMetadata);
                        return null;
                    });
                }
                catch (Exception e) {
                    logger.error(e);
                }
            }
        }
    }

    private void deleteRemovedParts(List<List<Object>> results)
    {
        List<String> keys = new ArrayList<>();

        for (List<Object> objects : results) {
            String database = objects.get(0).toString();
            String table = objects.get(1).toString();
            String part = objects.get(2).toString();
            keys.add(backupConfig.getIdentifier() + "/" + database +
                    "/" + Base64.getEncoder().encodeToString(table.getBytes(UTF_8)) +
                    "/" + part);
        }

        try {
            RetryDriver.retry().run("delete-removed-backuo", () -> {
                List<DeleteObjectsRequest.KeyVersion> keyList = keys.stream()
                        .map(e -> new DeleteObjectsRequest.KeyVersion(e))
                        .collect(Collectors.toList());

                try {
                    amazonS3Client.deleteObjects(new DeleteObjectsRequest(backupConfig.getBucket()).withKeys(keyList));
                }
                catch (Exception e) {
                    for (String key : keys) {
                        amazonS3Client.deleteObject(backupConfig.getBucket(), key);
                    }
                }

                return null;
            });
        }
        catch (Exception e) {
            logger.error(e);
        }
    }

    public class ByteArrayInOutStream
            extends ByteArrayOutputStream
    {
        public ByteArrayInOutStream()
        {
            super();
        }

        public ByteArrayInOutStream(int size)
        {
            super(size);
        }

        public ByteArrayInputStream getInputStream()
        {
            // create new ByteArrayInputStream that respect the current count
            ByteArrayInputStream in = new ByteArrayInputStream(this.buf);

            // set the buffer of the ByteArrayOutputStream
            // to null so it can't be altered anymore
            this.buf = null;

            return in;
        }
    }
}