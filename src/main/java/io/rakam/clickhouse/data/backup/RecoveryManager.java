package io.rakam.clickhouse.data.backup;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.google.common.escape.Escaper;
import com.google.common.net.UrlEscapers;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.rakam.clickhouse.BackupConfig;
import io.rakam.clickhouse.RetryDriver;
import io.rakam.clickhouse.data.ClickhouseClusterShardManager.Part;
import org.rakam.aws.AWSConfig;
import org.rakam.clickhouse.ClickHouseConfig;
import org.rakam.clickhouse.analysis.ClickHouseQueryExecution;
import org.rakam.report.QueryResult;
import org.xerial.snappy.SnappyFramedInputStream;

import javax.annotation.PostConstruct;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import static io.rakam.clickhouse.data.ClickhouseClusterShardManager.extractPart;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.FileVisitResult.CONTINUE;
import static java.nio.file.FileVisitResult.TERMINATE;

public class RecoveryManager
{
    private static Logger logger = Logger.get(RecoveryManager.class);
    private final BackupService backupService;
    private final AmazonS3Client amazonS3Client;
    private final BackupConfig backupConfig;

    @Inject
    public RecoveryManager(AWSConfig awsConfig, BackupService backupService, BackupConfig backupConfig)
    {
        this.backupConfig = backupConfig;
        this.backupService = backupService;
        amazonS3Client = new AmazonS3Client();
        amazonS3Client.setRegion(awsConfig.getAWSRegion());

        if (awsConfig.getS3Endpoint() != null) {
            amazonS3Client.setEndpoint(awsConfig.getS3Endpoint());
        }
    }

    @PostConstruct
    public void recover()
    {
        ObjectListing listing;
        try {
            listing = amazonS3Client.listObjects(backupConfig.getBucket(), backupConfig.getIdentifier());
        }
        catch (AmazonS3Exception e) {
            if (!amazonS3Client.listBuckets().stream().anyMatch(bucket -> bucket.getName().equals(backupConfig.getBucket()))) {
                amazonS3Client.createBucket(backupConfig.getBucket());
                return;
            }

            throw e;
        }

        List<Part> parts = new ArrayList<>();

        listing.getObjectSummaries().stream().map(e -> extractPart(backupConfig, e))
                .forEach(parts::add);

        while (listing.isTruncated()) {
            listing = amazonS3Client.listNextBatchOfObjects(listing);

            listing.getObjectSummaries().stream().map(e -> extractPart(backupConfig, e))
                    .forEach(parts::add);
        }

        QueryResult join = new ClickHouseQueryExecution(new ClickHouseConfig(), "SELECT database, table, name FROM system.parts")
                .getResult().join();

        List<Part> missingParts = new ArrayList<>();
        for (List<Object> objects : join.getResult()) {
            String database = objects.get(0).toString();
            String table = objects.get(1).toString();
            String partName = objects.get(2).toString();
            Part part = new Part(backupConfig.getIdentifier(), database, table, partName);

            if (!parts.contains(part)) {
                missingParts.add(part);
            }
        }

        Thread thread = new Thread(() -> {
            try {
                RetryDriver.retry().run("backup", () -> {
                    backupService.createNewParts(missingParts.iterator());
                    parts.forEach(this::recoverPartIfNotExists);
                    return null;
                });
            }
            catch (Exception e) {
                logger.error(e);
            }
        });
        thread.run();
    }

    private void recoverPartIfNotExists(Part summary)
    {
        Escaper urlEncoder = UrlEscapers.urlFormParameterEscaper();

        File partDirectory = Paths.get(backupConfig.getDirectory().getAbsolutePath(),
                urlEncoder.escape(summary.database), urlEncoder.escape(summary.table), summary.part).toFile();

        if (!partDirectory.exists()) {
            String key = summary.nodeIdentifier + "/" + summary.database + "/" +
                    Base64.getEncoder().encodeToString(summary.table.getBytes(UTF_8)) +
                    "/" + summary.part;

            S3Object object = amazonS3Client.getObject(backupConfig.getBucket(), key);

            File file = null;
            try {
                partDirectory.mkdir();
                SnappyFramedInputStream inputStream = new SnappyFramedInputStream(object.getObjectContent());

                byte[] bytes = new byte[4];
                inputStream.read(bytes);
                int fileCount = Ints.fromByteArray(bytes);

                for (int i = 0; i < fileCount; i++) {
                    inputStream.read(bytes);
                    int nameLength = Ints.fromByteArray(bytes);

                    if(nameLength <= 0) {
                        logger.error("Invalid backup file, file name bytes is not valid.: %s", summary);
                        return;
                    }

                    byte[] nameBytes = new byte[nameLength];
                    inputStream.read(nameBytes);

                    file = new File(partDirectory, new String(nameBytes, UTF_8));
                    file.createNewFile();

                    byte[] lengthArr = new byte[8];
                    inputStream.read(lengthArr);
                    long length = Longs.fromByteArray(lengthArr);

                    byte[] data = new byte[Ints.checkedCast(length)];
                    inputStream.read(data);

                    FileOutputStream output = new FileOutputStream(file);
                    output.write(data);
                    output.close();
                }

                logger.debug("Recovered part %s.%s.%s", summary.database, summary.table, summary.part);
            }
            catch (IOException e) {
                if(file != null) {
                    logger.error(e, "Error while dealing with file %s", file.getAbsolutePath());
                } else {
                    logger.error(e);
                }

                deleteAllFiles(partDirectory.toPath());
            }
            finally {
                try {
                    object.close();
                }
                catch (IOException e) {
                    logger.error(e);
                }
            }
        }
    }

    private void deleteAllFiles(Path partDirectory) {
        try {
            Files.walkFileTree(partDirectory, new SimpleFileVisitor<Path>()
            {
                @Override
                public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs)
                        throws IOException
                {
                    Files.delete(file);
                    return CONTINUE;
                }

                @Override
                public FileVisitResult visitFileFailed(final Path file, final IOException e)
                {
                    return handleException(e);
                }

                private FileVisitResult handleException(final IOException e)
                {
                    e.printStackTrace(); // replace with more robust error handling
                    return TERMINATE;
                }

                @Override
                public FileVisitResult postVisitDirectory(final Path dir, final IOException e)
                        throws IOException
                {
                    if (e != null) {
                        return handleException(e);
                    }
                    Files.delete(dir);
                    return CONTINUE;
                }
            });

            partDirectory.toFile().delete();
        }
        catch (IOException e1) {
            logger.error(e1);
        }
    }
}
