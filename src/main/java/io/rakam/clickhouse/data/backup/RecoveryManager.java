package io.rakam.clickhouse.data.backup;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.escape.Escaper;
import com.google.common.net.UrlEscapers;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.rakam.clickhouse.BackupConfig;
import io.rakam.clickhouse.data.ClickhouseClusterShardManager;
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
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;

public class RecoveryManager
{
    private static Logger logger = Logger.get(BackupService.class);
    private final BackupService backupService;
    private final AmazonS3Client amazonS3Client;
    private final BackupConfig backupConfig;

    @Inject
    public RecoveryManager(AWSConfig awsConfig, BackupService backupService, BackupConfig backupConfig)
    {
        this.backupConfig = backupConfig;
        this.backupService = backupService;
        amazonS3Client = new AmazonS3Client();
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

        listing.getObjectSummaries().stream().map(this::extractPart)
                .forEach(parts::add);

        while (listing.isTruncated()) {
            listing = amazonS3Client.listNextBatchOfObjects(listing);

            listing.getObjectSummaries().stream().map(this::extractPart)
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
        backupService.createNewParts(missingParts.iterator());

        parts.forEach(this::recoverPartIfNotExists);
    }

    private Part extractPart(S3ObjectSummary summary)
    {
        String[] parts = summary.getKey().split("/", 4);
        String database = parts[1];
        String table = new String(Base64.getDecoder().decode(parts[2]), UTF_8);
        String part = parts[3];
        return new Part(backupConfig.getIdentifier(), database, table, part);
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

            try {
                partDirectory.mkdir();
                SnappyFramedInputStream inputStream = new SnappyFramedInputStream(object.getObjectContent());

                byte[] bytes = new byte[4];
                inputStream.read(bytes);
                int fileCount = Ints.fromByteArray(bytes);

                for (int i = 0; i < fileCount; i++) {
                    inputStream.read(bytes);
                    int nameLength = Ints.fromByteArray(bytes);

                    byte[] nameBytes = new byte[nameLength];
                    inputStream.read(nameBytes);

                    File file = new File(partDirectory, new String(nameBytes, UTF_8));
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
                logger.error(e);
                partDirectory.delete();
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
}
