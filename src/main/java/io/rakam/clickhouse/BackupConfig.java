package io.rakam.clickhouse;

import io.airlift.configuration.Config;

import java.io.File;

public class BackupConfig
{
    private String bucket;
    private File directory;
    private String identifier;
    private boolean enableCloudWatch;

    public String getBucket()
    {
        return bucket;
    }

    public File getDirectory()
    {
        return directory;
    }

    @Config("data-directory")
    public BackupConfig setDirectory(File directory)
    {
        this.directory = directory;
        return this;
    }

    @Config("s3.bucket")
    public BackupConfig setBucket(String bucket) {
        this.bucket = bucket;
        return this;
    }

    public String getIdentifier()
    {
        return identifier;
    }

    @Config("node-identifier")
    public BackupConfig setIdentifier(String identifier) {
        this.identifier = identifier;
        return this;
    }

    @Config("aws.enable-cloudwatch")
    public BackupConfig setEnableCloudWatch(boolean enableCloudWatch)
    {
        this.enableCloudWatch = enableCloudWatch;
        return this;
    }

    public boolean getEnableCloudWatch()
    {
        return this.enableCloudWatch;
    }
}
