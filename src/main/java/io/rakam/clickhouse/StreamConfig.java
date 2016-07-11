package io.rakam.clickhouse;

import io.airlift.configuration.Config;
import io.airlift.units.DataSize;

import java.time.Duration;

import static io.airlift.units.DataSize.Unit.GIGABYTE;

public class StreamConfig
{
    private Duration maxFlushDuration = Duration.ofSeconds(5);
    private int maxFlushRecords = 50_000;
    private String appName;

    @Config("stream.max-flush-duration")
    public void setMaxFlushDuration(Duration maxFlushDuration)
    {
        this.maxFlushDuration = maxFlushDuration;
    }

    @Config("metastore.adapter.dynamodb.app_name")
    public void setAppName(String appName)
    {
        this.appName = appName;
    }

    public String getAppName()
    {
        return appName;
    }

    @Config("stream.max-flush-records")
    public void setMaxFlushRecords(int maxFlushRecords)
    {
        this.maxFlushRecords = maxFlushRecords;
    }

    public Duration getMaxFlushDuration()
    {
        return maxFlushDuration;
    }

    public int getMaxFlushRecords()
    {
        return maxFlushRecords;
    }
}

