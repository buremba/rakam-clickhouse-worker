package io.rakam.clickhouse;

import io.airlift.configuration.Config;

public class InstancesConfig
{
    private String instances;

    @Config("instances")
    public void setInstances(String instances)
    {
        this.instances = instances;
    }

    public String getInstances()
    {
        return instances;
    }
}
