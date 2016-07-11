package io.rakam.clickhouse.data;

import com.amazonaws.services.kinesis.model.Record;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Table;
import com.google.common.eventbus.EventBus;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import com.google.common.io.LittleEndianDataInputStream;
import org.rakam.clickhouse.ClickHouseConfig;
import org.rakam.clickhouse.analysis.ClickHouseMetastore;
import org.rakam.clickhouse.collection.ClickHouseEventStore;
import org.rakam.collection.FieldDependencyBuilder;
import org.rakam.collection.SchemaField;
import org.rakam.util.ProjectCollection;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.rakam.clickhouse.analysis.ClickHouseQueryExecution.readVarInt;

public class MessageTransformer
{
    private final LoadingCache<ProjectCollection, List<SchemaField>> schemaCache;

    public MessageTransformer()
    {
        ClickHouseMetastore clickHouseMetastore = new ClickHouseMetastore(new ClickHouseConfig(),
                new FieldDependencyBuilder.FieldDependency(ImmutableSet.of(), ImmutableMap.of()), new EventBus());

        schemaCache = CacheBuilder.newBuilder().build(new CacheLoader<ProjectCollection, List<SchemaField>>() {
            @Override
            public List<SchemaField> load(ProjectCollection key)
                    throws Exception
            {
                return clickHouseMetastore.getCollection(key.project, key.collection);
            }
        });
    }

    public Map<ProjectCollection, byte[]> convert(List<Record> records) throws IOException
    {
        Map<ProjectCollection, ByteArrayDataOutput> table = new HashMap<>();

        for (Record record : records) {
            ProjectCollection collection = extractCollection(record);
            byte[] data = getData(record);
            LittleEndianDataInputStream in = new LittleEndianDataInputStream(new ByteArrayInputStream(data));
            int fieldCount = readVarInt(in);

            ByteArrayDataOutput output = table.get(collection);
            if(output == null) {
                ByteArrayOutputStream out = new ByteArrayOutputStream(records.size() * 100);
                output = ByteStreams.newDataOutput(out);
                table.put(collection, output);
            }

            List<SchemaField> fields = schemaCache.getUnchecked(collection);
            output.write(data);

            if(fieldCount < fields.size()) {
                for (int i = fieldCount; i < fields.size(); i++) {
                    ClickHouseEventStore.writeValue(null, fields.get(i).getType(), output);
                }
            }
        }

        return table.entrySet().stream().collect(Collectors.toMap(
                e -> e.getKey(),
                e -> e.getValue().toByteArray()));
    }

    public ProjectCollection extractCollection(Record message)
    {
        String partitionKey = message.getPartitionKey();
        int splitterIndex = partitionKey.indexOf('|');
        String project = partitionKey.substring(0, splitterIndex);
        String collection = partitionKey.substring(splitterIndex + 1);
        return new ProjectCollection(project, collection);
    }

    public byte[] getData(Record record)
    {
        ByteBuffer data = record.getData();
        return data.array();
    }
}
