package io.rakam.clickhouse.data;

import com.amazonaws.services.kinesis.model.Record;
import com.google.common.eventbus.EventBus;
import com.google.common.io.LittleEndianDataInputStream;
import com.google.common.io.LittleEndianDataOutputStream;
import org.rakam.aws.AWSConfig;
import org.rakam.aws.dynamodb.metastore.DynamodbMetastore;
import org.rakam.aws.dynamodb.metastore.DynamodbMetastoreConfig;
import org.rakam.collection.FieldDependencyBuilder;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.util.ProjectCollection;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.rakam.clickhouse.analysis.ClickHouseQueryExecution.readVarInt;
import static org.rakam.clickhouse.collection.ClickHouseEventStore.writeValue;

public class MessageTransformer
{
    private final DynamodbMetastore clickHouseMetastore;

    public MessageTransformer(AWSConfig awsConfig, DynamodbMetastoreConfig metastoreConfig)
    {
        DynamodbMetastore clickHouseMetastore = new DynamodbMetastore(awsConfig, metastoreConfig,
                new FieldDependencyBuilder().build(), new EventBus());

        this.clickHouseMetastore = clickHouseMetastore;
    }

    public Map<ProjectCollection, Map.Entry<List<SchemaField>, ZeroCopyByteArrayOutputStream>> convert(List<Record> records)
            throws IOException
    {
        Map<ProjectCollection, ByteArrayBackedLittleEndianDataOutputStream> table = new HashMap<>();

        Map<ProjectCollection, List<SchemaField>> map = new HashMap<>();

        for (Record record : records) {
            ProjectCollection collection = extractCollection(record);
            byte[] data = getData(record);
            ByteArrayInputStream in = new ByteArrayInputStream(data);
            LittleEndianDataInputStream input = new LittleEndianDataInputStream(in);
            int fieldCount = readVarInt(input);

            ByteArrayBackedLittleEndianDataOutputStream output = table.get(collection);
            if (output == null) {
                ZeroCopyByteArrayOutputStream out = new ZeroCopyByteArrayOutputStream(1000);
                output = new ByteArrayBackedLittleEndianDataOutputStream(out);
                table.put(collection, output);
            }

            output.write(data, data.length - in.available(), in.available());

            List<SchemaField> fields = map.get(collection);
            if (fields == null) {
                fields = clickHouseMetastore.getCollection(collection.project, collection.collection);
                map.put(collection, fields);
            }

            if (fieldCount < fields.size()) {
                for (int i = fieldCount; i < fields.size(); i++) {
                    FieldType type = fields.get(i).getType();
                    writeValue(null, type, output);
                }
            }
        }

        return table.entrySet().stream().collect(Collectors.toMap(
                e -> e.getKey(),
                e -> new SimpleImmutableEntry<>(
                        map.get(e.getKey()),
                        e.getValue().getUnderlyingOutputStream())));
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

    public static class ZeroCopyByteArrayOutputStream
            extends ByteArrayOutputStream
    {

        public ZeroCopyByteArrayOutputStream(int size)
        {
            super(size);
        }

        public byte[] getUnderlyingArray()
        {
            return buf;
        }
    }

    public static class ByteArrayBackedLittleEndianDataOutputStream
            extends LittleEndianDataOutputStream
    {

        private final ZeroCopyByteArrayOutputStream thisOut;

        public ByteArrayBackedLittleEndianDataOutputStream(ZeroCopyByteArrayOutputStream out)
        {
            super(out);
            this.thisOut = out;
        }

        public ZeroCopyByteArrayOutputStream getUnderlyingOutputStream()
        {
            return thisOut;
        }
    }
}
