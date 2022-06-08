package com.wxb.flinksql.connector.oss;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

/**
 * OssDynamicTableSource
 *
 * @author weixubin
 * @date 2022-95-24
 */
public class OssDynamicTableSink implements DynamicTableSink {

    private final String endpoint;
    private final String accessKeyId;
    private final String accessKeySecret;
    private final String bucketName;
    private final String objectName;
    private final EncodingFormat<SerializationSchema<RowData>> encodingFormat;
    private final DataType producedDataType;

    public OssDynamicTableSink(
            String endpoint,
            String accessKeyId,
            String accessKeySecret,
            String bucketName,
            String objectName,
            EncodingFormat<SerializationSchema<RowData>> encodingFormat,
            DataType producedDataType) {
        this.endpoint = endpoint;
        this.accessKeyId = accessKeyId;
        this.accessKeySecret = accessKeySecret;
        this.bucketName = bucketName;
        this.objectName = objectName;
        this.encodingFormat = encodingFormat;
        this.producedDataType = producedDataType;
    }


    @Override
    public ChangelogMode getChangelogMode(ChangelogMode changelogMode) {
        return encodingFormat.getChangelogMode();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context runtimeProviderContext) {
        final SerializationSchema<RowData> serializer = encodingFormat.createRuntimeEncoder(
                runtimeProviderContext,
                producedDataType);

        final SinkFunction<RowData> sinkFunction = new OssSinkFunction(
                endpoint,
                accessKeyId,
                accessKeySecret,
                bucketName,
                objectName,
                serializer);

        return SinkFunctionProvider.of(sinkFunction, 1);
    }

    @Override
    public DynamicTableSink copy() {
        return new OssDynamicTableSink(endpoint, accessKeyId, accessKeySecret, bucketName, objectName, encodingFormat, producedDataType);
    }

    @Override
    public String asSummaryString() {
        return "OSS Table Sink";
    }
}
