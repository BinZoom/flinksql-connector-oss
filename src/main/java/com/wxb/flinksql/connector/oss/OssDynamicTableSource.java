package com.wxb.flinksql.connector.oss;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

/**
 * OssDynamicTableSource
 *
 * @author weixubin
 * @date 2022-95-24
 */
public class OssDynamicTableSource implements ScanTableSource {

    private final String endpoint;
    private final String accessKeyId;
    private final String accessKeySecret;
    private final String bucketName;
    private final String objectName;
    private final DecodingFormat<DeserializationSchema<RowData>> decodingFormat;
    private final DataType producedDataType;

    public OssDynamicTableSource(
            String endpoint,
            String accessKeyId,
            String accessKeySecret,
            String bucketName,
            String objectName,
            DecodingFormat<DeserializationSchema<RowData>> decodingFormat,
            DataType producedDataType) {
        this.endpoint = endpoint;
        this.accessKeyId = accessKeyId;
        this.accessKeySecret = accessKeySecret;
        this.bucketName = bucketName;
        this.objectName = objectName;
        this.decodingFormat = decodingFormat;
        this.producedDataType = producedDataType;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return decodingFormat.getChangelogMode();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        // 创建发布到集群的运行时类
        final DeserializationSchema<RowData> deserializer = decodingFormat.createRuntimeDecoder(
                runtimeProviderContext,
                producedDataType);

        final SourceFunction<RowData> sourceFunction = new OssSourceFunction(
                endpoint,
                accessKeyId,
                accessKeySecret,
                bucketName,
                objectName,
                deserializer);
        // 有界流
        return SourceFunctionProvider.of(sourceFunction, true);
    }

    @Override
    public DynamicTableSource copy() {
        return new OssDynamicTableSource(endpoint, accessKeyId, accessKeySecret, bucketName, objectName, decodingFormat, producedDataType);
    }

    @Override
    public String asSummaryString() {
        return "OSS Table Source";
    }
}
