package com.wxb.flinksql.connector.oss;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.*;
import org.apache.flink.table.types.DataType;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * OssTableConnecterFactory
 *
 * @author wxb
 * @date 2022-05-23
 */
public class OssTableConnecterFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    public static final ConfigOption<String> ENDPOINT = ConfigOptions.key("endpoint")
            .stringType()
            .noDefaultValue();

    public static final ConfigOption<String> ACCESS_KEY_ID = ConfigOptions.key("access-key-id")
            .stringType()
            .noDefaultValue();

    public static final ConfigOption<String> ACCESS_KEY_SECRET = ConfigOptions.key("access-key-secret")
            .stringType()
            .noDefaultValue();

    public static final ConfigOption<String> BUCKET_NAME = ConfigOptions.key("bucket-name")
            .stringType()
            .noDefaultValue();

    public static final ConfigOption<String> OBJECT_NAME = ConfigOptions.key("object-name")
            .stringType()
            .noDefaultValue();

    @Override
    public String factoryIdentifier() {
        return "oss";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(ENDPOINT);
        options.add(ACCESS_KEY_ID);
        options.add(ACCESS_KEY_SECRET);
        options.add(BUCKET_NAME);
        options.add(OBJECT_NAME);
        options.add(FactoryUtil.FORMAT);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Collections.emptySet();
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {

        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

        final DecodingFormat<DeserializationSchema<RowData>> decodingFormat = helper.discoverDecodingFormat(
                DeserializationFormatFactory.class,
                FactoryUtil.FORMAT);

        helper.validate();

        final ReadableConfig options = helper.getOptions();
        final String endpoint = options.get(ENDPOINT);
        final String accessKeyId = options.get(ACCESS_KEY_ID);
        final String accessKeySecret = options.get(ACCESS_KEY_SECRET);
        final String bucketName = options.get(BUCKET_NAME);
        final String objectName = options.get(OBJECT_NAME);

        final DataType producedDataType =
                context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType();

        return new OssDynamicTableSource(endpoint, accessKeyId, accessKeySecret, bucketName, objectName, decodingFormat, producedDataType);
    }


    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

        final EncodingFormat<SerializationSchema<RowData>> encodingFormat = helper.discoverEncodingFormat(
                SerializationFormatFactory.class,
                FactoryUtil.FORMAT);

        helper.validate();

        final ReadableConfig options = helper.getOptions();
        final String endpoint = options.get(ENDPOINT);
        final String accessKeyId = options.get(ACCESS_KEY_ID);
        final String accessKeySecret = options.get(ACCESS_KEY_SECRET);
        final String bucketName = options.get(BUCKET_NAME);
        final String objectName = options.get(OBJECT_NAME);

        final DataType producedDataType =
                context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType();
        return new OssDynamicTableSink(endpoint, accessKeyId, accessKeySecret, bucketName, objectName, encodingFormat, producedDataType);
    }
}

