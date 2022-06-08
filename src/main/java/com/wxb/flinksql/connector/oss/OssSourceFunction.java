package com.wxb.flinksql.connector.oss;

import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.model.OSSObject;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.RuntimeContextInitializationContextAdapters;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.data.RowData;

import java.io.BufferedReader;
import java.io.InputStreamReader;

/**
 * OssSourceFunction
 *
 * @author wxb
 * @date 2022-05-23
 */
public class OssSourceFunction extends RichSourceFunction<RowData> implements ResultTypeQueryable<RowData> {

    private final String endpoint;
    private final String accessKeyId;
    private final String accessKeySecret;
    private final String bucketName;
    private final String objectName;
    private final DeserializationSchema<RowData> deserializer;
    private OSS currentOssClient;

    public OssSourceFunction(String endpoint, String accessKeyId, String accessKeySecret, String bucketName, String objectName, DeserializationSchema<RowData> deserializer) {
        this.endpoint = endpoint;
        this.accessKeyId = accessKeyId;
        this.accessKeySecret = accessKeySecret;
        this.bucketName = bucketName;
        this.objectName = objectName;
        this.deserializer = deserializer;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return deserializer.getProducedType();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        deserializer.open(RuntimeContextInitializationContextAdapters.deserializationAdapter(
                getRuntimeContext()));
        currentOssClient = new OSSClientBuilder().build(endpoint, accessKeyId, accessKeySecret);
    }

    @Override
    public void run(SourceContext<RowData> ctx) throws Exception {
        OSSObject ossObject = null;
        BufferedReader reader = null;
        try {
            ossObject = currentOssClient.getObject(bucketName, objectName);
            reader = new BufferedReader(new InputStreamReader(ossObject.getObjectContent()));
            while (true) {
                // read stream
                String line = reader.readLine();
                if (line == null) {
                    break;
                } else {
                    ctx.collect(deserializer.deserialize(line.getBytes()));
                }
            }
        } catch (Throwable t) {
            t.printStackTrace();
        } finally {
            if (reader != null) {
                reader.close();
            }
            if (ossObject != null) {
                ossObject.close();
            }
        }
    }

    @Override
    public void cancel() {
        try {
            currentOssClient.shutdown();
        } catch (Throwable t) {
            // ignore
        }
    }
}
