package com.wxb.flinksql.connector.oss;

import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.model.AppendObjectRequest;
import com.aliyun.oss.model.AppendObjectResult;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.api.common.serialization.RuntimeContextInitializationContextAdapters;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.io.ByteArrayInputStream;


/**
 * OssSinkFunction
 *
 * @author wxb
 * @date 2022-05-23
 */
public class OssSinkFunction<IN> extends RichSinkFunction<IN> {

    private final String endpoint;
    private final String accessKeyId;
    private final String accessKeySecret;
    private final String bucketName;
    private final String objectName;
    private final SerializationSchema<IN> serializer;

    private OSS currentOssClient;
    private boolean isFirstRow = true;
    private AppendObjectRequest appendObjectRequest;
    private AppendObjectResult appendObjectResult;

    /**
     * \n character corresponds to byte 10
     */
    private final static Integer BYTE_DELIMITER = 10;

    public OssSinkFunction(String endpoint, String accessKeyId, String accessKeySecret, String bucketName, String objectName, SerializationSchema<IN> serializer) {
        this.endpoint = endpoint;
        this.accessKeyId = accessKeyId;
        this.accessKeySecret = accessKeySecret;
        this.bucketName = bucketName;
        this.objectName = objectName;
        this.serializer = serializer;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        serializer.open(RuntimeContextInitializationContextAdapters.serializationAdapter(
                getRuntimeContext()));
        currentOssClient = new OSSClientBuilder().build(endpoint, accessKeyId, accessKeySecret);
    }

    @Override
    public void invoke(IN record, SinkFunction.Context context) {
        try {
            if (isFirstRow) {
                // first append
                appendObjectRequest = new AppendObjectRequest(bucketName, objectName, new ByteArrayInputStream(ArrayUtils.addAll(serializer.serialize(record), (byte) (int) BYTE_DELIMITER)));
                appendObjectRequest.setPosition(0L);
                isFirstRow = false;
            } else {
                appendObjectRequest.setPosition(appendObjectResult.getNextPosition());
                appendObjectRequest.setInputStream(new ByteArrayInputStream(ArrayUtils.addAll(serializer.serialize(record), (byte) (int) BYTE_DELIMITER)));
            }
            appendObjectResult = currentOssClient.appendObject(appendObjectRequest);
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }

    @Override
    public void finish() {
        try {
            currentOssClient.shutdown();
        } catch (Throwable t) {
            // ignore
        }
    }

}
