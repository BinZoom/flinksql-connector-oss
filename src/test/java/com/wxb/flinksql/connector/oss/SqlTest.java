package com.wxb.flinksql.connector.oss;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * @author weixubin
 * @date 2022-06-08
 */
public class SqlTest {

    private static StreamExecutionEnvironment exeEnv;

    public static void main(String[] args) {
        exeEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        exeEnv.setParallelism(1);

        execute("sqls/ossAsSource.sql");
        execute("sqls/ossAsSink.sql");
    }

    private static void execute(String sqlPath) {
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(exeEnv, EnvironmentSettings.newInstance().inStreamingMode().build());
        List<String> sqls = splitSql(readFileContent(Thread.currentThread().getContextClassLoader().getResource(sqlPath).getPath()));
        for (String sql : sqls) {
            tableEnv.executeSql(sql);
        }
    }

    private static List<String> splitSql(List<String> sqlLines) {
        List<String> sqlList = new ArrayList<>();
        StringBuilder stmt = new StringBuilder();
        Pattern p = Pattern.compile("(?ms)('(?:''|[^'])*')|--.*?$|/\\*.*?\\*/|#.*?$|");
        for (String line : sqlLines) {
            stmt.append("\n").append(line.trim());
            if (line.trim().endsWith(";")) {
                sqlList.add(p.matcher(stmt.substring(0, stmt.length() - 1).trim()).replaceAll("$1"));
                //清空，复用
                stmt.setLength(0);
            }
        }
        return sqlList;
    }

    private static List<String> readFileContent(String filePath) {
        File file = new File(filePath);
        BufferedReader reader = null;
        List<String> sqlLines = new ArrayList<>();
        try {
            reader = new BufferedReader(new FileReader(file));
            String tempStr;
            while ((tempStr = reader.readLine()) != null) {
                sqlLines.add(tempStr);
            }
            reader.close();
            return sqlLines;
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            }
        }
        return sqlLines;
    }
}
