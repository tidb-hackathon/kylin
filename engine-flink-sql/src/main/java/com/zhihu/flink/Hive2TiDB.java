package com.zhihu.flink;

import com.mysql.jdbc.Driver;
import com.zhihu.tibigdata.flink.tidb.TiDBCatalog;
import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

public class Hive2TiDB {

  static {
    try {
      Class.forName("com.mysql.jdbc.Driver");
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException(e);
    }
  }

  public static void main(String[] args) throws Exception {
    ParameterTool parameterTool = ParameterTool.fromArgs(args);
    String kylinConfDir = parameterTool.getRequired("kylin.conf.dir");
    String hiveConfDir = parameterTool.getRequired("hive.conf.dir");
    String cubeName = parameterTool.getRequired("cube.name");
    Properties properties = loadKylinProperties(kylinConfDir);
    Information information = new Information(properties, cubeName);

    // print sql
    String tiDBCreateTableSql = information.getTiDBCreateTableSql();
    System.out.println("create tidb table sql: " + tiDBCreateTableSql);
    String flinkInsertSql = information.getFlinkInsertSql();
    System.out.println("insert data to tidb sql: " + flinkInsertSql);

    EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
    TableEnvironment tEnv = TableEnvironment.create(settings);

    // create tidb catalog
    Map<String, String> map = new HashMap<>();
    map.put("tidb.database.url", information.getKylinStorageUrl());
    map.put("tidb.username", information.getKylinStorageUserName());
    map.put("tidb.password", information.getKylinStoragePassword());

    TiDBCatalog tiDBCatalog = new TiDBCatalog(map);
    HiveCatalog hiveCatalog = new HiveCatalog("hive", "default", hiveConfDir);
    tEnv.registerCatalog("tidb", tiDBCatalog);
    tEnv.registerCatalog("hive", hiveCatalog);

    // create tidb table
    tiDBCatalog.sqlUpdate(tiDBCreateTableSql);

    // insert
    tEnv.sqlUpdate(flinkInsertSql);
    tEnv.execute("Test");
  }

  public static Properties loadKylinProperties(String kylinConfDir) throws Exception {
    FileInputStream inputStream = new FileInputStream(
        String.format("%s/kylin.properties", kylinConfDir));
    Properties properties = new Properties();
    properties.load(inputStream);
    return properties;
  }

}
