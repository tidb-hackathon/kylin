package com.zhihu.flink;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class Information {

  private final ObjectMapper mapper = new ObjectMapper();

  private final TypeReference<Map<String, Object>> typeReference = new TypeReference<Map<String, Object>>() {
  };

  private final Properties properties;

  private final String cubeName;

  private String kylinMetaUrl;

  private String kylinMetaUserName;

  private String kylinMetaPassword;

  private String kylinMetaTableName;

  private String kylinStorageUrl;

  private String kylinStorageUserName;

  private String kylinStoragePassword;

  private String kylinStorageTableName;

  private String hiveDatabaseName;

  private String hiveTableName;

  private List<String> dimensionNames;

  private List<String> measureNames;

  private List<String> measureExpressions;

  private List<String> measureTypes;

  private List<Map<String, String>> measureParameters;


  @SuppressWarnings("unchecked")
  public Information(Properties properties, String cubeName) {
    this.properties = properties;
    this.cubeName = cubeName;
    //kylin.metadata.url=kylin_metadata@jdbc,url=jdbc:mysql://10.101.1.115:4000/kylin,username=makoto,password=123456
    for (String kv : properties.getProperty("kylin.metadata.url").split(",")) {
      if (kv.matches(".+@jdbc")) {
        kylinMetaTableName = kv.split("@")[0];
      }
      if (kv.startsWith("url=")) {
        kylinMetaUrl = kv.split("=")[1];
      }
      if (kv.startsWith("username=")) {
        kylinMetaUserName = kv.split("=")[1];
      }
      if (kv.startsWith("password=")) {
        kylinMetaPassword = kv.split("=")[1];
      }
    }
    kylinStorageUrl = properties.getProperty("tidb.database.url");
    kylinStorageUserName = properties.getProperty("tidb.username");
    kylinStoragePassword = properties.getProperty("tidb.password");
    Map<String, Object> cubeMap = getCubeJsonObject();
    kylinStorageTableName = ((Map) ((List) cubeMap.get("segments")).get(0))
        .get("storage_location_identifier").toString();
    String[] split = ((Map) ((Map) ((List) cubeMap.get("segments")).get(0))
        .get("dictionaries")).values().stream().findFirst().get().toString().split("/")[2]
        .split("\\.");
    hiveDatabaseName = split[0].toLowerCase();
    hiveTableName = split[1].toLowerCase();
    Map<String, Object> cubeDescMap = getCubeDescJsonObject();
    dimensionNames = ((List<Object>) cubeDescMap.get("dimensions")).stream()
        .map(object -> ((Map) object).get("name").toString()).collect(Collectors.toList());
    List<Object> measures = (List) cubeDescMap.get("measures");
    measureNames = measures.stream().map(object -> ((Map) object).get("name").toString())
        .collect(Collectors.toList());
    measureTypes = measures.stream()
        .map(object -> ((Map) ((Map) object).get("function")).get("returntype").toString()
            .toUpperCase()).collect(Collectors.toList());
    measureExpressions = measures.stream()
        .map(object -> ((Map) ((Map) object).get("function")).get("expression").toString())
        .collect(Collectors.toList());
    measureParameters = measures.stream()
        .map(
            object -> (Map<String, String>) ((Map) ((Map) object).get("function")).get("parameter"))
        .collect(Collectors.toList());
    System.out.println(getFlinkInsertSql());
  }

  private Connection getMetaConnection() throws SQLException {
    return DriverManager.getConnection(kylinMetaUrl, kylinMetaUserName, kylinMetaPassword);
  }

  private Map<String, Object> getCubeJsonObject() {
    try (Connection connection = getMetaConnection();
        Statement statement = connection.createStatement()) {
      String sql = String.format(
          "SELECT `META_TABLE_CONTENT` FROM %s WHERE `META_TABLE_KEY` = '/cube/%s.json'",
          kylinMetaTableName, cubeName);
      ResultSet resultSet = statement.executeQuery(sql);
      if (!resultSet.next()) {
        throw new IllegalStateException("cube does not exist");
      }
      String content = resultSet.getString("META_TABLE_CONTENT");
      return mapper.readValue(content, typeReference);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private Map<String, Object> getCubeDescJsonObject() {
    try (Connection connection = getMetaConnection();
        Statement statement = connection.createStatement()) {
      String sql = String.format(
          "SELECT `META_TABLE_CONTENT` FROM %s WHERE `META_TABLE_KEY` = '/cube_desc/%s.json'",
          kylinMetaTableName, cubeName);
      ResultSet resultSet = statement.executeQuery(sql);
      if (!resultSet.next()) {
        throw new IllegalStateException("cube does not exist");
      }
      String content = resultSet.getString("META_TABLE_CONTENT");
      return mapper.readValue(content, typeReference);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  public String getTiDBCreateTableSql() {
    List<String> nameAndType = new ArrayList<>();
    dimensionNames.forEach(name -> nameAndType.add(String.format("`%s` VARCHAR(16)", name)));
    for (int i = 0; i < measureNames.size(); i++) {
      String name = measureNames.get(i);
      String expr = measureExpressions.get(i);
      String type = measureTypes.get(i);
      Map<String, String> parameters = measureParameters.get(i);
      if (name.equals("_COUNT_")) {
        nameAndType.add("`_KY_COUNT__` BIGINT");
        continue;
      }
      String column = parameters.get("value").split("\\.")[1];
      nameAndType.add(
          String.format("`_KY_%s_%s_%s_` %s", expr, hiveTableName.toUpperCase(), column, type));
    }
    nameAndType.add(String.format("UNIQUE KEY(%s)",
        dimensionNames.stream().map(name -> "`" + name + "`").collect(Collectors.joining(","))));
    return String
        .format("CREATE TABLE `%s`(\n%s\n)", kylinStorageTableName,
            String.join(",\n", nameAndType));
  }

  public String getFlinkInsertSql() {
    String dimString = dimensionNames.stream().map(name -> "`" + name.toLowerCase() + "`")
        .collect(Collectors.joining(","));
    List<String> measureAgg = new ArrayList<>();
    for (int i = 0; i < measureNames.size(); i++) {
      String name = measureNames.get(i);
      String expr = measureExpressions.get(i);
      Map<String, String> parameters = measureParameters.get(i);
      if (name.equals("_COUNT_")) {
        measureAgg.add("COUNT(*) AS _KY_COUNT__");
        continue;
      }
      String column = parameters.get("value").split("\\.")[1];
      measureAgg.add(String
          .format("%s(%s) AS _KY_%s_%s_%s_", expr, column.toLowerCase(), expr,
              hiveTableName.toUpperCase(), column));
    }
    String selectFields = String.format("%s,%s", dimString, String.join(",", measureAgg));
    return String.format("INSERT INTO `tidb`.`%s`.`%s` \n"
            + "SELECT %s FROM `hive`.`%s`.`%s` \n"
            + "GROUP BY %s",
        kylinStorageUrl.substring(kylinMetaUrl.lastIndexOf("/") + 1),
        kylinStorageTableName,
        selectFields,
        hiveDatabaseName,
        hiveTableName,
        dimString
    );
  }

  public String getKylinMetaUrl() {
    return kylinMetaUrl;
  }

  public String getKylinMetaUserName() {
    return kylinMetaUserName;
  }

  public String getKylinMetaPassword() {
    return kylinMetaPassword;
  }

  public String getKylinMetaTableName() {
    return kylinMetaTableName;
  }

  public String getKylinStorageUrl() {
    return kylinStorageUrl;
  }

  public String getKylinStorageUserName() {
    return kylinStorageUserName;
  }

  public String getKylinStoragePassword() {
    return kylinStoragePassword;
  }

  public String getKylinStorageTableName() {
    return kylinStorageTableName;
  }

  public String getHiveDatabaseName() {
    return hiveDatabaseName;
  }

  public String getHiveTableName() {
    return hiveTableName;
  }
}
