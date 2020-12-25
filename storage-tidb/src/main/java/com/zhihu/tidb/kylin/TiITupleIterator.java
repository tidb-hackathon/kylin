package com.zhihu.tidb.kylin;

import static java.util.Objects.requireNonNull;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kylin.common.QueryContextFacade;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.metadata.tuple.ITuple;
import org.apache.kylin.metadata.tuple.ITupleIterator;
import org.apache.kylin.metadata.tuple.TupleInfo;
import org.apache.kylin.storage.StorageContext;

public class TiITupleIterator implements ITupleIterator {

  public static final String DATABASE_URL = "tidb.database.url";

  public static final String USERNAME = "tidb.username";

  public static final String PASSWORD = "tidb.password";


  private final CubeInstance cubeInstance;

  private final StorageContext context;

  private final SQLDigest sqlDigest;

  private final TupleInfo returnTupleInfo;

  private final Connection connection;

  private final Statement statement;

  private final ResultSet resultSet;

  private final List<TblColRef> allColumns;

  private final List<String> allFields;

  private final Set<String> allMeasures;

  private final String sql;


  public TiITupleIterator(CubeInstance cubeInstance, StorageContext context, SQLDigest sqlDigest,
      TupleInfo returnTupleInfo) {
    this.cubeInstance = cubeInstance;
    this.context = context;
    this.sqlDigest = sqlDigest;
    this.returnTupleInfo = returnTupleInfo;
    this.allColumns = returnTupleInfo.getAllColumns();
    this.allFields = allColumns.stream().map(TblColRef::getName).collect(Collectors.toList());
    Set<String> allDimensions = cubeInstance.getAllDimensions().stream().map(TblColRef::getName)
        .collect(Collectors.toSet());
    this.allMeasures = cubeInstance.getAllColumns().stream().map(TblColRef::getName)
        .filter(name -> !allDimensions.contains(name)).collect(Collectors.toSet());
    this.sql = getSql();
    this.connection = getConnection();
    this.statement = getStatement();
    this.resultSet = getResultSet();
  }

  @Override
  public boolean hasNext() {
    try {
      return resultSet.next();
    } catch (SQLException e) {
      throw new IllegalStateException(e);
    }

  }

  @Override
  public ITuple next() {
    Object[] values = new Object[allColumns.size()];
    try {
      for (int i = 0; i < allColumns.size(); i++) {
        values[i] = resultSet.getObject(allFields.get(i));
      }
      return new TiITuple(allColumns, values);
    } catch (SQLException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public void close() {
    closeWithSuppression(resultSet);
    closeWithSuppression(statement);
    closeWithSuppression(connection);
  }

  private void closeWithSuppression(AutoCloseable closeable) {
    try {
      closeable.close();
    } catch (Exception e) {
      // do nothing here
    }
  }

  private Connection getConnection() {
    Properties properties = cubeInstance.getConfig().exportToProperties();
    try {
      return DriverManager.getConnection(
          requireNonNull(properties.getProperty(DATABASE_URL)),
          requireNonNull(properties.getProperty(USERNAME)),
          requireNonNull(properties.getProperty(PASSWORD))
      );
    } catch (SQLException e) {
      throw new IllegalStateException(e);
    }
  }

  private Statement getStatement() {
    try {
      return connection.createStatement();
    } catch (SQLException e) {
      throw new IllegalStateException(e);
    }
  }

  private String getSqlFromEnv() {
    return requireNonNull(System.clearProperty("sql-" + QueryContextFacade.current().getQueryId()));
  }

  // TODO: use sqlDigest
  private String getSql() {
    String selectFields = allFields.stream()
        .map(field -> allMeasures.contains(field) ? "NULL AS " + field : "`" + field + "`")
        .collect(Collectors.joining(","));
    String tableName = cubeInstance.getFirstSegment().getStorageLocationIdentifier();
    return getSqlFromEnv()
        .replace("\t", " ")
        .replace("\n", " ")
        .replaceAll(" +", " ")
        .replace("\"", "`")
        .replaceAll(" *, *", ",")
        .replaceAll("(?i)(SELECT.*FROM +[^ ]+)",
            String.format("SELECT %s FROM %s", selectFields, tableName))
        .replaceAll("(?i)(GROUP +BY +[^ ]+)", "");
  }

  private ResultSet getResultSet() {
    try {
      return statement.executeQuery(sql);
    } catch (SQLException e) {
      throw new IllegalStateException(e);
    }
  }

}
