package com.zhihu.tidb.kylin;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.tuple.ITuple;

public class TiITuple implements ITuple {

  private final List<TblColRef> allColumns;

  private final List<String> allFields;

  private final Object[] allValues;

  public TiITuple(List<TblColRef> allColumns, Object[] allValues) {
    this.allColumns = allColumns;
    this.allFields = allColumns.stream().map(TblColRef::getName).collect(Collectors.toList());
    this.allValues = allValues;
  }

  @Override
  public List<String> getAllFields() {
    return allFields;
  }

  @Override
  public List<TblColRef> getAllColumns() {
    return allColumns;
  }

  @Override
  public Object[] getAllValues() {
    return allValues;
  }

  @Override
  public ITuple makeCopy() {
    return new TiITuple(allColumns, allValues);
  }

  @Override
  public Object getValue(TblColRef col) {
    return allValues[allColumns.indexOf(col)];
  }
}
