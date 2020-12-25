package com.zhihu.tidb.kylin;

import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.metadata.tuple.ITupleIterator;
import org.apache.kylin.metadata.tuple.TupleInfo;
import org.apache.kylin.storage.IStorageQuery;
import org.apache.kylin.storage.StorageContext;

public class TiDBIStorageQuery implements IStorageQuery {

  private final CubeInstance cubeInstance;

  public TiDBIStorageQuery(CubeInstance cubeInstance) {
    this.cubeInstance = cubeInstance;
  }

  @Override
  public ITupleIterator search(StorageContext context, SQLDigest sqlDigest,
      TupleInfo returnTupleInfo) {
    return new TiITupleIterator(cubeInstance, context, sqlDigest, returnTupleInfo);
  }
}
