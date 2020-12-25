package com.zhihu.tidb.kylin;

import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.storage.IStorage;
import org.apache.kylin.storage.IStorageQuery;

public class TiDBStorage implements IStorage {

  @Override
  public IStorageQuery createQuery(IRealization realization) {
    return new TiDBIStorageQuery((CubeInstance) realization);
  }

  @Override
  public <I> I adaptToBuildEngine(Class<I> engineInterface) {
    throw new UnsupportedOperationException();
  }
}
