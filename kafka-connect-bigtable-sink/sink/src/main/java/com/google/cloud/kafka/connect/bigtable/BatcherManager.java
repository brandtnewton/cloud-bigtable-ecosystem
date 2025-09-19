package com.google.cloud.kafka.connect.bigtable;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.gax.batching.Batcher;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import com.google.cloud.kafka.connect.bigtable.mapping.MutationData;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class BatcherManager {

  private final BigtableDataClient bigtableData;
  private final Map<String, Batcher<RowMutationEntry, Void>> batchers;

  public BatcherManager(BigtableDataClient bigtableData) {
    this.bigtableData = bigtableData;
    this.batchers = new HashMap<>();
  }

  public void SendOutstanding(){
    for (Batcher<RowMutationEntry, Void> batcher : batchers.values()) {
      // We must flush the batchers to respect CONFIG_MAX_BATCH_SIZE.
      // We flush asynchronously and await the results instead.
      batcher.sendOutstanding();
    }
  }

  public ApiFuture<Void> Put(MutationData mutationData){
    String recordTableName = mutationData.getTargetTable();
    Batcher<RowMutationEntry, Void> batcher = batchers.computeIfAbsent(recordTableName, bigtableData::newBulkMutationBatcher);
    return batcher.add(mutationData.getUpsertMutation());
  }

  public void FlushAll() throws ExecutionException, InterruptedException {
    try {
      Iterable<ApiFuture<Void>> closures = batchers.values().stream().map(Batcher::closeAsync)
          .collect(Collectors.toList());
      ApiFutures.allAsList(closures).get();
    }finally {
      batchers.clear();
    }
  }
}
