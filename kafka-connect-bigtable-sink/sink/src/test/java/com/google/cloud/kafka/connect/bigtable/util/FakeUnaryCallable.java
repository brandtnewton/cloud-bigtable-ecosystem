package com.google.cloud.kafka.connect.bigtable.util;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.gax.rpc.ApiCallContext;
import com.google.api.gax.rpc.UnaryCallable;

public class FakeUnaryCallable<TRequest, TResponse> extends UnaryCallable<TRequest, TResponse> {

  private final TResponse response;

  public FakeUnaryCallable(TResponse response) {
    this.response = response;
  }

  @Override
  public ApiFuture<TResponse> futureCall(TRequest request, ApiCallContext context) {
    return ApiFutures.immediateFuture(response);
  }
}
