package com.google.cloud.kafka.connect.bigtable;

import com.google.bigtable.v2.MutateRowsRequest;
import com.google.common.base.Stopwatch;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import java.time.Duration;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * A gRPC interceptor that logs details about every outgoing Bigtable RPC.
 */
public class BigtableInterceptor implements ClientInterceptor {
  private final int failureRate;
  private final Duration maxRandomLag;
  private final String methodName;
  private final AtomicInteger count;

  public BigtableInterceptor(int failureRate, Duration maxRandomLag, String methodName) {
    this.failureRate = failureRate;
    this.maxRandomLag = maxRandomLag;
    this.methodName = methodName;
    count = new AtomicInteger();
  }

  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
      MethodDescriptor<ReqT, RespT> method,
      CallOptions callOptions,
      Channel next) {

    System.out.printf("CBTInterceptor method: %s\n%n", method.getFullMethodName());
    if (method.getFullMethodName().equals(methodName)) {
      count.incrementAndGet();
      if (count.get() % this.failureRate == 0) {
        return new FailureCall<>(method.getFullMethodName(), Status.UNKNOWN, maxRandomLag);
      }
    }
    return next.newCall(method, callOptions);
  }

  public static class FailureCall<ReqT, RespT> extends ClientCall<ReqT, RespT> {

    private final String method;
    private final Status statusToReturn;
    private final Duration delay;

    public FailureCall(String method, Status statusToReturn, Duration delay) {
      this.method = method;
      this.statusToReturn = statusToReturn;
      this.delay = delay;
    }

    @Override
    public void start(Listener<RespT> responseListener, Metadata metadata) {
      System.out.printf("CBTInterceptor Failing call to %s with status %s in %s...",
          method,
          statusToReturn,
          delay);

      try {
        // This blocks the client's thread, simulating a slow network call
        Thread.sleep(delay.toMillis());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        // If the client thread is interrupted (e.g., timeout), fail fast
        responseListener.onClose(
            Status.CANCELLED.withDescription("Client interrupted during simulated delay"),
            new Metadata());
        return;
      }

      // After the delay, return the programmed failure
      responseListener.onClose(statusToReturn, new Metadata());

    }

    // All other methods are empty no-ops
    @Override
    public void request(int numMessages) {
    }

    @Override
    public void cancel(String message, Throwable cause) {
    }

    @Override
    public void halfClose() {
    }

    @Override
    public void sendMessage(ReqT message) {
    }

  }
}