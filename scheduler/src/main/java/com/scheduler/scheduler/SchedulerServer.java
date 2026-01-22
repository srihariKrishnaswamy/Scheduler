package com.scheduler.scheduler;

import io.grpc.Server;
import io.grpc.ServerBuilder;

public final class SchedulerServer {
  public static void main(String[] args) throws Exception {
    int port = 50051;
    Server server = ServerBuilder.forPort(port)
        .addService(new SchedulerServiceImpl())
        .build()
        .start();

    System.out.println("Scheduler listening on " + port);
    server.awaitTermination();
  }
}
