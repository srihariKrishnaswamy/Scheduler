package com.scheduler.scheduler;

import static com.scheduler.common.Constants.PORT;

import io.grpc.Server;
import io.grpc.ServerBuilder;

public final class SchedulerServer {
  public static void main(String[] args) throws Exception {
    Server server = ServerBuilder.forPort(PORT)
        .addService(new SchedulerServiceImplV2())
        .build()
        .start();

    System.out.println("Scheduler listening on " + PORT);
    server.awaitTermination();
  }
}
