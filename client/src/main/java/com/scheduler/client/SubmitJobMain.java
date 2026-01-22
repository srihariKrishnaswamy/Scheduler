package com.scheduler.client;

import com.scheduler.proto.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.List;

public final class SubmitJobMain {
  public static void main(String[] args) {
    ManagedChannel channel = ManagedChannelBuilder.forTarget("localhost:50051")
        .usePlaintext()
        .build();
    SchedulerServiceGrpc.SchedulerServiceBlockingStub stub = SchedulerServiceGrpc.newBlockingStub(channel);

    // Default job if none provided
    List<String> cmd = List.of("bash", "-lc", "echo hello from job && sleep 1");

    JobSpec spec = JobSpec.newBuilder()
        .setName("demo")
        .addAllCommand(cmd)
        .build();

    SubmitJobResponse resp = stub.submitJob(SubmitJobRequest.newBuilder().setSpec(spec).build());
    System.out.println("Submitted job: " + resp.getJob().getJobId());

    channel.shutdownNow();
  }
}
