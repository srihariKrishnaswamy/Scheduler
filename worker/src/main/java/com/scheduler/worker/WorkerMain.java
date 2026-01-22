package com.scheduler.worker;

import com.scheduler.proto.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;
import java.util.UUID;

public final class WorkerMain {
  public static void main(String[] args) throws Exception {
    String target = "localhost:50053";
    String workerId = "worker-" + UUID.randomUUID();

    ManagedChannel channel = ManagedChannelBuilder.forTarget(target)
        .usePlaintext()
        .build();
    SchedulerServiceGrpc.SchedulerServiceBlockingStub stub = SchedulerServiceGrpc.newBlockingStub(channel);

    stub.registerWorker(RegisterWorkerRequest.newBuilder().setWorkerId(workerId).build());
    System.out.println("Registered worker: " + workerId);

    while (true) {
      PollWorkResponse resp = stub.pollWork(PollWorkRequest.newBuilder().setWorkerId(workerId).build());
      Job job = resp.hasJob() ? resp.getJob() : Job.newBuilder().build();

      if (job.getJobId() == null || job.getJobId().isEmpty()) {
        Thread.sleep(250);
        continue;
      }

      System.out.println("Executing job " + job.getJobId() + " : " + job.getSpec().getCommandList());
      runCommand(job.getSpec().getCommandList());

      stub.reportResult(ReportResultRequest.newBuilder()
          .setWorkerId(workerId)
          .setJobId(job.getJobId())
          .build());

      System.out.println("Completed job " + job.getJobId());
    }
  }

  private static void runCommand(List<String> command) throws Exception {
    ProcessBuilder pb = new ProcessBuilder(command);
    pb.redirectErrorStream(true);
    Process p = pb.start();

    try (BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
      String line;
      while ((line = br.readLine()) != null) {
        System.out.println("[job] " + line);
      }
    }

    int code = p.waitFor();
    if (code != 0) {
      // Phase 1 assumption: jobs don't fail.
      throw new RuntimeException("Job failed with exit code " + code);
    }
  }
}
