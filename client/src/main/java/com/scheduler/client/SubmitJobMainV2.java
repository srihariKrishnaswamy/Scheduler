package com.scheduler.client;

import static com.scheduler.common.Constants.PORT;

import com.scheduler.proto.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Usage examples:
 *   mvn -pl client exec:java
 *   mvn -pl client exec:java -Dexec.args="--target=localhost:50054 --name=demo -- bash -lc 'echo hi && sleep 1'"
 *
 * Notes:
 * - Everything in "-- ... " is treated as the command list for JobSpec.command.
 * - If no command is provided, we submit a default job.
 */
public final class SubmitJobMainV2 {

  public static void main(String[] args) {
    Cli cli = Cli.parse(args);

    ManagedChannel channel = ManagedChannelBuilder.forTarget(cli.target)
        .usePlaintext()
        .build();

    try {
      SchedulerServiceGrpc.SchedulerServiceBlockingStub stub =
          SchedulerServiceGrpc.newBlockingStub(channel);

      JobSpec spec = JobSpec.newBuilder()
          .setName(cli.name)
          .addAllCommand(cli.command)
          .build();

      SubmitJobResponse resp = stub.submitJob(
          SubmitJobRequest.newBuilder().setSpec(spec).build()
      );

      Job job = resp.getJob();
      System.out.println("Submitted job: " + job.getJobId()
          + " status=" + job.getStatus()
          + (job.getAssignedWorkerId().isEmpty() ? "" : " assigned=" + job.getAssignedWorkerId()));
    } finally {
      channel.shutdownNow();
    }
  }

  // -----------------------------
  // Minimal CLI parsing
  // -----------------------------
  private static final class Cli {
    final String target;
    final String name;
    final List<String> command;

    private Cli(String target, String name, List<String> command) {
      this.target = target;
      this.name = name;
      this.command = command;
    }

    static Cli parse(String[] args) {
      String target = "localhost:" + PORT; // match your scheduler port
      String name = "demo";

      List<String> cmd = null;

      // Parse flags until "--"
      for (int i = 0; i < args.length; i++) {
        String a = args[i];

        if ("--".equals(a)) {
          // Everything after -- is the command list (verbatim tokens)
          if (i + 1 < args.length) {
            cmd = new ArrayList<>(Arrays.asList(Arrays.copyOfRange(args, i + 1, args.length)));
          }
          break;
        }

        if (a.startsWith("--target=")) {
          target = a.substring("--target=".length());
          continue;
        }
        if (a.equals("--target") && i + 1 < args.length) {
          target = args[++i];
          continue;
        }

        if (a.startsWith("--name=")) {
          name = a.substring("--name=".length());
          continue;
        }
        if (a.equals("--name") && i + 1 < args.length) {
          name = args[++i];
          continue;
        }
      }

      // Default command if none provided
      if (cmd == null || cmd.isEmpty()) {
        cmd = List.of("bash", "-lc", "echo hello from job && sleep 1");
      }

      return new Cli(target, name, cmd);
    }
  }
}
