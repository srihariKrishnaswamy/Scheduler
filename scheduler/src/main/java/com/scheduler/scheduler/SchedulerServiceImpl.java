package com.scheduler.scheduler;

import com.scheduler.proto.*;
import io.grpc.stub.StreamObserver;

import java.time.Instant;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public final class SchedulerServiceImpl extends SchedulerServiceGrpc.SchedulerServiceImplBase {

  private static final class JobRecord {
    final JobSpec spec;
    volatile JobStatus status;
    volatile String assignedWorkerId;

    JobRecord(JobSpec spec) {
      this.spec = spec;
      this.status = JobStatus.QUEUED;
    }
  }

  private final Map<String, JobRecord> jobs = new ConcurrentHashMap<>();
  private final Queue<String> pending = new ConcurrentLinkedQueue<>();
  private final Set<String> workers = ConcurrentHashMap.newKeySet();

  @Override
  public void registerWorker(RegisterWorkerRequest request, StreamObserver<RegisterWorkerResponse> responseObserver) {
    workers.add(request.getWorkerId());
    responseObserver.onNext(RegisterWorkerResponse.newBuilder().build());
    responseObserver.onCompleted();
  }

  @Override
  public void submitJob(SubmitJobRequest request, StreamObserver<SubmitJobResponse> responseObserver) {
    if (request.getSpec().getCommandCount() == 0) {
      responseObserver.onError(new IllegalArgumentException("spec.command must be non-empty"));
      return;
    }

    String jobId = UUID.randomUUID().toString();
    JobRecord rec = new JobRecord(request.getSpec());
    jobs.put(jobId, rec);
    pending.add(jobId);

    Job job = Job.newBuilder()
        .setJobId(jobId)
        .setSpec(rec.spec)
        .setStatus(rec.status)
        .build();

    responseObserver.onNext(SubmitJobResponse.newBuilder().setJob(job).build());
    responseObserver.onCompleted();
  }

  @Override
  public void pollWork(PollWorkRequest request, StreamObserver<PollWorkResponse> responseObserver) {
    String workerId = request.getWorkerId();
    if (!workers.contains(workerId)) {
      responseObserver.onError(new IllegalArgumentException("worker not registered: " + workerId));
      return;
    }

    String jobId = pending.poll();
    if (jobId == null) {
      responseObserver.onNext(PollWorkResponse.newBuilder().build());
      responseObserver.onCompleted();
      return;
    }

    JobRecord rec = jobs.get(jobId);
    if (rec == null) {
      responseObserver.onNext(PollWorkResponse.newBuilder().build());
      responseObserver.onCompleted();
      return;
    }

    rec.status = JobStatus.RUNNING;
    rec.assignedWorkerId = workerId;

    Job job = Job.newBuilder()
        .setJobId(jobId)
        .setSpec(rec.spec)
        .setStatus(rec.status)
        .setAssignedWorkerId(workerId)
        .build();

    responseObserver.onNext(PollWorkResponse.newBuilder().setJob(job).build());
    responseObserver.onCompleted();
  }

  @Override
  public void reportResult(ReportResultRequest request, StreamObserver<ReportResultResponse> responseObserver) {
    JobRecord rec = jobs.get(request.getJobId());
    if (rec == null) {
      responseObserver.onError(new IllegalArgumentException("unknown job: " + request.getJobId()));
      return;
    }

    // Phase 1: assume success; worker only calls this when command succeeded.
    rec.status = JobStatus.SUCCEEDED;

    responseObserver.onNext(ReportResultResponse.newBuilder().build());
    responseObserver.onCompleted();
  }
}
