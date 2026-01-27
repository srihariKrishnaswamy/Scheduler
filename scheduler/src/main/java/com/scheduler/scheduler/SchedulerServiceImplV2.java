package com.scheduler.scheduler;

import com.scheduler.proto.*;
import io.grpc.stub.StreamObserver;

import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public final class SchedulerServiceImplV2 extends SchedulerServiceGrpc.SchedulerServiceImplBase {

  // ---- lease settings ----
  // If a job is RUNNING longer than this without reportResult, we requeue it.
  private static final long LEASE_MS = 30_000; // 30s is a good dev default

  private static final class JobRecord {
    final JobSpec spec;

    // guarded by synchronized(this)
    JobStatus status;
    String assignedWorkerId;
    long runningSinceMs;

    JobRecord(JobSpec spec) {
      this.spec = spec;
      this.status = JobStatus.QUEUED;
      this.assignedWorkerId = "";
      this.runningSinceMs = 0L;
    }
  }

  private final Map<String, JobRecord> jobs = new ConcurrentHashMap<>();
  private final Queue<String> pending = new ConcurrentLinkedQueue<>();
  private final Set<String> workers = ConcurrentHashMap.newKeySet();

  @Override
  public void registerWorker(RegisterWorkerRequest request,
                             StreamObserver<RegisterWorkerResponse> responseObserver) {
    String workerId = request.getWorkerId();
    if (workerId == null || workerId.isBlank()) {
      responseObserver.onError(new IllegalArgumentException("worker_id must be non-empty"));
      return;
    }

    workers.add(workerId);

    responseObserver.onNext(RegisterWorkerResponse.newBuilder().build());
    responseObserver.onCompleted();
  }

  @Override
  public void submitJob(SubmitJobRequest request,
                        StreamObserver<SubmitJobResponse> responseObserver) {
    JobSpec spec = request.getSpec();
    if (spec == null || spec.getCommandCount() == 0) {
      responseObserver.onError(new IllegalArgumentException("spec.command must be non-empty"));
      return;
    }

    String jobId = UUID.randomUUID().toString();
    JobRecord rec = new JobRecord(spec);

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
  public void pollWork(PollWorkRequest request,
                       StreamObserver<PollWorkResponse> responseObserver) {
    String workerId = request.getWorkerId();
    if (workerId == null || workerId.isBlank()) {
      responseObserver.onError(new IllegalArgumentException("worker_id must be non-empty"));
      return;
    }

    if (!workers.contains(workerId)) {
      responseObserver.onError(new IllegalArgumentException("worker not registered: " + workerId));
      return;
    }

    // Opportunistic cleanup: before assigning new work, requeue expired RUNNING jobs.
    // (Cheap + works without adding a background thread.)
    reapExpiredLeases();

    // Pop from pending until we find a valid QUEUED job to assign.
    for (int attempts = 0; attempts < 64; attempts++) {
      String jobId = pending.poll();
      if (jobId == null) {
        responseObserver.onNext(PollWorkResponse.newBuilder().build());
        responseObserver.onCompleted();
        return;
      }

      JobRecord rec = jobs.get(jobId);
      if (rec == null) {
        // stale entry
        continue;
      }

      Job jobToRun = null;

      synchronized (rec) {
        // Only assign if it's still QUEUED.
        if (rec.status != JobStatus.QUEUED) {
          continue;
        }

        rec.status = JobStatus.RUNNING;
        rec.assignedWorkerId = workerId;
        rec.runningSinceMs = System.currentTimeMillis();

        jobToRun = Job.newBuilder()
            .setJobId(jobId)
            .setSpec(rec.spec)
            .setStatus(rec.status)
            .setAssignedWorkerId(workerId)
            .build();
      }

      responseObserver.onNext(PollWorkResponse.newBuilder().setJob(jobToRun).build());
      responseObserver.onCompleted();
      return;
    }

    // Could not find a valid job quickly
    responseObserver.onNext(PollWorkResponse.newBuilder().build());
    responseObserver.onCompleted();
  }

  @Override
  public void reportResult(ReportResultRequest request,
                           StreamObserver<ReportResultResponse> responseObserver) {
    String jobId = request.getJobId();
    String workerId = request.getWorkerId();

    if (jobId == null || jobId.isBlank()) {
      responseObserver.onError(new IllegalArgumentException("job_id must be non-empty"));
      return;
    }
    if (workerId == null || workerId.isBlank()) {
      responseObserver.onError(new IllegalArgumentException("worker_id must be non-empty"));
      return;
    }

    JobRecord rec = jobs.get(jobId);
    if (rec == null) {
      responseObserver.onError(new IllegalArgumentException("unknown job: " + jobId));
      return;
    }

    synchronized (rec) {
      // If the lease expired and job was requeued/reassigned, ignore stale results.
      // (This avoids marking SUCCEEDED for a job that is now running elsewhere.)
      if (!workerId.equals(rec.assignedWorkerId)) {
        responseObserver.onError(new IllegalArgumentException(
            "stale result: job " + jobId + " currently assigned to '" + rec.assignedWorkerId
                + "', but result reported by '" + workerId + "'"));
        return;
      }

      // Idempotent success
      if (rec.status == JobStatus.SUCCEEDED) {
        responseObserver.onNext(ReportResultResponse.newBuilder().build());
        responseObserver.onCompleted();
        return;
      }

      if (rec.status != JobStatus.RUNNING) {
        responseObserver.onError(new IllegalStateException(
            "job " + jobId + " is not RUNNING (status=" + rec.status + ")"));
        return;
      }

      // Phase 2: assume worker only reports success.
      rec.status = JobStatus.SUCCEEDED;
      rec.runningSinceMs = 0L;
      // keep assignedWorkerId if you like for auditing; optional
    }

    responseObserver.onNext(ReportResultResponse.newBuilder().build());
    responseObserver.onCompleted();
  }

  /**
   * Requeue RUNNING jobs whose lease has expired.
   * This is intentionally "best effort": no background thread, just opportunistic.
   */
  private void reapExpiredLeases() {
    long now = System.currentTimeMillis();

    for (Map.Entry<String, JobRecord> e : jobs.entrySet()) {
      String jobId = e.getKey();
      JobRecord rec = e.getValue();

      boolean shouldRequeue = false;

      synchronized (rec) {
        if (rec.status == JobStatus.RUNNING) {
          long age = now - rec.runningSinceMs;
          if (rec.runningSinceMs > 0L && age > LEASE_MS) {
            // expire lease
            rec.status = JobStatus.QUEUED;
            rec.assignedWorkerId = "";
            rec.runningSinceMs = 0L;
            shouldRequeue = true;
          }
        }
      }

      if (shouldRequeue) {
        pending.add(jobId);
      }
    }
  }
}
