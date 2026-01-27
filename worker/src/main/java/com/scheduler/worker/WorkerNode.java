package com.scheduler.worker;

import com.scheduler.proto.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.io.IOException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public final class WorkerNode {
    // ---- constant capacity model ----
    private final int totalCapacity;
    private final AtomicInteger remainingCapacity;

    private final String schedulerTarget;
    private final String workerId;

    private ManagedChannel channel;
    private SchedulerServiceGrpc.SchedulerServiceBlockingStub stub;

    private final AtomicBoolean running = new AtomicBoolean(false);

    // tasklet tracking
    private final ConcurrentHashMap<String, TaskletHandle> tasklets = new ConcurrentHashMap<>();

    // pool for completion callbacks
    private final ExecutorService callbackPool =
            Executors.newCachedThreadPool(r -> {
                Thread t = new Thread(r);
                t.setName("worker-tasklet-callback");
                t.setDaemon(true);
                return t;
            });

    public WorkerNode(String schedulerTarget, String workerId, int totalCapacity) {
        if (totalCapacity <= 0) throw new IllegalArgumentException("totalCapacity must be > 0");
        this.schedulerTarget = Objects.requireNonNull(schedulerTarget);
        this.workerId = Objects.requireNonNull(workerId);
        this.totalCapacity = totalCapacity;
        this.remainingCapacity = new AtomicInteger(totalCapacity);
    }

    public void start() {
        if (!running.compareAndSet(false, true)) return;

        channel = ManagedChannelBuilder.forTarget(schedulerTarget)
                .usePlaintext()
                .build();
        stub = SchedulerServiceGrpc.newBlockingStub(channel);

        stub.registerWorker(RegisterWorkerRequest.newBuilder()
                .setWorkerId(workerId)
                .build());

        log("Registered worker " + workerId + " totalCapacity=" + totalCapacity);
    }

    public void runLoop() {
        ensureStarted();
        while (running.get()) {
            boolean didWork = pollOnce();
            if (!didWork) sleepQuietly(200);
        }
        stop();
    }

    /**
     * Poll once and possibly launch a tasklet.
     * Returns true if a job was launched.
     */
    public boolean pollOnce() {
        ensureStarted();

        // Don’t poll if no capacity; avoids “accepting” work we can’t run (given current scheduler model).
        if (remainingCapacity.get() <= 0) return false;

        PollWorkResponse resp;
        try {
            resp = stub.pollWork(PollWorkRequest.newBuilder().setWorkerId(workerId).build());
        } catch (Exception e) {
            log("pollWork failed: " + e.getMessage());
            return false;
        }

        Job job = resp.getJob();
        if (job == null || job.getJobId().isEmpty()) {
            return false;
        }

        // Reserve 1 slot per job
        if (!tryReserveOneSlot()) {
            // Should be rare due to pre-check, but safe anyway
            log("No capacity for job " + job.getJobId() + " remaining=" + remainingCapacity.get());
            return false;
        }

        launchInNewTasklet(job);
        return true;
    }

    public void stop() {
        if (!running.compareAndSet(true, false)) return;

        // Ensure any reserved capacity is released exactly once, even if callbacks never run.
        for (TaskletHandle h : tasklets.values()) {
            try {
                h.tasklet.killTask();
            } catch (Exception ignored) {}

            h.status.set(JobStatus.JOB_STATUS_UNSPECIFIED);

            // Release reserved slot if it hasn't been released yet
            releaseCapacityOnce(h);

            // Best-effort: cancel the future so waiters unblock (does not guarantee process termination)
            CompletableFuture<Integer> fut = h.future.get();
            if (fut != null) {
                fut.cancel(true);
            }
        }

        callbackPool.shutdownNow();
        if (channel != null) channel.shutdownNow();

        log("Stopped. remaining=" + remainingCapacity.get() + "/" + totalCapacity);
    }

    // ---- Public introspection methods ----

    public int getTotalCapacity() { return totalCapacity; }
    public int getRemainingCapacity() { return remainingCapacity.get(); }

    /** Snapshot of taskletId -> status */
    public Map<String, JobStatus> getTaskletStatuses() {
        Map<String, JobStatus> out = new HashMap<>();
        for (var e : tasklets.entrySet()) out.put(e.getKey(), e.getValue().status.get());
        return out;
    }

    public Optional<JobStatus> getTaskletStatus(String taskletId) {
        TaskletHandle h = tasklets.get(taskletId);
        return h == null ? Optional.empty() : Optional.of(h.status.get());
    }

    // ---- internals ----

    private boolean tryReserveOneSlot() {
        while (true) {
            int cur = remainingCapacity.get();
            if (cur <= 0) return false;
            if (remainingCapacity.compareAndSet(cur, cur - 1)) return true;
        }
    }

    /**
     * Release one slot WITHOUT clobbering under concurrency.
     * (Replaces the old increment+clamp, which can lose updates.)
     */
    private void releaseOneSlot() {
        while (true) {
            int cur = remainingCapacity.get();
            if (cur >= totalCapacity) return; // already full; no-op
            if (remainingCapacity.compareAndSet(cur, cur + 1)) return;
        }
    }

    /** Ensure we release reserved capacity at most once for this tasklet. */
    private void releaseCapacityOnce(TaskletHandle handle) {
        if (handle.capacityReleased.compareAndSet(false, true)) {
            releaseOneSlot();
        }
    }

    private void launchInNewTasklet(Job job) {
        String taskletId = "tasklet-" + UUID.randomUUID();
        Tasklet tasklet = new Tasklet(taskletId);
        TaskletHandle handle = new TaskletHandle(taskletId, tasklet, job);
        tasklets.put(taskletId, handle);

        log("Launching " + taskletId + " for job=" + job.getJobId()
                + " remaining=" + remainingCapacity.get());

        CompletableFuture<Integer> fut;
        try {
            fut = tasklet.runTask(job);
            handle.future.set(fut);
        } catch (IOException e) {
            handle.status.set(JobStatus.JOB_STATUS_UNSPECIFIED);

            // We already reserved capacity; release it exactly once.
            releaseCapacityOnce(handle);

            log("Failed to start " + taskletId + ": " + e.getMessage()
                    + " remaining=" + remainingCapacity.get());
            return;
        }

        fut.whenCompleteAsync((exitCode, err) -> {
            if (err != null) {
                handle.status.set(JobStatus.JOB_STATUS_UNSPECIFIED);
                log("Tasklet " + taskletId + " errored: " + err.getMessage());
            } else {
                handle.status.set((exitCode != null && exitCode == 0)
                        ? JobStatus.SUCCEEDED
                        : JobStatus.JOB_STATUS_UNSPECIFIED);
            }

            // report success to scheduler (phase 1: only report on success if you want)
            if (handle.status.get() == JobStatus.SUCCEEDED) {
                try {
                    stub.reportResult(ReportResultRequest.newBuilder()
                            .setWorkerId(workerId)
                            .setJobId(job.getJobId())
                            .build());
                } catch (Exception ex) {
                    log("reportResult failed for job=" + job.getJobId() + ": " + ex.getMessage());
                }
            }

            // Release capacity exactly once (prevents double-release if stop() already released)
            releaseCapacityOnce(handle);

            log("Finished " + taskletId + " job=" + job.getJobId()
                    + " status=" + handle.status.get()
                    + " remaining=" + remainingCapacity.get());

            // Optional: cleanup completed tasklets to avoid growth:
            // tasklets.remove(taskletId);

        }, callbackPool);
    }

    private void ensureStarted() {
        if (!running.get() || stub == null) {
            throw new IllegalStateException("WorkerNode not started. Call start() first.");
        }
    }

    private static void sleepQuietly(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException ignored) { Thread.currentThread().interrupt(); }
    }

    private static void log(String s) {
        System.out.println("[" + Instant.now() + "] " + s);
    }

    private static final class TaskletHandle {
        final String taskletId;
        final Tasklet tasklet;
        final Job job;

        // status is read from other threads; keep it atomic for safe visibility
        final AtomicReference<JobStatus> status = new AtomicReference<>(JobStatus.RUNNING);

        // Ensures capacity is released exactly once per tasklet
        final AtomicBoolean capacityReleased = new AtomicBoolean(false);

        // Store the future so stop() can attempt cancellation / debugging
        final AtomicReference<CompletableFuture<Integer>> future = new AtomicReference<>(null);

        TaskletHandle(String taskletId, Tasklet tasklet, Job job) {
            this.taskletId = taskletId;
            this.tasklet = tasklet;
            this.job = job;
        }
    }
}
