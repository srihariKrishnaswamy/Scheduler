package com.scheduler.worker;

import com.scheduler.proto.Job;
import com.scheduler.proto.JobSpec;
import com.scheduler.proto.JobStatus;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public class Tasklet {
    private final String taskletId;
    private Process currentProcess;
    private volatile Job currentJob;
    private volatile JobSpec currentSpec;
    private volatile JobStatus currentStatus;

    public Tasklet(String taskletId) {
        this.taskletId = taskletId;
        this.currentStatus = JobStatus.JOB_STATUS_UNSPECIFIED;
    }

    public CompletableFuture<Integer> runTask(Job job) throws IOException {
        if (currentStatus == JobStatus.RUNNING) {
            throw new IllegalStateException("Tasklet " + taskletId + " is already running a job");
        }
        System.out.println("Currently running job: " + job);

        this.currentJob = job;
        this.currentSpec = job.getSpec();
        this.currentStatus = JobStatus.RUNNING;

        ProcessBuilder processBuilder = new ProcessBuilder();
        processBuilder.command(currentSpec.getCommandList());
        processBuilder.redirectErrorStream(true);

        currentProcess = processBuilder.start();

        return CompletableFuture.supplyAsync(() -> {
            try {
                int exitCode = currentProcess.waitFor();
                currentStatus = (exitCode == 0) ? JobStatus.SUCCEEDED : JobStatus.JOB_STATUS_UNSPECIFIED;
                return exitCode;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                if (currentProcess != null) currentProcess.destroy();
                currentStatus = JobStatus.JOB_STATUS_UNSPECIFIED;
                return -1;
            } finally {
                currentJob = null;
                currentSpec = null;
            }
        });
    }

    public void killTask() {
        if (currentProcess != null && currentStatus == JobStatus.RUNNING) {
            currentProcess.destroy();
            currentStatus = JobStatus.JOB_STATUS_UNSPECIFIED;
            currentJob = null;
            currentSpec = null;
        }
    }

    public String getTaskletId() { return taskletId; }
    public boolean isRunning() { return currentStatus == JobStatus.RUNNING; }
    public JobStatus getCurrentStatus() { return currentStatus; }
    public Job getCurrentJob() { return currentJob; }
    public JobSpec getCurrentSpec() { return currentSpec; }
}
