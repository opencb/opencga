package org.opencb.opencga.catalog.monitor.executors;

/**
 * Created by pfurio on 22/08/16.
 */
public class ExecutorConfig {

    private String stdout;
    private String stderr;
    private String outdir;
    private int timeout;   // seconds
    private int numThreads;
    private int maxMem;    // MB

    public ExecutorConfig() {
        this("/tmp/stdout.txt", "/tmp/stderr.txt", "/tmp/", 3600, 1, 1024);
    }

    public ExecutorConfig(String stdout, String stderr, String outdir, int timeout, int numThreads, int maxMem) {
        this.stdout = stdout;
        this.stderr = stderr;
        this.outdir = outdir;
        this.timeout = timeout;
        this.numThreads = numThreads;
        this.maxMem = maxMem;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ExecutorConfig{");
        sb.append("stdout='").append(stdout).append('\'');
        sb.append(", stderr='").append(stderr).append('\'');
        sb.append(", outdir='").append(outdir).append('\'');
        sb.append(", timeout=").append(timeout);
        sb.append(", numThreads=").append(numThreads);
        sb.append(", maxMem=").append(maxMem);
        sb.append('}');
        return sb.toString();
    }

    public String getStdout() {
        return stdout;
    }

    public ExecutorConfig setStdout(String stdout) {
        this.stdout = stdout;
        return this;
    }

    public String getStderr() {
        return stderr;
    }

    public ExecutorConfig setStderr(String stderr) {
        this.stderr = stderr;
        return this;
    }

    public int getTimeout() {
        return timeout;
    }

    public ExecutorConfig setTimeout(int timeout) {
        this.timeout = timeout;
        return this;
    }

    public int getNumThreads() {
        return numThreads;
    }

    public ExecutorConfig setNumThreads(int numThreads) {
        this.numThreads = numThreads;
        return this;
    }

    public int getMaxMem() {
        return maxMem;
    }

    public ExecutorConfig setMaxMem(int maxMem) {
        this.maxMem = maxMem;
        return this;
    }

    public String getOutdir() {
        return outdir;
    }

    public ExecutorConfig setOutdir(String outdir) {
        this.outdir = outdir;
        return this;
    }
}
