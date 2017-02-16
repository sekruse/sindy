package de.hpi.isg.sindy.util;

import org.apache.flink.api.common.JobExecutionResult;

/**
 * Measures the execution time of a job. Also captures Flink's profiling results.
 *
 * @author Sebastian Kruse
 */
public class JobMeasurement {

    private final long startTime, endTime;

    private final String name;

    private final JobExecutionResult flinkResults;

    public JobMeasurement(String name, long startTime, long endTime, JobExecutionResult flinkResults) {
        super();
        this.name = name;
        this.startTime = startTime;
        this.endTime = endTime;
        this.flinkResults = flinkResults;
    }

    @Override
    public String toString() {
        return "JobMeasurement [name=" + name + ", " + getDuration() + " ms]";
    }

    /**
     * @return the flinkResults
     */
    public JobExecutionResult getFlinkResults() {
        return flinkResults;
    }

    public long getStartTime() {
        return startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public String getName() {
        return name;
    }

    public long getDuration() {
        return this.endTime - this.startTime;
    }

}
