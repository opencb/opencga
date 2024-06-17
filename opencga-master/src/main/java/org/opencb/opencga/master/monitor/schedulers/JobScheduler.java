package org.opencb.opencga.master.monitor.schedulers;

import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.job.Job;

import java.util.*;

public class JobScheduler {

    private TreeMap<Float, Job> jobTreeMap;

    private static final float PRIORITY_WEIGHT = 0.6F;
    private static final float IDLE_TIME_WEIGHT = 0.4F;
//    private static final float OPERATION_WEIGHT = 0.2F;
//    private static final float USER_PRIORITY_WEIGHT = 0.2F;

    public JobScheduler() {
        this.jobTreeMap = new TreeMap<>();
    }

    public void addJobs(List<Job> jobs) {
        for (Job job : jobs) {
            float priority = extractPriority(job);
            jobTreeMap.put(priority, job);
        }
    }

    public Job getNext() {
        return jobTreeMap.pollLastEntry().getValue();
    }

    public void emtpy() {
        jobTreeMap.clear();
    }

    private float extractPriority(Job job) {
        float value = 0;
        // Priority
        switch (job.getPriority()) {
            case URGENT:
                value += PRIORITY_WEIGHT;
                break;
            case HIGH:
                value += PRIORITY_WEIGHT * 4 / 5;
                break;
            case MEDIUM:
                value += PRIORITY_WEIGHT * 3 / 5;
                break;
            case LOW:
                value += PRIORITY_WEIGHT * 2 / 5;
                break;
            case UNKNOWN:
            default:
                value += PRIORITY_WEIGHT * 1 / 5;
                break;
        }

        // Idle time
        Date currentDate = TimeUtils.getDate();
        Calendar currentCal = new GregorianCalendar();
        currentCal.setTime(currentDate);

        Date jobDate = TimeUtils.toDate(job.getCreationDate());
        Calendar cal = new GregorianCalendar();
        cal.setTime(jobDate);

        cal.add(Calendar.HOUR, 24);
        if (cal.before(currentCal)) { // Job was created more than 24 hours ago
            value += IDLE_TIME_WEIGHT;
        } else {
            cal.add(Calendar.HOUR, -6); // 18 hours
            if (cal.before(currentCal)) {
                value += IDLE_TIME_WEIGHT * 6 / 7;
            } else {
                cal.add(Calendar.HOUR, -6); // 12 hours
                if (cal.before(currentCal)) {
                    value += IDLE_TIME_WEIGHT * 5 / 7;
                } else {
                    cal.add(Calendar.HOUR, -4); // 8 hours
                    if (cal.before(currentCal)) {
                        value += IDLE_TIME_WEIGHT * 4 / 7;
                    } else {
                        cal.add(Calendar.HOUR, -4); // 4 hours
                        if (cal.before(currentCal)) {
                            value += IDLE_TIME_WEIGHT * 3 / 7;
                        } else {
                            cal.add(Calendar.HOUR, -2); // 2 hours
                            if (cal.before(currentCal)) {
                                value += IDLE_TIME_WEIGHT * 2 / 7;
                            } else {
                                cal.add(Calendar.HOUR, -1); // 1 hour
                                if (cal.before(currentCal)) {
                                    value += IDLE_TIME_WEIGHT * 1 / 7;
                                }
                            }
                        }

                    }

                }
            }
        }

        // TODO: Check operation and user priority
        return value;
    }

    public Iterator<Job> iterator() {
        return jobTreeMap.values().iterator();
    }

}
