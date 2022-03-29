package org.opencb.opencga.catalog.migration;

import org.opencb.commons.datastore.core.Event;

import java.util.EnumMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class MigrationSummary {

    private EnumMap<MigrationRun.MigrationStatus, Long> statusCount;
    private Map<String, Long> versionCount;
    private List<Event> events;
    private long migrationsToBeApplied;

    public MigrationSummary() {
    }

    public Map<MigrationRun.MigrationStatus, Long> getStatusCount() {
        return statusCount;
    }

    public MigrationSummary setStatusCount(EnumMap<MigrationRun.MigrationStatus, Long> statusCount) {
        this.statusCount = statusCount;
        return this;
    }

    public Map<String, Long> getVersionCount() {
        return versionCount;
    }

    public MigrationSummary setVersionCount(Map<String, Long> versionCount) {
        this.versionCount = versionCount;
        return this;
    }

    public List<Event> getEvents() {
        return events;
    }

    public MigrationSummary setEvents(List<Event> events) {
        this.events = events;
        return this;
    }

    public MigrationSummary addEvent(Event event) {
        if (events == null) {
            events = new LinkedList<>();
        }
        events.add(event);
        return this;
    }

    public void setMigrationsToBeApplied(long migrationsToBeApplied) {
        this.migrationsToBeApplied = migrationsToBeApplied;
    }

    public long getMigrationsToBeApplied() {
        return migrationsToBeApplied;
    }
}
