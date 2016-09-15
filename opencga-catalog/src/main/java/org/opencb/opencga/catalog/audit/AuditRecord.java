package org.opencb.opencga.catalog.audit;


import org.opencb.commons.datastore.core.ObjectMap;

/**
 * Created on 18/08/15.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class AuditRecord {

    public enum Resource {user, project, study, file, sample, job, individual, cohort, dataset, panel, tool, variableSet}
    public enum Action {create, update, view, delete, restore, index, login, logout, share}
    public enum Magnitude {low, medium, high}

    private Object id;
    private Resource resource;
    private Action action;
    private Magnitude importance;
    private ObjectMap before;
    private ObjectMap after;
    /*
     * Time in milliseconds
     */
    private long timeStamp;
    private String userId;
    private String description;
    private ObjectMap attributes;

    public AuditRecord() {
    }

    public AuditRecord(Object id, Resource resource, Action action, Magnitude importance, ObjectMap before, ObjectMap after, long timeStamp,
                       String userId, String description, ObjectMap attributes) {
        this.id = id;
        this.resource = resource;
        this.action = action;
        this.importance = importance;
        this.before = before;
        this.after = after;
        this.timeStamp = timeStamp;
        this.userId = userId;
        this.description = description;
        this.attributes = attributes;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("AuditRecord{");
        sb.append("id=").append(id);
        sb.append(", resource=").append(resource);
        sb.append(", action=").append(action);
        sb.append(", importance=").append(importance);
        sb.append(", before=").append(before);
        sb.append(", after=").append(after);
        sb.append(", timeStamp=").append(timeStamp);
        sb.append(", userId='").append(userId).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", attributes=").append(attributes);
        sb.append('}');
        return sb.toString();
    }

    public Object getId() {
        return id;
    }

    public AuditRecord setId(Object id) {
        this.id = id;
        return this;
    }

    public Resource getResource() {
        return resource;
    }

    public AuditRecord setResource(Resource resource) {
        this.resource = resource;
        return this;
    }

    public Action getAction() {
        return action;
    }

    public AuditRecord setAction(Action action) {
        this.action = action;
        return this;
    }

    public ObjectMap getBefore() {
        return before;
    }

    public AuditRecord setBefore(ObjectMap before) {
        this.before = before;
        return this;
    }

    public ObjectMap getAfter() {
        return after;
    }

    public AuditRecord setAfter(ObjectMap after) {
        this.after = after;
        return this;
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public AuditRecord setTimeStamp(long timeStamp) {
        this.timeStamp = timeStamp;
        return this;
    }

    public String getUserId() {
        return userId;
    }

    public AuditRecord setUserId(String userId) {
        this.userId = userId;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public AuditRecord setDescription(String description) {
        this.description = description;
        return this;
    }

    public ObjectMap getAttributes() {
        return attributes;
    }

    public AuditRecord setAttributes(ObjectMap attributes) {
        this.attributes = attributes;
        return this;
    }

    public Magnitude getImportance() {
        return importance;
    }

    public AuditRecord setImportance(Magnitude importance) {
        this.importance = importance;
        return this;
    }
}
