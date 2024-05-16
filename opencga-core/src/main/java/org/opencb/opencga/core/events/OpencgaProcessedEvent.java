package org.opencb.opencga.core.events;

import java.util.List;

public class OpencgaProcessedEvent {

    private String user;
    private String date;
    private OpencgaEvent payload;
    private Subscriber subscriber;
    private Status status;

    public OpencgaProcessedEvent() {
    }

    public OpencgaProcessedEvent(String user, String date, OpencgaEvent payload, Subscriber subscriber, Status status) {
        this.user = user;
        this.date = date;
        this.payload = payload;
        this.subscriber = subscriber;
        this.status = status;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("OpencgaProcessedEvent{");
        sb.append("user='").append(user).append('\'');
        sb.append(", date='").append(date).append('\'');
        sb.append(", payload=").append(payload);
        sb.append(", subscriber=").append(subscriber);
        sb.append(", status=").append(status);
        sb.append('}');
        return sb.toString();
    }

    public String getUser() {
        return user;
    }

    public OpencgaProcessedEvent setUser(String user) {
        this.user = user;
        return this;
    }

    public String getDate() {
        return date;
    }

    public OpencgaProcessedEvent setDate(String date) {
        this.date = date;
        return this;
    }

    public OpencgaEvent getPayload() {
        return payload;
    }

    public OpencgaProcessedEvent setPayload(OpencgaEvent payload) {
        this.payload = payload;
        return this;
    }

    public Subscriber getSubscriber() {
        return subscriber;
    }

    public OpencgaProcessedEvent setSubscriber(Subscriber subscriber) {
        this.subscriber = subscriber;
        return this;
    }

    public Status getStatus() {
        return status;
    }

    public OpencgaProcessedEvent setStatus(Status status) {
        this.status = status;
        return this;
    }

    /* Additional classes */
    public enum Status {
        PROCESSING,
        DONE,
        ERROR
    }

    public static class Subscriber {
        private List<String> success;
        private List<String> error;

        public Subscriber() {
        }

        public List<String> getSuccess() {
            return success;
        }

        public Subscriber setSuccess(List<String> success) {
            this.success = success;
            return this;
        }

        public List<String> getError() {
            return error;
        }

        public Subscriber setError(List<String> error) {
            this.error = error;
            return this;
        }
    }

}
