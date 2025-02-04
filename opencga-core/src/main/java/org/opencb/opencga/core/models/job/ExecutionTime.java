package org.opencb.opencga.core.models.job;

public class ExecutionTime {

    private String month;
    private String year;
    private Time time;

    public ExecutionTime() {
    }

    public ExecutionTime(String month, String year, Time time) {
        this.month = month;
        this.year = year;
        this.time = time;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ExecutionTime{");
        sb.append("month='").append(month).append('\'');
        sb.append(", year='").append(year).append('\'');
        sb.append(", time=").append(time);
        sb.append('}');
        return sb.toString();
    }

    public String getMonth() {
        return month;
    }

    public ExecutionTime setMonth(String month) {
        this.month = month;
        return this;
    }

    public String getYear() {
        return year;
    }

    public ExecutionTime setYear(String year) {
        this.year = year;
        return this;
    }

    public Time getTime() {
        return time;
    }

    public ExecutionTime setTime(Time time) {
        this.time = time;
        return this;
    }

    public static class Time {
        private double hours;
        private double minutes;
        private double seconds;

        public Time() {
        }

        public Time(double hours, double minutes, double seconds) {
            this.hours = hours;
            this.minutes = minutes;
            this.seconds = seconds;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("Time{");
            sb.append("hours=").append(hours);
            sb.append(", minutes=").append(minutes);
            sb.append(", seconds=").append(seconds);
            sb.append('}');
            return sb.toString();
        }

        public double getHours() {
            return hours;
        }

        public Time setHours(double hours) {
            this.hours = hours;
            return this;
        }

        public double getMinutes() {
            return minutes;
        }

        public Time setMinutes(double minutes) {
            this.minutes = minutes;
            return this;
        }

        public double getSeconds() {
            return seconds;
        }

        public Time setSeconds(double seconds) {
            this.seconds = seconds;
            return this;
        }
    }

}
