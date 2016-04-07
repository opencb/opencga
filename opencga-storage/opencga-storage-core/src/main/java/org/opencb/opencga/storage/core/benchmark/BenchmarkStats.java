/*
 * Copyright 2015 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.storage.core.benchmark;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by imedina on 16/06/15.
 */
public class BenchmarkStats {

    private Map<String, List<Integer>> counters;
    private Map<String, List<Double>> std;

    public BenchmarkStats() {
        counters = new LinkedHashMap<>();
        std = new LinkedHashMap<>();
    }

    public void addExecutionTime(String counter, int executionTime) {
        if (!counters.containsKey(counter)) {
            counters.put(counter, new ArrayList<>());
        }
        counters.get(counter).add(executionTime);
    }

    public void addStdDeviation(String counter, double executionTime) {
        if (!std.containsKey(counter)) {
            std.put(counter, new ArrayList<>());
        }
        std.get(counter).add(executionTime);
    }

    public double avg(String counter) {
        if (counter != null && counters.get(counter) != null) {
            List<Integer> integers = counters.get(counter);
            double total = 0;
            for (Integer integer : integers) {
                total += integer.doubleValue();
            }
            double avgRoundOff = Math.round((total / integers.size()) * 1000d) / 1000d;
            return avgRoundOff; //total / integers.size();
        }
        return 0.0f;
    }

    double variance(String counter) {
        List<Integer> integers = counters.get(counter);
        double mean = avg(counter);
        double temp = 0;
        for (int a : integers) {
            temp += Math.pow((mean - a), 2);
        }
        double varianceRoundOff = Math.round((temp / integers.size()) * 1000d) / 1000d;
        return varianceRoundOff; //temp / integers.size();
    }

    double standardDeviation(String counter) {
        double stdDevRoundOff = Math.round(Math.sqrt(variance(counter)) * 1000d) / 1000d;
        return stdDevRoundOff; //Math.sqrt(variance(counter));
    }

//    private static final String ANSI_BLACK = "\u001B[30m";
//    private static final String ANSI_WHITE = "\u001B[37m";
//    private static final String ANSI_RESET = "\u001B[0m";
//    private static final String ANSI_RED = "\u001B[31m";
//    private static final String ANSI_BLUE = "\u001B[34m";
//    private static final String ANSI_PURPLE = "\u001B[35m";
    private static final String ANSI_GREEN = "\u001B[32m";
    private static final String ANSI_YELLOW = "\u001B[33m";
    private static final String ANSI_CYAN = "\u001B[36m";

    public void printSummary(String dbName, String tableName, int nuOfRepetition, int numOfThreads) {
        System.out.println(ANSI_YELLOW + "Following are the test stats");
        System.out.print(ANSI_GREEN + "Database name: " + ANSI_CYAN + dbName + ", ");
        System.out.print(ANSI_GREEN + "Table name: " + ANSI_CYAN + tableName + ", ");
        System.out.print(ANSI_GREEN + "Number of repetition: " + ANSI_CYAN + nuOfRepetition + ", ");
        System.out.print(ANSI_GREEN + "Number of parallel threads: " + ANSI_CYAN + numOfThreads);
        System.out.println();
        for (String key : counters.keySet()) {
            System.out.print(ANSI_GREEN + "Query: " + ANSI_CYAN + key);
            System.out.println();
            System.out.print(ANSI_GREEN + "Counter values: " + ANSI_CYAN + counters.get(key).toString() + ", ");
            System.out.print(ANSI_GREEN + "Average: " + ANSI_CYAN + avg(key) + ", ");
            System.out.print(ANSI_GREEN + "Variance: " + ANSI_CYAN + variance(key) + ", ");
            System.out.print(ANSI_GREEN + "Standard Deviation: " + ANSI_CYAN + standardDeviation(key));
            System.out.println();
        }
    }


    /*public void printSummary(String dbName, String tableName, int nuOfRepetition, int numOfThreads) {
        System.out.println("Following are the test stats");
        System.out.print("Database name: " + dbName + ", ");
        System.out.print("Table name: " + tableName + ", ");
        System.out.print("Number of repetition: " + nuOfRepetition + ", ");
        System.out.print("Number of parallel threads: " + numOfThreads);
        System.out.println();
        for (String key : counters.keySet()) {
            System.out.print("Query: " + key);
            System.out.println();
            System.out.print("Counter values: " + counters.get(key).toString() + ", ");
            System.out.print("avg: " + avg(key) + ", ");
            System.out.print("variance: " + variance(key) + ", ");
            System.out.print("standard deviation: " + standardDeviation(key));
            System.out.println();
        }
    }*/
}
