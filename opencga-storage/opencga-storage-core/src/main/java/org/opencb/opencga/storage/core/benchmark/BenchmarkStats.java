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

    public void addExecutionTime(String counter, int time) {
        if (!counters.containsKey(counter)) {
            counters.put(counter, new ArrayList<>());
        }
        counters.get(counter).add(time);
    }

    public void addStdDeviation(String counter, double time) {
        if (!std.containsKey(counter)) {
            std.put(counter, new ArrayList<>());
        }
        std.get(counter).add(time);
    }

    public double avg(String counter) {
        if (counter != null && counters.get(counter) != null) {
            List<Integer> integers = counters.get(counter);
            double total = 0;
            for (Integer integer : integers) {
                total += integer.doubleValue();
            }
            double roundOff = Math.round((total / integers.size()) * 1000d) / 1000d;
            return roundOff; //total / integers.size();
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
        double roundOff = Math.round((temp / integers.size()) * 1000d) / 1000d;
        return roundOff; //temp / integers.size();
    }

    double standardDeviation(String counter) {
        double roundOff = Math.round(Math.sqrt(variance(counter)) * 1000d) / 1000d;
        return roundOff; //Math.sqrt(variance(counter));
    }

    public void printSummary() {
        for (String key : counters.keySet()) {
            System.out.print("Counter: " + key + ", ");
            System.out.print("values: " + counters.get(key).toString() + ", ");
            System.out.print("avg: " + avg(key) + ", ");
            System.out.print("variance: " + variance(key) + ", ");
            System.out.print("standard deviation: " + standardDeviation(key));
            System.out.println();
        }
    }
}
