package org.opencb.opencga.core.models.clinical;

import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.sample.Sample;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class ProbandParam {
    private String id;
    private List<SampleParams> samples;

    public ProbandParam() {
    }

    public ProbandParam(String id, List<SampleParams> samples) {
        this.id = id;
        this.samples = samples;
    }

    public static ProbandParam of(Individual individual) {
        return new ProbandParam(individual.getId(),
                individual.getSamples() != null
                        ? individual.getSamples().stream().map(SampleParams::of).collect(Collectors.toList())
                        : Collections.emptyList());
    }

    public Individual toIndividual() {
        List<Sample> sampleList = new ArrayList<>(samples != null ? samples.size() : 0);
        if (samples != null) {
            for (SampleParams sample : samples) {
                sampleList.add(new Sample().setId(sample.getId()));
            }
        }

        return new Individual()
                .setId(id)
                .setSamples(sampleList);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ProbandParam{");
        sb.append("id='").append(id).append('\'');
        sb.append(", samples=").append(samples);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public ProbandParam setId(String id) {
        this.id = id;
        return this;
    }

    public List<SampleParams> getSamples() {
        return samples;
    }

    public ProbandParam setSamples(List<SampleParams> samples) {
        this.samples = samples;
        return this;
    }

}
