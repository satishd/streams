package com.hortonworks.iotas.layout.runtime;

import com.hortonworks.iotas.common.IotasEvent;
import com.hortonworks.iotas.common.Result;
import com.hortonworks.iotas.layout.runtime.transform.IdentityTransformRuntime;
import com.hortonworks.iotas.layout.runtime.transform.TransformRuntime;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TransformActionRuntime implements ActionRuntime {
    private final String stream;
    private final List<TransformRuntime> transformRuntimes;

    /**
     * Creates a new {@link TransformActionRuntime}
     *
     * @param stream the stream where the results are sent out
     */
    public TransformActionRuntime(String stream) {
        this(stream, Collections.<TransformRuntime>singletonList(new IdentityTransformRuntime()));
    }

    /**
     * Creates a new {@link TransformActionRuntime}
     *
     * @param stream  the stream where the results are sent out
     * @param transformRuntimes the chain of transformations to be applied (in order)
     */
    public TransformActionRuntime(String stream, List<TransformRuntime> transformRuntimes) {
        this.stream = stream;
        this.transformRuntimes = transformRuntimes;
    }

    /**
     * {@inheritDoc}
     * Recursively applies the list of {@link TransformRuntime} (s) associated with this
     * TransformAction object and returns the {@link Result}
     */
    @Override
    public List<Result> execute(IotasEvent input) {
        return Collections.singletonList(new Result(stream, doTransform(input)));
    }

    /*
     * applies the transformation chain to the input and returns the transformed events
     */
    private List<IotasEvent> doTransform(IotasEvent input) {
        return doTransform(input, 0);
    }

    /*
     * applies the i th transform and recursively invokes the method to apply
     * the rest of the transformations in the chain.
     */
    private List<IotasEvent> doTransform(IotasEvent inputEvent, int i) {
        if (i >= transformRuntimes.size()) {
            return Collections.singletonList(inputEvent);
        }
        List<IotasEvent> transformed = new ArrayList<>();
        final List<IotasEvent> iotasEvents = transformRuntimes.get(i).execute(inputEvent);
        //todo handle split/join events here.
        // add empty event when it returns null or empty collection
        // update total-count iotasEvents size is > 1
        for (IotasEvent event : iotasEvents) {
            transformed.addAll(doTransform(event, i + 1));
        }
        return transformed;
    }

    @Override
    public List<String> getOutputStreams() {
        return Collections.singletonList(stream);
    }

    @Override
    public String toString() {
        return "TransformAction{" +
                "stream='" + stream + '\'' +
                ", transforms=" + transformRuntimes +
                '}';
    }
}
