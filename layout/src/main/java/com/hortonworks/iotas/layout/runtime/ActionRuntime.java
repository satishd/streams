package com.hortonworks.iotas.layout.runtime;

import com.hortonworks.iotas.common.IotasEvent;
import com.hortonworks.iotas.common.Result;

import java.io.Serializable;
import java.util.List;

/**
 * Runtime abstraction for the action to be taken when a rule matches the condition
 */
public interface ActionRuntime extends Serializable {

    /**
     * Execute the current action and return a {@link List} of {@link Result}s.
     *
     * @param input the input IotasEvent
     * @return the result
     */
    List<Result> execute(IotasEvent input);


    /**
     * The streams where the result of this action are sent out
     *
     * @return streams where the result of this action are sent out
     */
    List<String> getOutputStreams();

}
