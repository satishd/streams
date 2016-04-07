/**
 *
 */
package com.hortonworks.iotas.layout.runtime.pipeline;

import com.hortonworks.iotas.common.IotasEvent;
import com.hortonworks.iotas.common.Result;

import java.util.List;

/**
 *
 */
public interface Splitter {

    public List<Result> splitEvent(IotasEvent iotasEvent);
}
