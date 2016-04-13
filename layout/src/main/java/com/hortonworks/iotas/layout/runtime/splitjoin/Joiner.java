/**
 *
 */
package com.hortonworks.iotas.layout.runtime.splitjoin;

import com.hortonworks.iotas.common.IotasEvent;

/**
 * Joins the received {@link EventGroup} and generates a resultant {@link IotasEvent}
 *
 * todo write a default joiner
 */
public interface Joiner {

    public IotasEvent join(EventGroup eventGroup);
}
