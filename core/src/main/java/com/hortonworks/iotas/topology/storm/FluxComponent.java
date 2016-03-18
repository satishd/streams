package com.hortonworks.iotas.topology.storm;

import com.hortonworks.iotas.util.exception.BadTopologyLayoutException;

import java.util.List;
import java.util.Map;

/**
 * An interface to be implemented by different storm components like spout,
 * bolt, streams to return the flux yaml equivalent of the component given a
 * configuration
 */
public interface FluxComponent {

    /**
     * Initialize the implementation with catalog root url
     */
    void withCatalogRootUrl(String catalogRootUrl);

    /**
     * Initializes the implementation with a configuration
     */
    void withConfig(Map<String, Object> config);

    /**
     * Returns yaml maps of all the components referenced by this component
     * Expected to return equivalent of something like below.
     * <pre>
     * - id: "zkHosts"
     * className: "org.apache.storm.kafka.ZkHosts"
     * constructorArgs:
     *  - ${kafka.spout.zkUrl}
     *  - id: "spoutConfig"
     * className: "org.apache.storm.kafka.SpoutConfig"
     * constructorArgs:
     *  - ref: "zkHosts"
     * </pre>
     */
    List<Map<String, Object>> getReferencedComponents();

    /**
     * Get yaml map for this component. Note that the id field will be
     * overwritten and hence is optional.
     * Expected to return equivalent of something like below
     * <pre>
     * - id: "KafkaSpout"
     * className: "org.apache.storm.kafka.KafkaSpout"
     * constructorArgs:
     *  - ref: "spoutConfig"
     * </pre>
     */
    Map<String, Object> getComponent();

    /**
     * Validate the configuration for this component.
     *
     * @throws BadTopologyLayoutException if configuration is not correct
     */
    void validateConfig() throws BadTopologyLayoutException;
}
