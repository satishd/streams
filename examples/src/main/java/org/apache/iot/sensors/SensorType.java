/**
 * Autogenerated by Avro
 * 
 * DO NOT EDIT DIRECTLY
 */
package org.apache.iot.sensors;  
@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public enum SensorType { 
  temeperature, humidity, illuminance  ;
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"enum\",\"name\":\"SensorType\",\"namespace\":\"org.apache.iot.sensors\",\"symbols\":[\"temeperature\",\"humidity\",\"illuminance\"]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }
}
