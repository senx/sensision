//
//   Copyright 2018  SenX S.A.S.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//

package io.warp10.sensision.hadoop;

import io.warp10.sensision.Sensision;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.configuration.SubsetConfiguration;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsSink;
import org.apache.hadoop.metrics2.MetricsTag;

public class SensisionSink implements MetricsSink {
    
  public static String HADOOP_SENSISION_TTL = "hadoop.sensision.ttl";
  
  private static final String SENSISION_SINK_PREFIX = "sensision.sink.prefix";
  
  private String prefix = "";
  
  private Long ttl = null;

  public SensisionSink() {
    if (null != System.getProperty(HADOOP_SENSISION_TTL)) {
      this.ttl = Long.parseLong(System.getProperty(HADOOP_SENSISION_TTL));
    }
  }
  
  @Override
  public void flush() {}
  
  @Override
  public void init(SubsetConfiguration config) {
    this.prefix = System.getProperty(SENSISION_SINK_PREFIX, "");
  }
  
  @Override
  public void putMetrics(MetricsRecord record) {
    Map<String,String> labels = new HashMap<String,String>();
    for (MetricsTag tag: record.tags()) {
      if (null != tag.value()) {
        labels.put(tag.name(), tag.value());
      }
    }
    
    StringBuilder sb = new StringBuilder();
    
    for (AbstractMetric metric: record.metrics()) {
      sb.setLength(0);
      sb.append(this.prefix);
      sb.append(metric.name());
      String cls = sb.toString();
      Sensision.set(cls, labels, record.timestamp() * Sensision.TIME_UNITS_PER_MS, null, null, null,  metric.value(), ttl);
    }
  }
}
