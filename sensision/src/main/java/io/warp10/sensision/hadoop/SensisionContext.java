//
//   Copyright 2016  Cityzen Data
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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.metrics.spi.AbstractMetricsContext;
import org.apache.hadoop.metrics.spi.OutputRecord;

public class SensisionContext extends AbstractMetricsContext {
  
  // Pattern to extract the labels from the metric name. Note that we use non greedy stars
  private static final Pattern RSDS_TBL_CF = Pattern.compile("^(tbl\\.(.*?)\\.)?(region\\.(.*?)\\.)?(cf\\.(.*?)\\.)?(bt\\.(.*?)\\.)?(at\\.(.*?)\\.)?([^.]+)$");
  
  private Long ttl = null;
  
  public SensisionContext() {
    if (null != System.getProperty(SensisionSink.HADOOP_SENSISION_TTL)) {
      this.ttl = Long.parseLong(System.getProperty(SensisionSink.HADOOP_SENSISION_TTL));
    }
  }

  @Override
  protected void emitRecord(String context, String recordName, OutputRecord outputRec) throws IOException {
    Map<String,String> labels = new HashMap<String,String>();
    
    for (String tagName: outputRec.getTagNames()) {
      labels.put(tagName, outputRec.getTag(tagName).toString());
    }
    
    labels.put("context", context);

    for (String metricName: outputRec.getMetricNames()) {
      
      //
      // Do some special handling of RegionServerDynamicStatistics
      //

      String cls;
      
      if (recordName.startsWith("RegionServerDynamicStatistics")) {        
        Matcher m = RSDS_TBL_CF.matcher(metricName);
        
        if (m.matches()) {
          if (null != m.group(1)) {
            labels.put("tbl", m.group(2));
          }
          if (null != m.group(3)) {
            labels.put("region", m.group(4));
          }
          if (null != m.group(5)) {
            labels.put("cf", m.group(6));
          }
          if (null != m.group(7)) {
            labels.put("bt", m.group(8));
          }
          if (null != m.group(9)) {
            labels.put("at", m.group(10));
          }
          cls = "" + recordName + "." + m.group(11);
          Sensision.set(cls, labels, outputRec.getMetric(metricName));
          labels.remove("tbl");
          labels.remove("region");
          labels.remove("cf");
          labels.remove("bt");
          labels.remove("at");
        } else {
          cls = "" + recordName + "." + metricName;
          Sensision.set(cls, labels, System.currentTimeMillis() * Sensision.TIME_UNITS_PER_MS, null, null, null, outputRec.getMetric(metricName), this.ttl);            
        }
      } else {
        cls = "" + recordName + "." + metricName;
        Sensision.set(cls, labels, System.currentTimeMillis() * Sensision.TIME_UNITS_PER_MS, null, null, null, outputRec.getMetric(metricName), this.ttl);        
      }
    }
  }
}
