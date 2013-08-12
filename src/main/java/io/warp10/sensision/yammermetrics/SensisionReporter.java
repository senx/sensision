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

package io.warp10.sensision.yammermetrics;

import io.warp10.sensision.Sensision;
import io.warp10.sensision.Sensision.Value;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Metered;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricPredicate;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.reporting.AbstractPollingReporter;
import com.yammer.metrics.stats.Snapshot;

public class SensisionReporter extends AbstractPollingReporter implements Iterable<Value> {
  
  private final MetricPredicate predicate;
  private final String prefix;
  
  public SensisionReporter(MetricsRegistry registry, MetricPredicate predicate) {
    this("", registry, predicate);
  }
  
  public SensisionReporter(String prefix, MetricsRegistry registry, MetricPredicate predicate) {
    super(registry, "sensision-reporter");
    this.predicate = predicate;
    this.prefix = prefix;
    Sensision.addValueProvider(this);
  }
  
  
  @Override
  public void run() {}
  
  @Override
  public Iterator<Value> iterator() {
    final List<Value> values = new ArrayList<Value>();
    
    for (Map.Entry<String, SortedMap<MetricName, Metric>> entry : getMetricsRegistry().groupedMetrics(predicate).entrySet()) {
      for (Map.Entry<MetricName, Metric> subEntry : entry.getValue().entrySet()) {
        final Metric metric = subEntry.getValue();
        if (metric != null) {
          try {            
            if (metric instanceof Counter) {
              values.addAll(counterValues(subEntry.getKey(), (Counter) subEntry.getValue()));
            } else if (metric instanceof Gauge<?>) {
              values.addAll(gaugeValues(subEntry.getKey(), (Gauge<?>) subEntry.getValue()));
            } else if (metric instanceof Histogram) {
              values.addAll(histogramValues(subEntry.getKey(), (Histogram) subEntry.getValue()));
            } else if (metric instanceof Metered) {
              values.addAll(meterValues(subEntry.getKey(), (Metered) subEntry.getValue()));
            } else if (metric instanceof Timer) {
              values.addAll(timerValues(subEntry.getKey(), (Timer) subEntry.getValue()));
            }            
          } catch (Exception e) {            
          }
        }
      }
    }            

    final int size = values.size();
    
    return new Iterator<Value>() {
      int idx = 0;
      
      @Override
      public boolean hasNext() {
        return idx < size;
      }
      
      @Override
      public Value next() {
        return values.get(idx++);
      }
      
      @Override
      public void remove() {}
    };
  }

  public Collection<Value> counterValues(MetricName name, Counter counter) throws Exception {    
    List<Value> values = new ArrayList<Value>();
    
    long now = System.currentTimeMillis() * 1000L;
    
    Map<String,String> labels = new HashMap<String,String>();;

    labels.put("group", name.getGroup());
    
    if (name.hasScope()) {
      labels.put("scope", name.getScope());
    }
    
    labels.put("type", name.getType());

    values.add(new Value(prefix + name.getName() + ":count", labels, now, null, null, null, counter.count()));
    
    return values;
  }
  

  public Collection<Value> gaugeValues(MetricName name, Gauge<?> gauge) throws Exception {
    List<Value> values = new ArrayList<Value>();

    long now = System.currentTimeMillis() * 1000L;
    
    Map<String,String> labels = new HashMap<String,String>();;

    labels.put("group", name.getGroup());
    
    if (name.hasScope()) {
      labels.put("scope", name.getScope());
    }
    
    labels.put("type", name.getType());

    Object value = gauge.value();
    
    if (value instanceof Number || value instanceof Boolean) {
      values.add(new Value(prefix + name.getName(), labels, now, null, null, null, value));      
    } else {
      values.add(new Value(prefix + name.getName(), labels, now, null, null, null, value.toString()));            
    }
    
    return values;
  }
  
  public Collection<Value> histogramValues(MetricName name, Histogram histogram) throws Exception {    
    List<Value> values = new ArrayList<Value>();
    
    long now = System.currentTimeMillis() * 1000L;
    
    Map<String,String> labels = new HashMap<String,String>();;

    labels.put("group", name.getGroup());
    
    if (name.hasScope()) {
      labels.put("scope", name.getScope());
    }
    
    labels.put("type", name.getType());

    String vname = prefix + name.getName();
    
    values.add(new Value(vname + ":count", labels, now, null, null, null, histogram.count()));
    values.add(new Value(vname + ":max", labels, now, null, null, null, histogram.max()));
    values.add(new Value(vname + ":min", labels, now, null, null, null, histogram.min()));
    values.add(new Value(vname + ":mean", labels, now, null, null, null, histogram.mean()));
    values.add(new Value(vname + ":sd", labels, now, null, null, null, histogram.stdDev()));
    values.add(new Value(vname + ":sum", labels, now, null, null, null, histogram.sum()));
    
    final Snapshot snapshot = histogram.getSnapshot();
    
    values.add(new Value(vname + ":median", labels, now, null, null, null, snapshot.getMedian()));
    values.add(new Value(vname + ":75percentile", labels, now, null, null, null, snapshot.get75thPercentile()));
    values.add(new Value(vname + ":95percentile", labels, now, null, null, null, snapshot.get95thPercentile()));
    values.add(new Value(vname + ":98percentile", labels, now, null, null, null, snapshot.get98thPercentile()));
    values.add(new Value(vname + ":99percentile", labels, now, null, null, null, snapshot.get99thPercentile()));
    values.add(new Value(vname + ":999percentile", labels, now, null, null, null, snapshot.get999thPercentile()));
    
    return values;
  }
  
  public Collection<Value> meterValues(MetricName name, Metered meter) throws Exception {
    List<Value> values = new ArrayList<Value>();
    
    long now = System.currentTimeMillis() * 1000L;
    
    Map<String,String> labels = new HashMap<String,String>();;

    labels.put("group", name.getGroup());

    if (name.hasScope()) {
      labels.put("scope", name.getScope());
    }

    labels.put("type", name.getType());
    labels.put("event", meter.eventType());
    
    String vname = prefix + name.getName();
    
    values.add(new Value(vname + ":count", labels, now, null, null, null, meter.count()));
    values.add(new Value(vname + ":rate:15m", labels, now, null, null, null, meter.fifteenMinuteRate()));
    values.add(new Value(vname + ":rate:5m", labels, now, null, null, null, meter.fiveMinuteRate()));
    values.add(new Value(vname + ":rate:1m", labels, now, null, null, null, meter.oneMinuteRate()));
    values.add(new Value(vname + ":rate:mean", labels, now, null, null, null, meter.meanRate()));
    
    return values;
  }
  
  public Collection<Value> timerValues(MetricName name, Timer timer) throws Exception {    
    List<Value> values = new ArrayList<Value>();
    
    long now = System.currentTimeMillis() * 1000L;
    
    Map<String,String> labels = new HashMap<String,String>();;

    labels.put("group", name.getGroup());
    
    if (name.hasScope()) {
      labels.put("scope", name.getScope());
    }
    
    labels.put("type", name.getType());
    
    String vname = prefix + name.getName();
    
    values.add(new Value(vname + ":count", labels, now, null, null, null, timer.count()));
    values.add(new Value(vname + ":rate:15m", labels, now, null, null, null, timer.fifteenMinuteRate()));
    values.add(new Value(vname + ":rate:5m", labels, now, null, null, null, timer.fiveMinuteRate()));
    values.add(new Value(vname + ":rate:1m", labels, now, null, null, null, timer.oneMinuteRate()));
    values.add(new Value(vname + ":rate:mean", labels, now, null, null, null, timer.meanRate()));
    values.add(new Value(vname + ":max", labels, now, null, null, null, timer.max()));
    values.add(new Value(vname + ":min", labels, now, null, null, null, timer.min()));
    values.add(new Value(vname + ":mean", labels, now, null, null, null, timer.mean()));
    values.add(new Value(vname + ":sd", labels, now, null, null, null, timer.stdDev()));
    values.add(new Value(vname + ":sum", labels, now, null, null, null, timer.sum()));

    final Snapshot snapshot = timer.getSnapshot();
    
    values.add(new Value(vname + ":median", labels, now, null, null, null, snapshot.getMedian()));
    values.add(new Value(vname + ":75percentile", labels, now, null, null, null, snapshot.get75thPercentile()));
    values.add(new Value(vname + ":95percentile", labels, now, null, null, null, snapshot.get95thPercentile()));
    values.add(new Value(vname + ":98percentile", labels, now, null, null, null, snapshot.get98thPercentile()));
    values.add(new Value(vname + ":99percentile", labels, now, null, null, null, snapshot.get99thPercentile()));
    values.add(new Value(vname + ":999percentile", labels, now, null, null, null, snapshot.get999thPercentile()));

    return values;
  }
}
