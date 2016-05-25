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

package io.warp10.sensision.kafka;

import io.warp10.sensision.yammermetrics.SensisionReporter;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.MetricPredicate;

import kafka.metrics.KafkaMetricsReporter;
import kafka.utils.VerifiableProperties;

public class KafkaSensisionReporter implements KafkaMetricsReporter {
  
  private SensisionReporter reporter;
  
  private MetricPredicate predicate = MetricPredicate.ALL;
  
  @Override
  public void init(VerifiableProperties props) {
    this.reporter = new SensisionReporter("kafka.", Metrics.defaultRegistry(), this.predicate);
  }
}
