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

package sensision;

import io.warp10.sensision.Sensision;
import io.warp10.sensision.yammermetrics.SensisionReporter;

import java.io.PrintWriter;

import org.junit.Test;

import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricPredicate;
import com.yammer.metrics.core.MetricsRegistry;

public class SensisionReporterTest {
  @Test
  public void test() throws Exception {
    MetricsRegistry registry = new MetricsRegistry();
    //System.setProperty("sensision.default.labels","host=foo,rack=toto");
    SensisionReporter reporter = new SensisionReporter(registry, MetricPredicate.ALL);
    
    Counter counter = registry.newCounter(new MetricName(this.getClass(), "counter"));

    counter.inc();
    PrintWriter pw = new PrintWriter(System.out);
    Sensision.dump(pw);
    pw.flush();
  }
}
