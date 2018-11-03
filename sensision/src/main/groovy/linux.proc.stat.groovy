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

/**
 * Produce metrics about kernel statistics
 */

import java.io.PrintWriter;
import static io.warp10.sensision.Utils.*;

BufferedReader br = null;
PrintWriter pw = null;

try {

  populateSymbolTable(this);

  SHOW_ERRORS = false;

  //
  // FILTER - If FILTER is not null, only metrics that match (start with) this FILTER will be retained
  // FILTER_NAME = ['cpu','processes'];
  //
  FILTER_NAME = null;

  //
  // Common labels for all metrics
  //

  Map<String,String> commonLabels = [:];

  //
  // Output file
  //

  long now = System.currentTimeMillis() * 1000L;

  File OUTFILE = getMetricsFile('linux.proc.stat');

  //
  // Open the file with a '.pending' suffix so it does not get picked up while we fill it
  //

  File outfile = OUTFILE;
  File tmpfile = new File("${OUTFILE.getAbsolutePath()}.pending");

  pw = new PrintWriter(tmpfile);

  labels = [:];
  labels.putAll(commonLabels);

  br = new BufferedReader(new FileReader("/proc/stat"));

  while (true) {        
    String line = br.readLine();

    if (null == line) {
      break;
    }

    String[] tokens = line.split("\\s+");
    
    // Is the current metric name match selection (FILTER) - true by default
    boolean selected = true;

    if (null != FILTER_NAME) {
      selected = FILTER_NAME.find { 
        if (tokens[0].startsWith(it)) { 
          return true;
        }
        return false;
      }
    }

    if (selected) {
      if (tokens[0].startsWith("cpu")) {
        cpu = tokens[0];
        userhz_user = Long.valueOf(tokens[1]);
        userhz_nice = Long.valueOf(tokens[2]);
        userhz_system = Long.valueOf(tokens[3]);
        userhz_idle = Long.valueOf(tokens[4]);
        userhz_iowait = Long.valueOf(tokens[5]);
        userhz_irq = Long.valueOf(tokens[6]);
        userhz_softirq = Long.valueOf(tokens[7]);

        labels['cpu'] = cpu;

        storeMetric(pw, now, 'linux.proc.stat.userhz.user', labels, userhz_user);
        storeMetric(pw, now, 'linux.proc.stat.userhz.nice', labels, userhz_nice);
        storeMetric(pw, now, 'linux.proc.stat.userhz.system', labels, userhz_system);
        storeMetric(pw, now, 'linux.proc.stat.userhz.idle', labels, userhz_idle);
        storeMetric(pw, now, 'linux.proc.stat.userhz.iowait', labels, userhz_iowait);
        storeMetric(pw, now, 'linux.proc.stat.userhz.irq', labels, userhz_irq);
        storeMetric(pw, now, 'linux.proc.stat.userhz.softirq', labels, userhz_softirq);

        labels.remove('cpu');
      } else if ("intr".equals(tokens[0])) {
        interrupts = Long.valueOf(tokens[1]);
        labels['irq'] = 'all';
        storeMetric(pw, now, 'linux.proc.stat.interrupts', labels, interrupts);
        for (int i = 2; i < tokens.length; i++) {
          interrupts = Long.valueOf(tokens[i]);
          // Only emit a metric if interrupt count > 0
          if (interrupts > 0) {
            labels['irq'] = Long.toString(i - 2);
            storeMetric(pw, now, 'linux.proc.stat.interrupts', labels, interrupts);
          }
        }
        labels.remove('irq');
      } else if ("ctxt".equals(tokens[0])) {
        ctxt = Long.valueOf(tokens[1]);
        storeMetric(pw, now, 'linux.proc.stat.ctxt', labels, ctxt);
      } else if ("btime".equals(tokens[0])) {
        btime = Long.valueOf(tokens[1]);
        storeMetric(pw, now, 'linux.proc.stat.btime', labels, btime);
      } else if ("processes".equals(tokens[0])) {
        processes = Long.valueOf(tokens[1]);
        storeMetric(pw, now, 'linux.proc.stat.processes', labels, processes);
      } else if ("softirq".equals(tokens[0])) {
        interrupts = Long.valueOf(tokens[1]);
        labels['irq'] = 'all';
        storeMetric(pw, now, 'linux.proc.stat.softirqs', labels, interrupts);
        for (int i = 2; i < tokens.length; i++) {
          interrupts = Long.valueOf(tokens[i]);
          // Only emit a metric if interrupt count > 0
          if (interrupts > 0) {
            labels['irq'] = Long.toString(i - 2);
            storeMetric(pw, now, 'linux.proc.stat.softirqs', labels, interrupts);
          }
        }
        labels.remove('irq');
      }
    }
  }

  //
  // Move file to final location
  //

  tmpfile.renameTo(outfile);

} catch (Exception e) {
  if (SHOW_ERRORS) { e.printStackTrace(System.err); }
} finally {
  try { br.close(); } catch (IOException ioe) {}
  try { if (null != pw) pw.close(); } catch (IOException ioe) {}
}