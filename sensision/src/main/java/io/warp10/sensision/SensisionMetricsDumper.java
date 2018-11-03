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

package io.warp10.sensision;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * Thread which will periodically dump all known metrics to
 * SENSISION_HOME
 */
public class SensisionMetricsDumper extends Thread {
  
  /**
   * Delay in ms between two dumps
   */
  private long period;
      
  private boolean useValueTS;
  
  public SensisionMetricsDumper() {
    
    if (Sensision.disable) {
      return;
    }
    
    try {
      this.period = Long.valueOf(System.getProperty(Sensision.SENSISION_DUMP_PERIOD));
    } catch (NumberFormatException nfe) {
      this.period = Sensision.DEFAULT_DUMP_PERIOD;        
    }
  
    if ("true".equals(System.getProperty(Sensision.SENSISION_DUMP_CURRENTTS))) {
      this.useValueTS = false;
    } else {
      this.useValueTS = true;
    }
    
    this.setDaemon(true);
    this.setName("Sensision MetricsDumper");
    this.start();
  }
  
  @Override
  public void run() {
    while(true) {
      try {
        Thread.sleep(this.period);
      } catch (InterruptedException ie) {
        continue;
      }

      flushMetrics(this);
      Sensision.flushEvents();
    }
  }
  
  public static void flushMetrics(SensisionMetricsDumper dumper) {
    String home = System.getProperty(Sensision.SENSISION_HOME, Sensision.DEFAULT_SENSISION_HOME);
    
    // Build the filename for the registration as a list of dot separated tokens
    
    // First token is the current time reversed.
    String filename = Long.toHexString(Long.MAX_VALUE - System.currentTimeMillis());
    
    // next token is the reversed timestamp of start time
    filename = filename + ".";
    filename = filename + Long.toHexString(Long.MAX_VALUE - Sensision.getStartTime());
    
    // Next token is the Sensision uuid
    filename = filename + ".";
    filename = filename + Sensision.getUUID();
    
    filename = filename + Sensision.SENSISION_METRICS_SUFFIX;
    
    File metricsDir = new File(home, Sensision.SENSISION_METRICS_SUBDIR);
    File reg = new File(metricsDir, filename + ".tmp");

    PrintWriter out = null;
    
    try {
      out = new PrintWriter(reg);
      if (null == dumper) {
        Sensision.dump(out);
      } else {
        dumper.dump(out);
      }
    } catch (IOException ioe) {
    } finally {
      if (null != out) {
        out.close();
      }
    }
    
    if (0 == reg.length()) {
      // Delete if no metrics were written
      reg.delete();
    } else {
      // Rename file atomically

      reg.renameTo(new File(metricsDir, filename));
    }    
  }
  
  public void dump(PrintWriter out) throws IOException {
    Sensision.dump(out, this.useValueTS);
  }
}
