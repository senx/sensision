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

/**
 * Produce metrics for network statistics on a Linux box
 */

import java.io.PrintWriter;
import static io.warp10.sensision.Utils.*;

populateSymbolTable(this);

//
// FILTER - If FILTER is not null, only interface that matches (exact match) these names will be retained
// FILTER_NAME = ['lo','eth0'];
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

File OUTFILE = getMetricsFile('linux.proc.net.dev');

//
// Open the file with a '.pending' suffix so it does not get picked up while we fill it
//

File outfile = OUTFILE;
File tmpfile = new File("${OUTFILE.getAbsolutePath()}.pending");

PrintWriter pw = new PrintWriter(tmpfile);

BufferedReader br = null;



try {
  //
  // Open /proc/net/dev
  //

  br = new BufferedReader(new FileReader("/proc/net/dev"));

  while(true) {        
    String line = br.readLine();

    if (null == line) {
      break;
    }

    //
    // Lines with valid statistics start with the interface name followed by ':'
    //

    if (!line.contains(":")) {
      continue;
    }
    
    //
    // Split on whitespace
    //

    String[] tokens = line.trim().split("[:\\s]+");
    
    String iface = tokens[0];

    if (null != FILTER_NAME) {
      if (!FILTER_NAME.contains(iface)) {
        continue;
      }
    }

    labels = [:];
    labels['iface'] = iface;
    labels.putAll(commonLabels);
    
    int idx = 1;
    storeMetric(pw, now, 'linux.proc.net.dev.receive.bytes', labels, Long.valueOf(tokens[idx++]));
    storeMetric(pw, now, 'linux.proc.net.dev.receive.packets', labels, Long.valueOf(tokens[idx++]));
    storeMetric(pw, now, 'linux.proc.net.dev.receive.errs', labels, Long.valueOf(tokens[idx++]));
    storeMetric(pw, now, 'linux.proc.net.dev.receive.drops', labels, Long.valueOf(tokens[idx++]));
    storeMetric(pw, now, 'linux.proc.net.dev.receive.fifo', labels, Long.valueOf(tokens[idx++]));
    storeMetric(pw, now, 'linux.proc.net.dev.receive.frame', labels, Long.valueOf(tokens[idx++]));
    storeMetric(pw, now, 'linux.proc.net.dev.receive.compressed', labels, Long.valueOf(tokens[idx++]));
    storeMetric(pw, now, 'linux.proc.net.dev.receive.multicast', labels, Long.valueOf(tokens[idx++]));
    storeMetric(pw, now, 'linux.proc.net.dev.transmit.bytes', labels, Long.valueOf(tokens[idx++]));
    storeMetric(pw, now, 'linux.proc.net.dev.transmit.packets', labels, Long.valueOf(tokens[idx++]));
    storeMetric(pw, now, 'linux.proc.net.dev.transmit.errs', labels, Long.valueOf(tokens[idx++]));
    storeMetric(pw, now, 'linux.proc.net.dev.transmit.drops', labels, Long.valueOf(tokens[idx++]));
    storeMetric(pw, now, 'linux.proc.net.dev.transmit.fifo', labels, Long.valueOf(tokens[idx++]));
    storeMetric(pw, now, 'linux.proc.net.dev.transmit.colls', labels, Long.valueOf(tokens[idx++]));
    storeMetric(pw, now, 'linux.proc.net.dev.transmit.carrier', labels, Long.valueOf(tokens[idx++]));
    storeMetric(pw, now, 'linux.proc.net.dev.transmit.compressed', labels, Long.valueOf(tokens[idx++]));
  }
} catch (IOException ioe) {        
} finally {
  try { if (null != br) br.close(); } catch (IOException ioe) {}
  try { if (null != pw) pw.close(); } catch (IOException ioe) {}
}

//
// Move file to final location
//

tmpfile.renameTo(outfile);
