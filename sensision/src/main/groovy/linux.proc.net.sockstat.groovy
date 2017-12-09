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
 * Produce metrics about available random bits
 * This metric is important to watch as it becoming low
 * will have effects on any ops needing random seeding such
 * as crypto functions (as in SSL)
 */

import java.io.PrintWriter;
import static io.warp10.sensision.Utils.*;

BufferedReader br = null;
PrintWriter pw = null;
File tmpfile = null;

try {

  populateSymbolTable(this);

  SHOW_ERRORS = false;

  //
  // FILTER - If FILTER is not null, only protocols that match (exact match) this FILTER will be retained
  // FILTER_NAME = ['TCP','RAW','UDP'];
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

  File OUTFILE = getMetricsFile('linux.proc.net.sockstat');

  //
  // Open the file with a '.pending' suffix so it does not get picked up while we fill it
  //

  File outfile = OUTFILE;
  tmpfile = new File("${OUTFILE.getAbsolutePath()}.pending");

  pw = new PrintWriter(tmpfile);

  labels = [:];
  labels.putAll(commonLabels);

  for (String file: ['/proc/net/sockstat', '/proc/net/sockstat6']) {
    br = new BufferedReader(new FileReader("/proc/net/sockstat"));

    while(true) {
      String line = br.readLine();
    
      if (null == line) {
        break;
      }
    
      String[] tokens = line.split('\\s+');

      // Don't take into account the total stat
      if ("sockets:".equals(tokens[0])) {
        continue;
      }

      //
      // Retrieve protocol
      //

      String protocol = tokens[0].replaceAll(":.*","");

      if (null != FILTER_NAME) {
        if (!FILTER_NAME.contains(protocol)) {
          continue;
        }
      }

      for (int i = 1; i < tokens.length; i += 2) {
        String stat = tokens[i];
        long value = Long.valueOf(tokens[i+1]);

        labels['proto'] = protocol;
        storeMetric(pw, now, "linux.proc.net.sockstat.${stat}", labels, value);
      }
    }  

    br.close();
  }

  //
  // Move file to final location
  //

  tmpfile.renameTo(outfile);

} catch (Exception e) {
  if (SHOW_ERRORS) { e.printStackTrace(System.err); }
} finally {
  try { if (null != br) br.close(); } catch (IOException ioe) {}
  try {
    if (null != pw) {
      pw.close();
      if ((tmpfile.exists()) && (0 == tmpfile.length())) {
        tmpfile.delete()
      }
    }
  } catch (IOException ioe) {}
}
