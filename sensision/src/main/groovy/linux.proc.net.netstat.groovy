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
 * Produce metrics about network statistics
 */

import java.io.PrintWriter;
import static io.warp10.sensision.Utils.*;

try {

  populateSymbolTable(this);

  SHOW_ERRORS = false;

  //
  // Common labels for all metrics
  //

  Map<String,String> commonLabels = [:];

  //
  // Output file
  //

  long now = System.currentTimeMillis() * 1000L;

  File OUTFILE = getMetricsFile('linux.proc.net.netstat');

  //
  // Open the file with a '.pending' suffix so it does not get picked up while we fill it
  //

  File outfile = OUTFILE;
  File tmpfile = new File("${OUTFILE.getAbsolutePath()}.pending");

  PrintWriter pw = new PrintWriter(tmpfile);

  BufferedReader br = null;

  labels = [:];
  labels.putAll(commonLabels);

  
  br = new BufferedReader(new FileReader("/proc/net/netstat"));

  lastprefix = null;
  varnames = [];

  while(true) {
    String line = br.readLine();
    
    if (null == line) {
      break;
    }
    
    String[] tokens = line.split('\\s+');

    //
    // Extract variable names when the line prefix changes
    //

    if (!tokens[0].equals(lastprefix)) {
      lastprefix = tokens[0];
      varnames = [];
      for (int i = 0; i < tokens.length; i++) {
        varnames.add(tokens[i]);
      }
      continue;
    } else {
      //
      // Same prefix as the previous line, extract prefix (protocol) and values
      //
      protocol = tokens[0].replaceAll(':.*','');

      for (int i = 1; i < tokens.length; i++) {
        value = Long.valueOf(tokens[i]);
        storeMetric(pw, now, "linux.proc.net.netstat.${protocol}.${varnames.get(i)}", labels, value);
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
  try { if (null != br) br.close(); } catch (IOException ioe) {}
  try { if (null != pw) pw.close(); } catch (IOException ioe) {}
}
