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
 * Produce metrics about memory
 */

import java.io.PrintWriter;
import static io.warp10.sensision.Utils.*;

BufferedReader br = null;
PrintWriter pw = null;

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

  File OUTFILE = getMetricsFile('linux.proc.meminfo');

  //
  // Open the file with a '.pending' suffix so it does not get picked up while we fill it
  //

  File outfile = OUTFILE;
  File tmpfile = new File("${OUTFILE.getAbsolutePath()}.pending");

  pw = new PrintWriter(tmpfile);

  labels = [:];
  labels.putAll(commonLabels);

  br = new BufferedReader(new FileReader("/proc/meminfo"));

  while(true) {
    String line = br.readLine();
    
    if (null == line) {
      break;
    }
    
    String[] tokens = line.split("[:\\s]+");
    
    name = tokens[0];
    value = Long.valueOf(tokens[1]);

    if (tokens.length > 2) {
      if ("kB".equals(tokens[2])) {
        // kB in /proc/meminfo means 1024 bytes,
        // you can see for yourself by running 'free -b' and comparing
        value = value * 1024L;
      }
    }

    storeMetric(pw, now, "linux.proc.meminfo.${name}", labels, value);
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