//
//   Copyright 2018-2021  SenX S.A.S.
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
  // Common labels for all metrics
  //

  Map<String,String> commonLabels = [:];

  //
  // Output file
  //

  long now = System.currentTimeMillis() * 1000L;

  File OUTFILE = getMetricsFile('linux.proc.sys.kernel.random');

  //
  // Open the file with a '.pending' suffix so it does not get picked up while we fill it
  //

  File outfile = OUTFILE;
  tmpfile = new File("${OUTFILE.getAbsolutePath()}.pending");

  pw = new PrintWriter(tmpfile);

  labels = [:];
  labels.putAll(commonLabels);
  
  br = new BufferedReader(new FileReader("/proc/sys/kernel/random/entropy_avail"));

  while(true) {
    String line = br.readLine();
    
    if (null == line) {
      break;
    }
    
    long entropy_avail = Long.valueOf(line);

    storeMetric(pw, now, 'linux.proc.sys.kernel.random.entropy_avail', labels, entropy_avail);
  }

  //
  // Move file to final location
  //

  tmpfile.renameTo(outfile);

} catch (Exception e) {
  if (SHOW_ERRORS) { e.printStackTrace(System.err); }

  // Make sure the temp file is deleted if there was an error
  if (null != tmpfile) {
    tmpfile.delete();
  }
} finally {
  try { if (null != br) br.close(); } catch (IOException ioe) {}
  try { if (null != pw) pw.close(); } catch (IOException ioe) {}
}
