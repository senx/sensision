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
 * Produce metrics for load average on a Linux box
 */

import java.io.PrintWriter;
import static io.warp10.sensision.Utils.*;

BufferedReader br = null;
PrintWriter pw = null;

try {

  SHOW_ERRORS = false;

  //
  // Common labels for all metrics
  //

  Map<String,String> commonLabels = [:];

  //
  // Output file
  //

  long now = System.currentTimeMillis() * 1000L;

  File OUTFILE = getMetricsFile('linux.proc.sys.fs');

  //
  // Open the file with a '.pending' suffix so it does not get picked up while we fill it
  //

  File outfile = OUTFILE;
  File tmpfile = new File("${OUTFILE.getAbsolutePath()}.pending");

  pw = new PrintWriter(tmpfile);

  //
  // Read /proc/proc/sys/fs/file-nr
  //

  br = new BufferedReader(new FileReader("/proc/sys/fs/file-nr"));
  
  labels = [:];
  labels.putAll(commonLabels);
 
  String line = br.readLine();
  
  String[] fields = line.split("\\s+");
  
  storeMetric(pw, now, 'linux.proc.sys.fs.file-nr.allocated', labels, Long.valueOf(fields[0]));

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
