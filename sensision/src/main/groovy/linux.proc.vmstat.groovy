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
 * Produce metrics for vm statistics on a Linux box
 */

import java.io.PrintWriter;
import static io.warp10.sensision.Utils.*;

populateSymbolTable(this);

SHOW_ERRORS = false;

//
// FILTER - If FILTER is not null, only vm metrics that match (exact match) this FILTER will be retained
// FILTER_NAME = ['nr_file_pages','pgalloc_normal'];
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

File OUTFILE = getMetricsFile('linux.proc.vmstat');

//
// Open the file with a '.pending' suffix so it does not get picked up while we fill it
//

File outfile = OUTFILE;
File tmpfile = new File("${OUTFILE.getAbsolutePath()}.pending");

PrintWriter pw = new PrintWriter(tmpfile);

BufferedReader br = null;

try {
  //
  // Read /proc/vmstat
  //

  br = new BufferedReader(new FileReader("/proc/vmstat"));
  
  labels = [:];
  labels.putAll(commonLabels);
 
  while(true) {        
    String line = br.readLine();

    if (null == line) {
      break;
    }
   
    // Split lines on whitespace
    String[] tokens = line.split("\\s+");
    
    name = tokens[0];
    value = Long.valueOf(tokens[1]);

    if (null != FILTER_NAME) {
      if (!FILTER_NAME.contains(name)) {
        continue;
      }
    }

    storeMetric(pw, now, "linux.proc.vmstat.${name}", labels, value);
  }
} catch (Exception e) {
  if (SHOW_ERRORS) { e.printStackTrace(System.err); }
} finally {
  try { if (null != br) br.close(); } catch (IOException ioe) {}
  try { if (null != pw) pw.close(); } catch (IOException ioe) {}
}

//
// Move file to final location
//

tmpfile.renameTo(outfile);
