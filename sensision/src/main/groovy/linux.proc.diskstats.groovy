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
 * Produce metrics for disk statistics on a Linux box
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
  // FILTER - If FILTER is not null, only device that matches (starts with) these names will be retained
  // FILTER_NAME = ['sd','md'];
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

  File OUTFILE = getMetricsFile('linux.proc.diskstats');

  //
  // Open the file with a '.pending' suffix so it does not get picked up while we fill it
  //

  File outfile = OUTFILE;
  tmpfile = new File("${OUTFILE.getAbsolutePath()}.pending");

  pw = new PrintWriter(tmpfile);
  
  //
  // Our data source is /proc/diskstats
  //

  br = new BufferedReader(new FileReader("/proc/diskstats"));

  while(true) {        
    String line = br.readLine();

    if (null == line) {
      break;
    }

    // Split diskstats lines on whitespaces
    String[] tokens = line.trim().split("\\s+");
    
    //
    // Extract all fields, see https://github.com/torvalds/linux/blob/master/Documentation/iostats.txt
    // We support 2.4 (15 fields) and 2.6 (14 fields)
    //

    //
    // Device major/minor
    //

    String major = tokens[0].trim();
    String minor = tokens[1].trim();

    String name = null;

    int idx;

    // Check file format, see source code at https://github.com/torvalds/linux/blob/4ff2473bdb4cf2bb7d208ccf4418d3d7e6b1652c/block/genhd.c#L1161
    if (15 == tokens.length) {
      // Linux 2.4

      name = tokens[3];
      idx = 4;
    } else if (14 == tokens.length || 18 == tokens.length || 20 == tokens.length) {
      // Linux 2.6+ / 4.18+ / 5.5+
     
      name = tokens[2];
      idx = 3;
    } else {
      continue // Skip line, because format is unknown
    }

    // Is the current device name match selection (FILTER) - true by default
    boolean selected = true;

    if (null != FILTER_NAME) {
      selected = FILTER_NAME.find { 
        if (name.startsWith(it)) { 
          return true;
        }
        return false;
      }
    }

    if (selected) {
      long reads_completed = Long.valueOf(tokens[idx++]);
      long reads_merged = Long.valueOf(tokens[idx++]);
      long sectors_read = Long.valueOf(tokens[idx++]);
      long ms_reading = Long.valueOf(tokens[idx++]);
      long writes_completed = Long.valueOf(tokens[idx++]);
      long writes_merged = Long.valueOf(tokens[idx++]);
      long sectors_written = Long.valueOf(tokens[idx++]);
      long ms_writing = Long.valueOf(tokens[idx++]);
      long io_in_progress = Long.valueOf(tokens[idx++]);
      long ms_io = Long.valueOf(tokens[idx++]);
      long weighted_ms_io = Long.valueOf(tokens[idx++]);

      labels = [:];
      labels.putAll(commonLabels);
      labels['major'] = major;
      labels['minor'] = minor;
      labels['device'] = name;

      storeMetric(pw, now, 'linux.proc.diskstats.reads.completed', labels, reads_completed);
      storeMetric(pw, now, 'linux.proc.diskstats.reads.merged', labels, reads_merged);
      storeMetric(pw, now, 'linux.proc.diskstats.reads.sectors', labels, sectors_read);
      storeMetric(pw, now, 'linux.proc.diskstats.reads.ms', labels, ms_reading);

      storeMetric(pw, now, 'linux.proc.diskstats.writes.completed', labels, writes_completed);
      storeMetric(pw, now, 'linux.proc.diskstats.writes.merged', labels, writes_merged);
      storeMetric(pw, now, 'linux.proc.diskstats.writes.sectors', labels, sectors_written);
      storeMetric(pw, now, 'linux.proc.diskstats.writes.ms', labels, ms_writing);

      storeMetric(pw, now, 'linux.proc.diskstats.io.inprogress', labels, io_in_progress);

      storeMetric(pw, now, 'linux.proc.diskstats.io.ms', labels, ms_io);
      storeMetric(pw, now, 'linux.proc.diskstats.io.ms.weighted', labels, weighted_ms_io);
    }
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
