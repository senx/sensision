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
 * Produce metrics for mounted disk volumes on a Linux box
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
  // FILTER - If FILTER is not null, only mount point that matches (starts with) these names will be retained
  // FILTER_NAME = ['/dev/sd','/dev/md'];
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

  File OUTFILE = getMetricsFile('linux.proc.mounts');

  //
  // Open the file with a '.pending' suffix so it does not get picked up while we fill it
  //

  File outfile = OUTFILE;
  tmpfile = new File("${OUTFILE.getAbsolutePath()}.pending");

  pw = new PrintWriter(tmpfile);

  //
  // Open /proc/mounts (which is a link to /proc/self/mounts after 2.4.19)
  //

  br = new BufferedReader(new FileReader("/proc/mounts"));

  while(true) {        
    String line = br.readLine();

    if (null == line) {
      break;
    }

    // Split lines on whitespace
    String[] tokens = line.trim().split("\\s+");
    
    String device = tokens[0];
    String mountpoint = tokens[1];
    String fstype = tokens[2];
    String mount_options = tokens[3];

    labels = [:]
    labels['device'] = device;
    labels['mountpoint'] = mountpoint;
    labels.putAll(commonLabels);

    // Is the current device match selection (FILTER) - true by default
    boolean selected = true;

    if (null != FILTER_NAME) {
      selected = FILTER_NAME.find { 
        if (device.startsWith(it)) { 
          return true;
        }
        return false;
      }
    }

    if (selected) {

      storeMetric(pw, now, 'linux.proc.mount.fs', labels, fstype);
      storeMetric(pw, now, 'linux.proc.mount.options', labels, mount_options);

      //
      // Determine the capacity and free space of some mounted volumes.
      // This is done on a per filesystem type basis.
      // ext{2,3,4}, xfs, jfs, tmpfs, ramfs ar OK
      // NFS is evil as calls to determine capacity or free space might hang
      //
      
      if (fstype in [ "ext2", "ext3", "ext4", "jfs", "xfs", "tmpfs", "ramfs" ]) {
        File dir = new File(mountpoint);
        capacity = dir.getTotalSpace();
        free = dir.getFreeSpace();

        storeMetric(pw, now, 'linux.df.bytes.free', labels, free);
        storeMetric(pw, now, 'linux.df.bytes.capacity', labels, capacity);
      }
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
