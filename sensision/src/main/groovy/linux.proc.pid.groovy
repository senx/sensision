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
 * Produce metrics about processes on a Linux box
 * Warning: this script depends on 'procDump' tool if SENSISIONID_ONLY is true
 */

import java.io.PrintWriter;
import static io.warp10.sensision.Utils.*;

BufferedReader br = null;
PrintWriter pw = null;

try {

  populateSymbolTable(this);

  SHOW_ERRORS = false;

  //
  // Verbose mode: add some labels (session, tty..)
  // false by default
  //
  VERBOSE = false;
   
  //
  // Take into account only processes that provide a custom pid (See SENSISIONID_KEY) 
  // false by default
  //

  SENSISIONID_ONLY = false;

  //
  // The procDump binary available in the github repository is x86 64bits compliant. For the other platforms you have to recompile it.
  // For more information See https://github.com/cityzendata/sensision/tree/master/procDump
  //
  PROCDUMP_COMMAND = "/opt/sensision/bin/procDump";

  //
  // Name of the environment variable to override the default pid
  // You have to export this environment variable when process is starting
  // Example: export SENSISIONID=warp10
  //

  SENSISIONID_KEY = "SENSISIONID";

  //
  // FILTER - If FILTER is not null, only process name that match (exact match) this FILTER will be retained
  // FILTER_NAME = ['java','XXX'];
  // If SENSISIONID_ONLY = true, only processes that provide a custom pid (SENSISIONID_KEY) and that match this FILTER will be retained
  //
  FILTER_NAME = null;

  //
  // Set _SC_CLK_TCK to the number of ticks per second (typically 100 on a Linux post 2.6)
  // if on a post 2.6 kernel. On a pre 2.6 (< 2.6) kernel, set it to 0 so start time computation
  // is correctly done (in pre 2.6 kernels, starttime is reported in jiffies instead of
  // clock ticks)
  //

  _SC_CLK_TCK = 100;

  //
  // Minimum age of process (in ms) for its metrics to be reported
  //

  AGE_THRESHOLD = 900000;

  //
  // Quiet period after boot (in ms) during which we ignore processes (mostly system ones)
  //

  BOOT_QUIET_PERIOD = 0;

  //
  // Process groups for which no metrics are reported
  //

  EXCLUDED_PGRP = ['0','1'];

  //
  // Common labels for all metrics
  //

  Map<String,String> commonLabels = [:];

  //
  // Output file
  //

  long now = System.currentTimeMillis() * 1000L;

  File OUTFILE = getMetricsFile('linux.proc.pid');

  //
  // Open the file with a '.pending' suffix so it does not get picked up while we fill it
  //

  File outfile = OUTFILE;
  File tmpfile = new File("${OUTFILE.getAbsolutePath()}.pending");

  pw = new PrintWriter(tmpfile);

  jiffies_since_boot = 0L;
  now_nsecs = 0L;

  now = System.currentTimeMillis();

  //
  // Read /proc/timer_list to retrieve the number of jiffies since boot time
  //

  br = new BufferedReader(new FileReader("/proc/timer_list"));

  while (true) {
    String line = br.readLine();
    if (null == line) {
      break;
    }
    if (line.startsWith('now at ') && line.endsWith('nsecs')) {
      now_nsecs = Long.valueOf(line.substring(7).replaceAll(' .*',''));
    }
    if (line.startsWith('jiffies: ')) {
      jiffies_since_boot = Long.valueOf(line.substring(9));
      break;
    }
  }

  br.close();

  ms_since_boot = now_nsecs / 1000000.0D;

  //
  // Compute ms per jiffy
  //

  ms_per_jiffy = ms_since_boot / jiffies_since_boot;

  //
  // Read all /proc/XXXX/stat
  //

  def String[] pids = new File("/proc").list();

  Arrays.sort(pids)
  pids = pids.reverse();

  for (fpid in pids) {
    if (!fpid.matches("^[0-9]+\$")) {
      continue;
    }

    //
    // Read /proc/PID/stat
    //

    br = new BufferedReader(new FileReader(new File("/proc/${fpid}/stat")));
    
    now = System.currentTimeMillis();
    line = br.readLine();

    br.close();
    
    tokens = line.split("\\s+");

    pName = tokens[1].substring(1, tokens[1].length() - 1);

    if (null != FILTER_NAME) {
      if (!FILTER_NAME.contains(pName)) {
        continue;
      }
    }

    String pid = tokens[0];
    String customPid = null;

    if (SENSISIONID_ONLY) {
      //
      // Get SENSISIONID from /proc/${pid}/environ
      // Permission denied so we have to use procDump
      //

      // Do not provide /proc at the beginning to procDump
      String procFilePath = "${pid}/environ";

      String dumpCmd = PROCDUMP_COMMAND + " " + procFilePath;

      def p = dumpCmd.execute();

      String varEnvMatched = null;

      p.text.eachLine { 
        if (it.contains(SENSISIONID_KEY)) {
          varEnvMatched = it;
        }
      }

      p.waitForOrKill(4000);

      if (p.exitValue() && SHOW_ERRORS) {
        System.err.println("[ERROR] - " + ${p.getErrorStream()});
      }
      
      if (null != varEnvMatched) {
        String[] values = varEnvMatched.split("\0");

        values.each {
          if (it.startsWith(SENSISIONID_KEY)) {
            customPid = it.split("=",2)[1];

            // override default pid
            pid = customPid;
          }
        }
      }

      //
      // SENSISIONID_ONLY => If custom pid has not been set by the current process, ignore it !
      //

      if (null == customPid) {
        continue;
      }
    }

    // Compute start time in ms
    jiffies_at_start = Long.valueOf(tokens[21]);

    ms_at_start = 0;

    if (0 != _SC_CLK_TCK) {
      ms_at_start = jiffies_at_start * 1000 / _SC_CLK_TCK;
    } else {
      ms_at_start = ms_per_jiffy * jiffies_at_start;
    }

    starttime = now - ms_since_boot + ms_at_start;

    // Compute process age
    process_age = now - starttime;

    // Compute start time after boot (in ms)

    if (process_age < AGE_THRESHOLD || ms_at_start < BOOT_QUIET_PERIOD || tokens[4] in EXCLUDED_PGRP) {
      continue;
    }
    
    //
    // Build labels
    //

    labels = [:];
    labels.putAll(commonLabels)

    labels['name'] = pName;

    labels['pid'] = pid;

    if (VERBOSE) {
      labels['ppid'] = tokens[3];
      labels['pgrp'] = tokens[4];
      labels['session'] = tokens[5];
      labels['tty_nr'] = tokens[6];
    }

    now = System.currentTimeMillis() * 1000L;
    storeMetric(pw, now, 'linux.proc.pid.state', labels, tokens[2]);
    storeMetric(pw, now, 'linux.proc.pid.flags', labels, Long.valueOf(tokens[8]));
    storeMetric(pw, now, 'linux.proc.pid.minflt', labels, Long.valueOf(tokens[9]));
    storeMetric(pw, now, 'linux.proc.pid.cminflt', labels, Long.valueOf(tokens[10]));
    storeMetric(pw, now, 'linux.proc.pid.majflt', labels, Long.valueOf(tokens[11]));
    storeMetric(pw, now, 'linux.proc.pid.cmajflt', labels, Long.valueOf(tokens[12]));
    storeMetric(pw, now, 'linux.proc.pid.utime', labels, Long.valueOf(tokens[13]));
    storeMetric(pw, now, 'linux.proc.pid.stime', labels, Long.valueOf(tokens[14]));
    storeMetric(pw, now, 'linux.proc.pid.cutime', labels, Long.valueOf(tokens[15]));
    storeMetric(pw, now, 'linux.proc.pid.cstime', labels, Long.valueOf(tokens[16]));
    storeMetric(pw, now, 'linux.proc.pid.nice', labels, Long.valueOf(tokens[18]));
    storeMetric(pw, now, 'linux.proc.pid.nthreads', labels, Long.valueOf(tokens[19]));
    //storeMetric(pw, now, 'linux.proc.pid.starttime', labels, Long.valueOf(tokens[21]));
    storeMetric(pw, now, 'linux.proc.pid.starttime', labels, (long) starttime);
    storeMetric(pw, now, 'linux.proc.pid.vsize', labels, Long.valueOf(tokens[22]));
    storeMetric(pw, now, 'linux.proc.pid.rss', labels, Long.valueOf(tokens[23]));

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