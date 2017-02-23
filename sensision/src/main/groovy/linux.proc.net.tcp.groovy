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
 * Produce metrics about TCP connections
 */

import java.io.PrintWriter;
import static io.warp10.sensision.Utils.*;

populateSymbolTable(this);

SHOW_ERRORS = false;

//
// FILTER - If FILTER is not null, only ports(src or dst port) that match (exact match) this FILTER will be retained
// FILTER_NAME = ['22','8080'];
//
FILTER_NAME = null;

//
// Common labels for all metrics
//

Map<String,String> commonLabels = [:];

//
// TCP Conncection states
//

TCP_CONNECTION_STATES = [ 
  '01': 'established',
  '02': 'syn_sent',
  '03': 'syn_recv',
  '04': 'fin_wait1',
  '05': 'fin_wait2',
  '06': 'time_wait',
  '07': 'close',
  '08': 'close_wait',
  '09': 'last_ack',
  '0A': 'listen',
  '0B': 'closing',
];

//
// List of well known ports whose value will be replaced with a name
//

WELL_KNOWN_PORTS = [
  22: 'ssh',
  25: 'smtp',
  80: 'http',
  443: 'https',
  2181: 'zookeeper',
  9000: 'hdfs-namenode',
  9092: 'kafka',
  50020: 'hdfs-datanode',
  60020: 'hbase-regionserver',
];

//
// List of users for which detailed metrics will be reported
//

DETAILED_USERS = [
  'hbase',
  'hdfs',
  'kafka',
  'mapred',
  'sshd',
  'zk',
];

//
// Output file
//

File OUTFILE = getMetricsFile('linux.proc.net.tcp');

//
// Open the file with a '.pending' suffix so it does not get picked up while we fill it
//

File outfile = OUTFILE;
File tmpfile = new File("${OUTFILE.getAbsolutePath()}.pending");

PrintWriter pw = new PrintWriter(tmpfile);

BufferedReader br = null;

counters = [:];

long now = System.currentTimeMillis() * 1000L;

try {

  //
  // Read user names and ids out of /etc/passed
  //

  br = new BufferedReader(new FileReader('/etc/passwd'));
 
  def usernames = [:];

  while(true) {
    String line = br.readLine();
    if (null == line) {
      break;
    }
    String name = line.replaceAll(':.*','');
    String uid = line.substring(name.length() + 1).replaceAll(':.*','');
    usernames.put(uid, name);
  }

  br.close();

  //
  // Now read infos about TCP connections and update counters accordingly
  //

  for (file in ['/proc/net/tcp','/proc/net/tcp6']) {

    try {
      br = new BufferedReader(new FileReader(file));
    } catch (IOException ioe) {
      // Skip to the next /proc/xxx file in case of error
      continue;
    }

    while(true) {
      String line = br.readLine();

      if (null == line) {
        break;
      }

      String[] tokens = line.trim().split('\\s+');

      // Skip header line
      if ('sl'.equals(tokens[0])) {
        continue;
      }

      // Determine if src or dst IPs are internal (RFC1918 or loopback or 0.0.0.0)
      // For this we check the lowest four bytes of the IP (as the bytes are reversed, this
      // works for both IPv4 and IPv6).

      int srcip = Long.valueOf(tokens[1].replaceAll('.*(........):.*','\$1'), 16);
      int dstip = Long.valueOf(tokens[2].replaceAll('.*(........):.*','\$1'), 16);

      internal = false;

      ips = [ srcip, dstip ];

      for (ip in ips) {
        if ((ip & 0xff) == 127
          || (ip & 0xff) == 10
          || (ip & 0xff) == 0
          || ((ip & 0xff) == 172 && ((dstip >> 8) & 0xff) > 16)
          || ((ip & 0xff) == 192 && ((dstip >> 8) & 0xff) == 168)) {
          internal = true;
        }
      }

      int srcport = Integer.valueOf(tokens[1].replaceAll('.*:',''), 16);
      int dstport = Integer.valueOf(tokens[2].replaceAll('.*:',''), 16);

      if (null != FILTER_NAME) {
        if (!FILTER_NAME.contains(Integer.toString(srcport)) && !FILTER_NAME.contains(Integer.toString(dstport))) {
          continue;
        }
      }

      //
      // Extract service name from well known ports list
      // Use the source port then fallback to the destination port
      //

      String service = null != WELL_KNOWN_PORTS[srcport] ? WELL_KNOWN_PORTS[srcport] : (null != WELL_KNOWN_PORTS[dstport] ? WELL_KNOWN_PORTS[dstport] : 'other');

      //
      // Extract TCP connection state name
      //

      String state = TCP_CONNECTION_STATES[tokens[3]];

      //
      // Extract username
      //

      String user = usernames[tokens[7]];

      if (!(user in DETAILED_USERS)) {
        user = 'other';
      }

      labels = [
        'state': state,
        'endpoint': (internal ? 'internal' : 'external'),
        'service': service,
        'user': user
      ];

      //
      // Update counter
      //

      counter = counters.get(labels);

      if (null == counter) {
        counters.put(labels, 1);
      } else {
        counters.put(labels, counter + 1);
      }
    }

    br.close();
  }

  services = ['other'] as HashSet;
  services.addAll(WELL_KNOWN_PORTS.values());

  users = ['other'] as HashSet;
  users.addAll(DETAILED_USERS);

  for (user in users) {
    for (service in services) {
      for (endpoint in ['internal', 'external']) {
        for (state in TCP_CONNECTION_STATES.values()) {
          labels = [
            'user': user,
            'service': service,
            'endpoint': endpoint,
            'state': state, 
          ];

          labels.putAll(commonLabels);

          counter = counters.get(labels);

          if (null == counter) {
            // Force to 0 if you want to report a value for all label combos
            //counter = 0;
          }

          if (null != counter) {
            storeMetric(pw, now, 'linux.proc.net.tcp', labels, counter);
          }
        }
      }
    }
  }
} catch (Exception e) {
  if (SHOW_ERRORS) { e.printStackTrace(System.err); }
} finally {
  try { if (null != pw) pw.close(); } catch (IOException ioe) {}
}

//
// Move file to final location
//

tmpfile.renameTo(outfile);
