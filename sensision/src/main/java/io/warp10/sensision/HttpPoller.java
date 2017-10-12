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

package io.warp10.sensision;

import java.io.File;
import java.io.FileFilter;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;

/**
 * Periodically scan a directory for a list of targets.
 * 
 * Periodically poll the said targets and store the retrieved
 * metrics into the 'queued' directory.
 */
public class HttpPoller extends Thread {

  private static final String DEFAULT_HTTPPOLLER_SLEEP = "10000";
  private static final String DEFAULT_HTTPPOLLER_SCANPERIOD = "60000";
  private static final String DEFAULT_HTTPPOLLER_FORCEDHINT = "0";
  
  /**
   * How long to sleep between two pollables scan
   */
  private final long sleep;

  /**
   * How often to scan the 'targets' directory.
   */
  private final long scanPeriod;
  
  /**
   * Forced value of polling hint, if 0, use the hint given by the target
   */
  private final long forcedhint;
  
  private final ExecutorService executor;
  
  /**
   * Map of ports to registration file
   */
  private Map<Integer, File> ports;

  /**
   * Map of ports to last event
   */
  private Map<Integer, Long> lastevents = new HashMap<Integer, Long>();
  
  /**
   * Polling periodicity of each port
   */
  private Map<Integer, Long> periodicities;

  /**
   * Should we display which urls Sensision Service should poll
   */
  private final boolean urlDebug;
  
  public HttpPoller(Properties config) {
        
    this.sleep = Long.valueOf(config.getProperty(Sensision.SENSISION_HTTPPOLLER_SLEEP, DEFAULT_HTTPPOLLER_SLEEP));
    this.scanPeriod = Long.valueOf(config.getProperty(Sensision.SENSISION_HTTPPOLLER_SCANPERIOD, DEFAULT_HTTPPOLLER_SCANPERIOD));
    this.forcedhint = Long.valueOf(config.getProperty(Sensision.SENSISION_HTTPPOLLER_FORCEDHINT, DEFAULT_HTTPPOLLER_FORCEDHINT));
    this.executor = new ThreadPoolExecutor(2, 16, 2 * sleep, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(1024));
    this.urlDebug = Boolean.valueOf(config.getProperty(Sensision.SENSISION_URLDEBUG, Sensision.DEFAULT_SENSISION_URLDEBUG));

    this.setDaemon(true);
    this.setName("[Sensision HttpPoller]");
    this.start();
  }
  
  @Override
  public void run() {
      
    //
    // Map of port to next scheduled run
    //
    
    final Map<Integer,Long> nextpoll = new HashMap<Integer,Long>();
    
    //
    // Priority queue of pollables ports
    //
    
    PriorityQueue<Integer> pollables = new PriorityQueue<Integer>(1, new Comparator<Integer>() {
      @Override
      public int compare(Integer o1, Integer o2) {
        long nextrun1 = null != nextpoll.get(o1) ? nextpoll.get(o1) : Long.MAX_VALUE;
        long nextrun2 = null != nextpoll.get(o2) ? nextpoll.get(o2) : Long.MAX_VALUE;
        if (nextrun1 < nextrun2) {
          return -1;
        } else if (nextrun1 > nextrun2) {
          return 1;
        } else {
          return 0;
        }
      }
    });

    long lastscan = 0L;
    
    while(true) {
      long now = System.currentTimeMillis();

      if (System.currentTimeMillis() - lastscan > this.scanPeriod) {
        getTargets();
        lastscan = System.currentTimeMillis();
      }

      long sleepuntil = lastscan + this.scanPeriod;

      //
      // Find port to poll next, class them 
      //
    
      pollables.clear();

      Set<Integer> knownPorts = new HashSet<Integer>();
      
      synchronized(ports) {
        knownPorts.addAll(ports.keySet());
      }
      
      for (int port: knownPorts) {

        //
        // If port has no scheduled poll yet or should be polled immediately, select it
        //
        
        Long schedule = nextpoll.remove(port);
        
        if (null == schedule || schedule < now) {
          pollables.add(port);
        } else if (null != schedule) {
          nextpoll.put(port, schedule);
          // Update sleepuntil so we don't sleep past the next scheduled poll
          if (schedule < sleepuntil) {
            sleepuntil = schedule;
          }
        }
      }

      while(pollables.size() > 0) {

        final int port = pollables.poll();
        // Set next run now. This will be overwritten at the end of execution
        // of the script (from inside the Runnable)
        nextpoll.put(port, System.currentTimeMillis() + periodicities.get(port));

        final int tcpport = port;
        
        this.executor.submit(new Callable<Boolean>() {
          @Override
          public Boolean call() throws Exception {
            //
            // Call endpoint
            //
            
            URL url = new URL("http://127.0.0.1:" + tcpport + "/metrics");

            if (urlDebug) {
              System.out.println("Get metrics from " + url);
            }

            HttpURLConnection conn = null;
            
            try {
              conn = (HttpURLConnection) url.openConnection();
            } catch (IOException ioe) {
              return false;
            }
            
            File outfile = null;
            boolean hasContent = false;
            String newname = null;
            
            try {
              InputStream is = conn.getInputStream();

              if (200 != conn.getResponseCode()) {
                is.close();
                return false;
              }            
              
              String uuid = conn.getHeaderField(Sensision.HTTP_HEADER_UUID);
              String ts = conn.getHeaderField(Sensision.HTTP_HEADER_TIMESTAMP);
              
              //BufferedReader br = new BufferedReader(new InputStreamReader(is));
              
              //
              // Create outfile
              //
              
              StringBuilder sb = new StringBuilder();
              long now = System.currentTimeMillis();
              sb.append(Long.toHexString(Long.MAX_VALUE - now));
              sb.append(".");
              sb.append(ts);
              sb.append(".");
              sb.append(uuid);
              sb.append(Sensision.SENSISION_METRICS_SUFFIX);
              
              newname = sb.toString();
              
              outfile = new File(Sensision.getQueueDir(), sb.toString() + ".new");
              OutputStream os = new FileOutputStream(outfile);
              byte[] buf = new byte[8192];
              
              while(true) {
                int len = is.read(buf);
                
                if (len < 0) {
                  break;
                }
                
                os.write(buf, 0, len);
                hasContent = true;
              }              
              
              is.close();
              os.close();

              //
              // Retrieve events
              //
              
              if (lastevents.containsKey(tcpport)) {
                url = new URL("http://127.0.0.1:" + tcpport + "/events?" + SensisionMetricsServer.SENSISION_SERVER_LASTEVENT_PARAM + "=" + lastevents.get(tcpport));
              } else {
                url = new URL("http://127.0.0.1:" + tcpport + "/events");
              }
              
              conn = null;
              
              try {
                conn = (HttpURLConnection) url.openConnection();
              } catch (IOException ioe) {
                return false;
              }
              
              is = conn.getInputStream();

              if (200 != conn.getResponseCode()) {
                is.close();
                return false;
              }            
              
              String lastevent = conn.getHeaderField(Sensision.HTTP_HEADER_LASTEVENT);
                            
              os = new FileOutputStream(outfile, true);
              
              while(true) {
                int len = is.read(buf);
                
                if (len < 0) {
                  break;
                }
                
                os.write(buf, 0, len);
                hasContent = true;
              }              
              
              is.close();
              os.close();

              if (null != lastevent) {
                try {
                  lastevents.put(tcpport, Long.parseLong(lastevent));
                } catch (NumberFormatException nfe) {                  
                }
              }
            } catch (IOException ioe) {
              //
              // If we encountered a ConnectException, remove the target file as it is surely stale.
              // If for an unknown reason this was not the case, the target file would be recreated
              // in a short while anyway.
              //
              if (ioe instanceof ConnectException) {
                synchronized(ports) {
                  ports.get(tcpport).delete();
                  ports.remove(tcpport);
                }
                periodicities.remove(tcpport);
              }
            } finally {
              conn.disconnect();
              // Update next scheduled poll
              if (periodicities.containsKey(port)) {
                nextpoll.put(port, System.currentTimeMillis() + periodicities.get(port));
              }
              
              if (hasContent && null != outfile) {
                // Atomically rename outfile to remove the ".new" suffix
                outfile.renameTo(new File(Sensision.getQueueDir(), newname));
              } else if (null != outfile) { 
                outfile.delete();
              }
            }
                        
            return true;
          }
        });
      }
      
      try {
        long sleeptime = sleepuntil - System.currentTimeMillis();
        if (sleeptime > 0) {
          Thread.sleep(sleeptime);
        }
      } catch (InterruptedException ie) {        
      }
    }
  }
  
  private void getTargets() {
    final File targetsDir = Sensision.getTargetsDir();

    Map<Integer, File> newports = new HashMap<Integer, File>();
    Map<Integer, Long> newperiodicities = new HashMap<Integer, Long>();

    File[] targets = targetsDir.listFiles(new FileFilter() {
      @Override
      public boolean accept(File file) {

        if (!file.isFile()) {
          return false;
        }
        if (file.getName().endsWith(Sensision.SENSISION_TARGETS_SUFFIX)) {
          return true;
        }
        return false;
      }
    });

    if (null != targets) {
      //
      // Sort the file names, they will be in reverse chronological order
      //

      Arrays.sort(targets);

      for (File target : targets) {
        Matcher m = Sensision.getTargetPattern().matcher(target.getName());

        if (m.matches()) {
          //
          // Extract periodicity hint
          //
          long periodicity = Long.valueOf(m.group(2));

          //
          // Extract port
          //

          int port = Integer.valueOf(m.group(4));

          //
          // Port is already known, delete the target file, it must be stale
          //

          if (newports.containsKey(port)) {
            target.delete();
          }

          newports.put(port, target);

          newperiodicities
              .put(port, 0 == forcedhint ? periodicity : forcedhint);
        }
      }
    }

    this.ports = newports;
    this.periodicities = newperiodicities;
    
    //
    // Remove orphaned lastevents
    //
    
    Set<Integer> orphaned = new HashSet<Integer>();
    
    for (int port: this.lastevents.keySet()) {
      if (!this.ports.containsKey(port)) {
        orphaned.add(port);
      }
    }
    
    for (int port: orphaned) {
      this.lastevents.remove(port);
    }
  }
}
