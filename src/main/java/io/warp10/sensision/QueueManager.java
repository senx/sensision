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

import io.warp10.sensision.Sensision.Value;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.DirectoryStream.Filter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class periodically scans the 'queued' directory and takes
 * care of the top N '.metrics' files, ventilating their content in various '.queued' files
 * depending on regular expressions.
 */
public class QueueManager extends Thread {
  
  private static final Logger LOGGER = LoggerFactory.getLogger(QueueManager.class);
  
  public static final String SENSISION_QM_DEFAULT = "sensision.qm.default";
  
  /**
   * Queue selector prefix. The rest of the parameter name is the name of the
   * target queue. The value is a selector.
   */
  public static final String SELECTOR_PREFIX = "sensision.qm.selector.";
  public static final String PENDING_SUFFIX = ".pending";
  
  /**
   * Number of files to read at each scan
   */
  public static final String HTTP_TOPN = "sensision.qm.topn";
  
  /**
   * Number of milliseconds between two directory scans
   */
  public static final String HTTP_PERIOD = "sensision.qm.period";
  
  private final File queueDir;
  
  /**
   * Maximum number of files to consider at each run
   */
  private final int topn;
  
  /**
   * Delay between queue dir scans
   */
  private final long period;
  
  private final Map<String,Pattern> queues = new HashMap<String, Pattern>();

  private final String defaultQueue;
  
  public QueueManager(Properties properties) throws Exception {
    this.queueDir = Sensision.getQueueDir();
    this.topn = Integer.valueOf(properties.getProperty(HTTP_TOPN));
    this.period = Long.valueOf(properties.getProperty(HTTP_PERIOD));
        
    //
    // Extract queue configuration
    //
    
    for (Object key: properties.keySet()) {
      if (key.toString().startsWith(SELECTOR_PREFIX)) {
        //
        // Extract queue name
        //
        
        String queue = key.toString().substring(SELECTOR_PREFIX.length());
        
        if ("".equals(queue) || !"".equals(queue.replaceAll("[a-zA-z0-9_-]", ""))) {
          throw new RuntimeException("Invalid queue name at property '" + key + "'.");
        }
        
        //
        // Extract regexp
        //

        Pattern p = Pattern.compile(properties.getProperty(key.toString()));
        
        queues.put(queue, p);
      }
    }

    this.defaultQueue = properties.getProperty(SENSISION_QM_DEFAULT);
    
    if (queues.isEmpty() && null == defaultQueue) {
      LOGGER.warn("No default queue defined, some metrics may be lost.");
    }
    
    this.setDaemon(true);
    this.setName("[Sensision QueueManager]");
    this.start();
  }
  
  @Override
  public void run() {
    
    Map<String,PrintWriter> openQueueWriters = new HashMap<String,PrintWriter>();
    List<String> openQueueFiles = new ArrayList<String>();
    
    while(true) {
      
      openQueueWriters.clear();
      
      int idx = 0;
      
      boolean error = false;
      
      DirectoryStream<Path> files = null;

      List<File> currentFiles = new ArrayList<File>();

      try {
        //
        // Retrieve a list of files to transmit
        //
        
       Filter<Path> filter = new Filter<Path>() {
          
          @Override
          public boolean accept(Path entry) throws IOException {
            
            if (!queueDir.equals(entry.getParent().toFile()) || !entry.getName(entry.getNameCount() - 1).toString().endsWith(Sensision.SENSISION_METRICS_SUFFIX)) {
              return false;
            }
            return true;
          }
        };

        files = Files.newDirectoryStream(this.queueDir.toPath(), filter);

        Iterator<Path> iterator = files.iterator();
        
        long now = System.currentTimeMillis();

        //
        // Generate a UUID for this queue manager run (now/uuid must be a unique combination)
        //
        
        String uuid = UUID.randomUUID().toString();
        
        while (null != files && idx < this.topn && iterator.hasNext()) {
          //
          // Open metrics file
          //
        
          File file = iterator.next().toFile();
          idx++;
          currentFiles.add(file);
          
          BufferedReader br = new BufferedReader(new FileReader(file));
              
          while(true) {
            String line = br.readLine();
                
            if (null == line) {
              break;
            }

            //
            // Attempt to parse metric
            //
                
            Value value = Sensision.parseMetric(line);
                
            // Skip invalid metrics
            if (null == value) {
              continue;
            }

            //
            // Loop over the queues to determine where this metric should be sent
            //
            
            boolean queued = false;
            
            for (Entry<String,Pattern> entry: queues.entrySet()) {
              Matcher m = entry.getValue().matcher(value.cls);
              
              if (!m.matches()) {
                continue;
              }
              
              //
              // Check if we have an open OutputStream for the current queue
              //
              
              PrintWriter out = openQueueWriters.get(entry.getKey());
              
              if (null == out) {
                StringBuilder sb = new StringBuilder();

                sb.append(Long.toHexString(Long.MAX_VALUE - now));
                sb.append(".");
                sb.append(uuid);
                sb.append(".");
                sb.append(entry.getKey());
                sb.append(Sensision.SENSISION_QUEUED_SUFFIX);
        
                openQueueFiles.add(sb.toString());
                
                sb.append(PENDING_SUFFIX);
                
                out = new PrintWriter(new File(Sensision.getQueueDir(), sb.toString()));
                openQueueWriters.put(entry.getKey(), out);
              }

              //
              // Output the value
              //
              
              Sensision.dumpValue(out, value, true, true);
              
              queued = true;
            }               
            
            if (!queued && null != defaultQueue) {
              PrintWriter out = openQueueWriters.get(defaultQueue);

              if (null == out) {
                StringBuilder sb = new StringBuilder();

                sb.append(Long.toHexString(Long.MAX_VALUE - now));
                sb.append(".");
                sb.append(uuid);
                sb.append(".");
                sb.append(defaultQueue);
                sb.append(Sensision.SENSISION_QUEUED_SUFFIX);
        
                openQueueFiles.add(sb.toString());
                
                sb.append(PENDING_SUFFIX);
                
                out = new PrintWriter(new File(Sensision.getQueueDir(), sb.toString()));
                openQueueWriters.put(defaultQueue, out);
              }

              //
              // Output the value
              //
              
              Sensision.dumpValue(out, value, true, true);              
            }
          }
          br.close();
        }
      } catch (IOException ioe) { 
        error = true;
        LOGGER.error("Caught IO exception in 'run'", ioe);
        if (ioe instanceof ConnectException) {
          break;
        }
      } catch (Exception e) {
        error = true;
        LOGGER.error("Caught exception in 'run'", e);
      } finally {
        
        if (null != files) {
          try { files.close(); } catch (IOException ioe) {}
        }
        
        //
        // Close open queue files
        //
        
        for (PrintWriter pw: openQueueWriters.values()) {
          pw.close();
        }

        openQueueWriters.clear();

        if (!error) {          
          for (String filename: openQueueFiles) {
            File file = new File(Sensision.getQueueDir(), filename + PENDING_SUFFIX);
            file.renameTo(new File(Sensision.getQueueDir(), filename));
          }
          
          //
          // Remove ventilated files
          //
          
          for (File file: currentFiles) {
            file.delete();
          }          
        } else {
          for (String filename: openQueueFiles) {
            File file = new File(Sensision.getQueueDir(), filename + PENDING_SUFFIX);
            file.delete();
          }
        }
        
        openQueueFiles.clear();
      }
      
      try {
        Thread.sleep(this.period);
      } catch(InterruptedException ie) {        
      }
    }
  }
}
