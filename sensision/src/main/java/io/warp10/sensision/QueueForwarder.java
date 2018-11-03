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

package io.warp10.sensision;

import io.warp10.sensision.Sensision.Value;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.InputStreamReader;
import java.io.InputStream;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URL;
import java.nio.file.DirectoryStream;
import java.nio.file.DirectoryStream.Filter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.zip.GZIPOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.MinMaxPriorityQueue;

/**
 * This class periodically scans the 'queued' directory and takes
 * care of the top N files, sending their content to Cityzen Data
 * using HTTP(S).
 */
public class QueueForwarder extends Thread {

  private static final Logger LOGGER = LoggerFactory.getLogger(QueueForwarder.class);
  
  /**
   * Comma separated list of queues to forward
   */
  public static final String QF_QUEUES = "sensision.qf.queues";

  /**
   * Warp 10 endpoint update URL
   */
  public static final String HTTP_URL = "sensision.qf.url";
  
  /**
   * Warp 10 token to use for storing data
   */
  public static final String HTTP_TOKEN = "sensision.qf.token";
  
  /**
   * Name of header to use for providing the token
   */
  public static final String HTTP_TOKEN_HEADER = "sensision.qf.tokenheader";
    
  /**
   * Number of files to read at each scan
   */
  public static final String HTTP_TOPN = "sensision.qf.topn";
  
  /**
   * Number of milliseconds between two directory scans
   */
  public static final String HTTP_PERIOD = "sensision.qf.period";
  
  /**
   * Maximum number of files to merge when sending metrics.
   */
  public static final String HTTP_BATCHSIZE = "sensision.qf.batchsize";
  
  public static final String HTTP_PROXY_HOST = "sensision.qf.proxy.host";
  public static final String HTTP_PROXY_PORT = "sensision.qf.proxy.port";
  
  private final File queueDir;
  
  /**
   * Maximum number of files to consider at each run
   */
  private final int topn;
  
  /**
   * Maximum number of files to merge when making an HTTP call
   */
  private final int batchsize;
  
  /**
   * URL to connect to for sending data
   */
  private final URL url;
  
  /**
   * OAuth 2.0 access token for talking to Cityzen Data
   */
  private final String token;
  
  /**
   * Delay between queue dir scans
   */
  private final long period;
  
  private final DeduplicationManager deduplicationManager;

  private final Proxy proxy;
  
  private final String queue;
  
  private final String tokenHeader;
  
  public QueueForwarder(String queue, Properties properties) throws Exception {
    this.queue = queue;
    this.queueDir = Sensision.getQueueDir();
    this.topn = Integer.valueOf(properties.getProperty(HTTP_TOPN + "." + queue));
    this.batchsize = Integer.valueOf(properties.getProperty(HTTP_BATCHSIZE + "." + queue));
    this.url = new URL(properties.getProperty(HTTP_URL + "." + queue));
    this.token = properties.getProperty(HTTP_TOKEN + "." + queue);
    this.period = Long.valueOf(properties.getProperty(HTTP_PERIOD + "." + queue));
    
    this.deduplicationManager = new DeduplicationManager(queue,properties);
  
    if (null != properties.get(HTTP_PROXY_HOST + "." + queue) && null != properties.get(HTTP_PROXY_PORT + "." + queue)) {
      if ("noproxy".equals(properties.get(HTTP_PROXY_PORT + "." + queue))) {
        this.proxy = Proxy.NO_PROXY;
      } else {
        this.proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(properties.getProperty(HTTP_PROXY_HOST + "." + queue), Integer.valueOf(properties.getProperty(HTTP_PROXY_PORT + "." + queue))));
      }
    } else {
      this.proxy = null;
    }
    
    if (null != properties.getProperty(HTTP_TOKEN_HEADER + "." + queue)) {
      this.tokenHeader = properties.getProperty(HTTP_TOKEN_HEADER + "." + queue);
    } else {
      this.tokenHeader = Sensision.SENSISION_HTTP_TOKEN_HEADER_DEFAULT;
    }
    
    this.setDaemon(true);
    this.setName("[Sensision QueueForwarder (" + queue + ")]");
    this.start();
  }
  
  @Override
  public void run() {
    
    while(true) {

      DirectoryStream<Path> files = null;
      
      try {
        //
        // Retrieve a list of files to transmit
        //

        Filter<Path> filter = new Filter<Path>() {
          
          @Override
          public boolean accept(Path entry) throws IOException {
            
            if (!queueDir.equals(entry.getParent().toFile()) || !entry.getName(entry.getNameCount() - 1).toString().endsWith(queue + Sensision.SENSISION_QUEUED_SUFFIX)) {
              return false;
            }
            return true;
          }
        };

        files = Files.newDirectoryStream(this.queueDir.toPath(), filter);

        int idx = 0;
        
        Iterator<Path> iterator = files.iterator();
        
        MinMaxPriorityQueue<Path> topfiles = MinMaxPriorityQueue.maximumSize(this.topn).create();
        
        int queued = 0;
        
        while(iterator.hasNext()) {
          topfiles.add(iterator.next());
          queued++;
        }
                
        Map<String,String> labels = new HashMap<String,String>();
        labels.put(SensisionConstants.SENSISION_LABEL_QUEUE, queue);
        Sensision.set(SensisionConstants.SENSISION_CLASS_QF_QUEUED, labels, queued);

        iterator = topfiles.iterator();
        
        //
        // We should populate a list of the top N files, using a priority queue/sorted list and then process those files
        //
        
        while (null != files && idx < this.topn && iterator.hasNext()) {
          int batchsize = 0;
          
          //
          // FIXME(hbs): we should switch to Apache HttpClient which would allow us to
          // stream the metrics. Using HttpURLConnection, metrics are first buffered in memory
          // prior to being sent, thus consuming more memory, even though we compress them on
          // the fly. If we ever hit a problem this way, read this FIXME over...
          //
          // FIXED(hbs): using setChunkedStreamingMode fixes the problem
          //
          
          
          HttpURLConnection conn = null;

          long count = 0;
          
          try {
            if (null == this.proxy) {
              conn = (HttpURLConnection) this.url.openConnection();
            } else {
              conn = (HttpURLConnection) this.url.openConnection(this.proxy);
            }

            conn.setDoOutput(true);
            conn.setDoInput(true);
            conn.setRequestMethod("POST");
            conn.setRequestProperty(this.tokenHeader, this.token);
            conn.setRequestProperty("Content-Type", "application/gzip");
            conn.setChunkedStreamingMode(65536);

            conn.connect();
            
            OutputStream os = conn.getOutputStream();
            GZIPOutputStream out = new GZIPOutputStream(os);
            PrintWriter pw = new PrintWriter(out);
            
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            PrintWriter spw = new PrintWriter(baos);

            List<File> batchfiles = new ArrayList<File>();
            
            while (batchsize < this.batchsize && idx+batchsize < this.topn && iterator.hasNext()) {
              //
              // Open metrics file
              //
              
              File file = iterator.next().toFile();
              
              batchfiles.add(file);
              
              BufferedReader br = new BufferedReader(new FileReader(file));
              batchsize++;
              
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
                // Replace line with a sanitized version of it (with default labels/location/elevation) and
                // labels in lexicographic order, so deduplication can work correctly
                // 
                
                Sensision.dumpValue(spw, value, true, false);
                spw.flush();
                line = baos.toString("UTF-8");
                baos.reset();
                
                //
                // Call dedupper with metric, if dedupper returns true, skip metric
                //
                
                if (!this.deduplicationManager.isDuplicate(line)) {
                  pw.print(line);
                  pw.print("\r\n");
                  count++;
                }
              }
                          
              br.close();
            }
            
            pw.close();
            
            //InputStream is = conn.getInputStream();
            
            //
            // Update was successful, delete all batchfiles
            //
            
            if (HttpURLConnection.HTTP_OK == conn.getResponseCode()) {
              for (File file: batchfiles) {
                file.delete();
              }
            } else {
              LOGGER.error(url + " failed - error code: " + conn.getResponseCode());
              InputStream is = conn.getErrorStream();
              BufferedReader errorReader = new BufferedReader(new InputStreamReader(is));
              String line = errorReader.readLine();
              while (null != line) {
                LOGGER.error(line);
                line = errorReader.readLine();
              }
              is.close();
            }
          } catch (IOException ioe) {
            LOGGER.error("Caught IO exception while in 'run'", ioe);
            if (ioe instanceof ConnectException) {
              LOGGER.error("(ConnectException) url: " + this.url);
            }
          } finally {
            
            labels = new HashMap<String,String>();
            labels.put(SensisionConstants.SENSISION_LABEL_QUEUE, this.queue);
            
            Sensision.update(SensisionConstants.SENSISION_CLASS_QF_RUNS, labels, 1);
            Sensision.update(SensisionConstants.SENSISION_CLASS_QF_DATAPOINTS, labels, count);
            
            idx += batchsize;
            
            if (null != conn) {
              conn.disconnect();
            }
          }
        }        
      } catch (Throwable t) {
        LOGGER.error("Caught throwable while in 'run'", t);
      } finally {
        if (null != files) {
          try { files.close(); } catch (IOException ioe) {}
        }        
      }
                  
      try {
        Thread.sleep(this.period);
      } catch(InterruptedException ie) {        
      }
    }
  }
}
