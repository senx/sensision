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
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.DirectoryStream.Filter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;

/**
 * Periodically scans the 'metrics' directory of SENSISION_HOME and
 * move the found files into the 'queued' directory.
 */
public class FilePoller extends Thread {
  
  /**
   * Period of directory scans, in ms.
   */
  public static final String DEFAULT_POLLING_PERIOD = "15000";
  

  /**
   * Delay between directory scans
   */
  private final long period;
  
  public FilePoller(Properties config) {
    this.period = Long.valueOf(config.getProperty(Sensision.SENSISION_POLLING_PERIOD, DEFAULT_POLLING_PERIOD));
    this.setDaemon(true);
    this.setName("[Sensision FilePoller]");
    this.start();
  }
  
  @Override
  public void run() {
    
    final Path metricsHome = Sensision.getMetricsDir().toPath();
    File queueDir = Sensision.getQueueDir();
    
    //
    // Filter which only retains metrics files
    //
    
    Filter<Path> filter = new Filter<Path>() {      
      @Override
      public boolean accept(Path entry) throws IOException {
        if (!metricsHome.equals(entry.getParent())) {
          return false;
        }
        if (!entry.getName(entry.getNameCount() - 1).toString().endsWith(Sensision.SENSISION_METRICS_SUFFIX)) {
          return false;
        }
        return true;
      }
    };
    while(true) {
      
      DirectoryStream<Path> files = null;
      
      try {
        //
        // List files in SENSISION_HOME/METRICS_SUBDIR
        //
             
        files = Files.newDirectoryStream(metricsHome, filter);
        
        //
        // Move files to 'queue' directory, prepending the current reversed timestamp
        //
        
        long now = System.currentTimeMillis();
        
        for (Path path: files) {
          File file = path.toFile();
          file.renameTo(new File(queueDir, Long.toHexString(Long.MAX_VALUE - now) + "-" + file.getName()));
        }                
        
      } catch (IOException ioe) {
        
      } finally {
        if (null != files) {
          try { files.close(); } catch (IOException ioe) {}
        }
      }
      
      //
      // Sleep for a while...
      //
      
      try {
        Thread.sleep(this.period);
      } catch (InterruptedException ie) {        
      }
      
    }
  }
}
