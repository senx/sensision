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

import groovy.lang.Binding;
import groovy.lang.GroovySystem;
import groovy.util.GroovyScriptEngine;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Periodically run scripts residing in subdirectories
 * of the given root.
 */
public class ScriptRunner extends Thread {
  
  private static final Logger LOGGER = LoggerFactory.getLogger(ScriptRunner.class);
  
  private static final String DEFAULT_ROOT = "/var/run/sensision/scripts";
  private static final String DEFAULT_NTHREADS = "1";
  private static final String DEFAULT_SCANPERIOD = "60000";
  
  private final ExecutorService executor;
  
  private final List<GroovyScriptEngine> engines;
  
  private final int nthreads;
  
  private final long scanperiod;
  
  private final String root;
  
  /**
   * @param root Root directory where scripts reside.
   * @param nthreads Number of threads to use for running scripts
   */
  public ScriptRunner(Properties config) throws IOException {
    
    this.root = config.getProperty(Sensision.SENSISION_SCRIPTRUNNER_ROOT, DEFAULT_ROOT);
    this.nthreads = Integer.valueOf(config.getProperty(Sensision.SENSISION_SCRIPTRUNNER_NTHREADS, DEFAULT_NTHREADS));
    this.scanperiod = Long.valueOf(config.getProperty(Sensision.SENSISION_SCRIPTRUNNER_SCANPERIOD, DEFAULT_SCANPERIOD));
    this.executor = new ThreadPoolExecutor(1, nthreads, 30000L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(1024));
    
    //
    // Allocate GroovyScriptEngines
    //
    
    this.engines = new ArrayList<GroovyScriptEngine>();
    
    for (int i = 0; i < nthreads; i++) {
      this.engines.add(new GroovyScriptEngine("file://" + root));
    }
  
    this.setDaemon(true);
    this.setName("[Sensision ScriptRunner]");
    this.start();
  }
  
  @Override
  public void run() {
    
    long lastscan = System.currentTimeMillis() - 2 * scanperiod;
    
    //
    // Periodicity of scripts
    //
    
    final Map<String,Long> scripts = new HashMap<String,Long>();
    
    //
    // Map of script path to next scheduled run
    //
    
    final Map<String,Long> nextrun = new HashMap<String,Long>();
    
    PriorityQueue<String> runnables = new PriorityQueue<String>(1, new Comparator<String>() {
      @Override
      public int compare(String o1, String o2) {
        long nextrun1 = null != nextrun.get(o1) ? nextrun.get(o1) : Long.MAX_VALUE;
        long nextrun2 = null != nextrun.get(o2) ? nextrun.get(o2) : Long.MAX_VALUE;
        if (nextrun1 < nextrun2) {
          return -1;
        } else if (nextrun1 > nextrun2) {
          return 1;
        } else {
          return 0;
        }
      }
    });
    
    while(true) {
      long now = System.currentTimeMillis();
      
      if (now - lastscan > this.scanperiod) {
        Map<String,Long> newscripts = scanRoot(this.root);
        scripts.clear();
        scripts.putAll(newscripts);
        lastscan = now;
      }

      //
      // Find script to run next
      //
    
      runnables.clear();
      
      for (String script: scripts.keySet()) {
        //
        // If script has no scheduled run yet or should run immediately, select it
        //
        
        Long schedule = nextrun.remove(script);
        
        if (null == schedule || schedule < now) {
          runnables.add(script);
        } else if (null != schedule) {
          nextrun.put(script, schedule);
        }
      }
      
      while (runnables.size() > 0) {
        final String script = runnables.poll();
        // Set next run now. This will be overwritten at the end of execution
        // of the script (from inside the Runnable)
        nextrun.put(script, System.currentTimeMillis() + scripts.get(script));
        try {
          this.executor.submit(new Runnable() {            
            @Override
            public void run() {
                
              //
              // Get a GroovyScriptEngine to run script in
              //
                
              GroovyScriptEngine engine = borrowEngine();            
              
              try {              
                Binding binding = new Binding();
                binding.setVariable("sensision_home", root);
                engine.run(script, binding);
              } catch (Throwable t) {
                LOGGER.error("Caught exception while running '" + script + "'", t);
              } finally {
                //
                // Return GroovyScriptEngine to the pool
                //
                  
                returnEngine(engine);
                  
                //
                // Schedule the next run
                //
              
                Long period = scripts.get(script);
                
                if (null != period) {
                  nextrun.put(script, System.currentTimeMillis() + period);
                }
              }
            }
          });                  
        } catch (RejectedExecutionException ree) {
          // Reschedule script immediately
          nextrun.put(script, System.currentTimeMillis());
        }
      }
      
      try {
        Thread.sleep(50L);
      } catch (InterruptedException ie) {        
      }
    }
  }
  
  
  /**
   * Scan a directory and return a map keyed by
   * script path and whose values are run periodicities in ms.
   */
  
  private Map<String,Long> scanRoot(String root) {
    
    Map<String,Long> scripts = new TreeMap<String, Long>();
    
    //
    // Retrieve directory content
    //
    
    File dir = new File(root);
    
    if (!dir.exists()) {
      return scripts;
    }
    
    String[] children = dir.list();
    
    for (String file: children) {
      File f = new File(dir, file);

      //
      // If child is a directory whose name is a valid
      // number of ms, scan its content.
      //
            
      if (!f.isDirectory() || !f.getParentFile().equals(dir)) {
        continue;
      }
      
      long period;
      
      try {
        period = Long.valueOf(f.getName()); 
      } catch (NumberFormatException nfe) {
        continue;
      }
      
      File[] subchildren = f.listFiles(new FilenameFilter() {
        
        @Override
        public boolean accept(File dir, String name) {
          return name.endsWith(".groovy");
        }
      });

      Arrays.sort(subchildren);
      
      for (File script: subchildren) {
        scripts.put(script.getAbsolutePath(), period);
      }
    }
    
    return scripts;
  }
  
  private final GroovyScriptEngine borrowEngine() {
    synchronized (this.engines) {
      return this.engines.remove(0);
    }
  }
  
  private final void returnEngine(GroovyScriptEngine engine) {
    synchronized(this.engines) {
      this.engines.add(engine);
    }
  }
}
