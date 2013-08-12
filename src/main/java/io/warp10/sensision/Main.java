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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

public class Main {
  public static void main(String[] args) throws Exception {
    
    if (1 != args.length && null == System.getProperty(Sensision.SENSISION_CONFIG_FILE)) {
      System.err.println("Usage: Main CONFIG_FILE or set '" + Sensision.SENSISION_CONFIG_FILE + "'");
    }
    
    Properties props;
    
    //
    // This leads to reading config file twice if relying on the system property but it's no big deal...
    //
    
    if (null == System.getProperty(Sensision.SENSISION_CONFIG_FILE)) {
      props = Sensision.readConfigFile(args[0]);      
    } else {
      props = Sensision.readConfigFile(System.getProperty(Sensision.SENSISION_CONFIG_FILE));
    }
    
    //
    // Start various subsystems if they were requested
    //
    
    // Queue Manager
    
    QueueManager qm = new QueueManager(props);
    
    //
    // Extract list of queues to forward
    //
    
    String queues = props.getProperty(QueueForwarder.QF_QUEUES);
    
    if (null != queues) {
      String[] q = queues.split(",");
      
      for (String queue: q) {
        QueueForwarder qf = new QueueForwarder(queue.trim(), props);
      }
    } else {
      System.out.println("WARNING, no QueueForwarder was defined, queued metrics will not be forwarded.");
    }
        
    // Pollers
    
    String[] tokens = props.getProperty(Sensision.SENSISION_POLLERS, "").split(",");
    
    Set<Object> pollers = new HashSet<Object>();
    
    for (String token: tokens) {
      if ("http".equals(token)) {
        pollers.add(new HttpPoller(props));
      } else if ("file".equals(token)) {
        pollers.add(new FilePoller(props));
      } else if ("proxy".equals(token)) {
        pollers.add(new ProxyPoller(props));
      }
    }
    
    // Script runner
    
    ScriptRunner runner;
    
    if ("true".equals(props.getProperty(Sensision.SENSISION_SCRIPTRUNNER, "false"))) {
      runner = new ScriptRunner(props);
    }
    
    //
    // Loop forever...
    //
    
    while(true) {
      try {
        Thread.sleep(Long.MAX_VALUE);
      } catch (InterruptedException ie) {        
      }
    }
  }
}
