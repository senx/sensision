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

import java.lang.instrument.Instrumentation;

public class SensisionJMXAgent {
  
  public static void premain(String agentArgs, Instrumentation instrumentation) {    
    SensisionJMXPoller poller = null;
    
    if ("file".equals(System.getProperty(Sensision.SENSISION_JMX_POLLER))) {
      poller = new SensisionFileJMXAgent(agentArgs, instrumentation);
    } else if ("http".equals(System.getProperty(Sensision.SENSISION_JMX_POLLER))) {      
      poller = new SensisionHttpJMXAgent(agentArgs, instrumentation);
    } else {
      throw new RuntimeException("Unsupported jmx poller '" + System.getProperty(Sensision.SENSISION_JMX_POLLER) + "'");
    }
  }

  public static void main(String[] args) {
    try {
      Thread.sleep(100000000000L);
    } catch (InterruptedException ie) {
      
    }
  }
}
