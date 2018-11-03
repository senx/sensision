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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.bio.SocketConnector;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.nio.SelectChannelConnector;
import org.eclipse.jetty.util.thread.QueuedThreadPool;

public class SensisionMetricsServer extends Thread {
  
  /**
   * Port on which the MetricsServer actually listens.
   */
  private int port;
  
  public static final String SENSISION_SERVER_CONNECTOR = "sensision.server.connector";

  public static final String SENSISION_SERVER_BINDALL = "sensision.server.bindall";
  
  public static final String SENSISION_SERVER_LASTEVENT_PARAM = "lastevent";
  public static final String SENSISION_SERVER_EVENTS_PARAM = "events";
  public static final String SENSISION_SERVER_PEEK_PARAM = "peek";
  
  private static final class MetricsHandler extends AbstractHandler {
   
    private final SensisionMetricsServer server;
    
    public MetricsHandler(SensisionMetricsServer server) {
      this.server = server;
    }
    
    @Override
    public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {        
      if ("/metrics".equals(target)) {
        
        String valuets = request.getParameter("valuets");
        
        response.setContentType("text/plain;charset=utf-8");
        if (null != System.getProperty(Sensision.SENSISION_HTTP_NOKEEPALIVE)) {
          response.setHeader("Connection", "close");
        }
        response.setHeader(Sensision.HTTP_HEADER_UUID, Sensision.getUUID());
        response.setHeader(Sensision.HTTP_HEADER_TIMESTAMP, Long.toHexString(Long.MAX_VALUE - Sensision.getStartTime()));
        response.setStatus(HttpServletResponse.SC_OK);
        baseRequest.setHandled(true);
        
        PrintWriter out = response.getWriter();
        
        server.dumpMetrics(out, null != valuets);
      } else if ("/events".equals(target)) {
        
        boolean onDisk = Sensision.onDisk();
        
        boolean peek = null != request.getParameter(SENSISION_SERVER_PEEK_PARAM);
        
        if (!onDisk || (onDisk && peek)) {
          long lastevent = -1L;
          int nevents = Integer.MAX_VALUE;
          
          if (null != request.getParameter(SENSISION_SERVER_LASTEVENT_PARAM)) {
            lastevent = Long.parseLong(request.getParameter(SENSISION_SERVER_LASTEVENT_PARAM));
          }

          if (null != request.getParameter(SENSISION_SERVER_EVENTS_PARAM)) {
            nevents = Integer.parseInt(request.getParameter(SENSISION_SERVER_EVENTS_PARAM));
          }

          response.setContentType("text/plain;charset=utf-8");
          if (null != System.getProperty(Sensision.SENSISION_HTTP_NOKEEPALIVE)) {
            response.setHeader("Connection", "close");
          }
          response.setHeader(Sensision.HTTP_HEADER_UUID, Sensision.getUUID());
          response.setHeader(Sensision.HTTP_HEADER_TIMESTAMP, Long.toHexString(Long.MAX_VALUE - Sensision.getStartTime()));
          response.setStatus(HttpServletResponse.SC_OK);
          baseRequest.setHandled(true);
          
          server.dumpEvents(response, lastevent, nevents);          
        } else {
          //
          // We're not having a peek, flush the events File
          //
          baseRequest.setHandled(true);
          Sensision.flushEvents();
        }
        
      } else {
        baseRequest.setHandled(false);
        return;
      }
    }        
  }
  
  public SensisionMetricsServer() {
    
    if (Sensision.disable) {
      return;
    }
    
    try {
      this.port = Integer.valueOf(System.getProperty(Sensision.SENSISION_SERVER_PORT, "0"));
    } catch (NumberFormatException nfe) {
      throw new RuntimeException("Unable to parse server port " + System.getProperty(Sensision.SENSISION_SERVER_PORT));
    }
    
    this.setDaemon(true);
    this.setName("[Sensision MetricsServer]");
    this.start();
  }
  
  @Override
  public void run() {
    
    Server server = new Server();

    QueuedThreadPool pool = new QueuedThreadPool();
    pool.setDaemon(true);    

    server.setThreadPool(pool);
    server.setStopAtShutdown(true);
    
    String conn = System.getProperty(SENSISION_SERVER_CONNECTOR, "nio");
    
    boolean bindall = "true".equals(System.getProperty(SENSISION_SERVER_BINDALL));
    
    if ("bio".equals(conn)) {
      SocketConnector connector = new SocketConnector();
      if (bindall) {
        connector.setHost(null);        
      } else {
        connector.setHost("127.0.0.1");
      }
      connector.setPort(this.port);
      connector.setAcceptors(1);
      server.setConnectors(new Connector[] { connector });
    } else {
      SelectChannelConnector connector = new SelectChannelConnector();
      if (bindall) {
        connector.setHost(null);        
      } else {
        connector.setHost("127.0.0.1");
      }      
      connector.setPort(this.port);
      connector.setAcceptors(1);
      server.setConnectors(new Connector[] { connector });
    }
    
    server.setHandler(new MetricsHandler(this));
    try {
      server.start();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    
    //
    // Extract port so we can register this server
    //
    
    //this.port = ((ServerSocketChannel) server.getConnectors()[0].getTransport()).socket().getLocalPort();      
    this.port = server.getConnectors()[0].getLocalPort();
    
    this.setName("[Sensision MetricsServer (" + this.port + ")]");

    //
    // Loop on registration until server is stopping.
    // This way we get a chance to recreate the target file if it was deleted.
    // Re-register every minute.
    //
    
    while (!server.isStopping()) {
      try {
        register();
      } catch (Throwable t) {        
      }
      try {
        Thread.sleep(60 * 1000L);
      } catch (InterruptedException ie) {        
      }
    }
    
    try {
      server.join();
    } catch (InterruptedException ie) {
      throw new RuntimeException(ie);
    }
  }
  
  /**
   * Register the server under SENSISION_HOME
   */
  private void register() {
    File targetDir = Sensision.getTargetsDir();
    
    // Build the filename for the registration as a list of dot separated tokens
    // First token is the reversed timestamp of registration time
    String filename = Long.toHexString(Long.MAX_VALUE - Sensision.getStartTime());
    // Second token is the Sensision polling hint
    filename = filename + "." + System.getProperty(Sensision.SENSISION_POLLING_HINT, Sensision.DEFAULT_SENSISION_POLLING_HINT);   
    // Third token is the Sensision uuid
    filename = filename + ".";
    filename = filename + Sensision.getUUID();
    // Fourth token is the MetricsServer port
    filename = filename + ".";
    filename = filename + this.port;
    // Add a custom string if it exists
    if (null != Sensision.getInstance()) {
      filename = filename + "." + Sensision.getInstance();
    }
    // Next is a suffix
    filename = filename + Sensision.SENSISION_TARGETS_SUFFIX;
    
    File reg = new File(targetDir, filename);
    
    // Force file to be deleted on exit, this way we'll only
    // have stale files for apps which terminated abnormally
    reg.deleteOnExit();
    
    //
    // Do nothing if file already exists
    //
    
    if (reg.exists()) {
      return;
    }
    
    try {
      // Create file
      Writer writer = new  FileWriter(reg);
      writer.append("");
      writer.close();                
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }
  
  public void dumpMetrics(PrintWriter out, boolean useValueTimestamp) throws IOException {
    Sensision.dump(out, useValueTimestamp);          
  }
  
  /**
   * We do not synchronize this method with the 'event' one in Sensision so as to not
   * slow down event storage when dumping events.
   */
  public void dumpEvents(HttpServletResponse response, long lastevent, int n) throws IOException {
    //
    // Determine start offset to display
    //
        
    List<String> events = Sensision.getEvents();
        
    long curevent = Sensision.getCurrentEvent();
    
    if (0 == events.size() || -1 == curevent || n <= 0) {
      return;
    }
    
    // Shift lastevent to be closer to the current event if
    // it has already been overwritten
    if (lastevent >= 0L) {
      lastevent = Math.max(Math.max(lastevent, curevent - events.size()), -1L);
    } else {
      lastevent = Math.max(curevent - events.size(), -1L);
    }
    
    // Adjust n
    if (n > events.size()) {
      n = events.size();
    }
    
    response.setHeader(Sensision.HTTP_HEADER_LASTEVENT, Long.toString(Math.min(curevent, lastevent + events.size())));

    PrintWriter out = response.getWriter();
    
    while(n > 0 && lastevent < Sensision.getCurrentEvent()) {
      // Increment the event number we will display next
      lastevent++;
      
      // Adjust lastevent to take into consideration the possible arrival of new events
      lastevent = Math.max(Sensision.getCurrentEvent() - events.size() + 1, lastevent);
      
      // Account for cycle
      int idx = (int) (lastevent % events.size());
      out.println(events.get(idx));
      n--;
    }
  }
}
