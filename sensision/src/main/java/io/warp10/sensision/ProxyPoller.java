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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;
import java.util.zip.GZIPInputStream;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.bio.SocketConnector;
import org.eclipse.jetty.server.handler.AbstractHandler;

/**
 * Pseudo poller which acts as the Cityzen Data platform and queues
 * received metrics as if they had been polled.
 *
 */
public class ProxyPoller extends Thread {
  
  private static final String DEFAULT_PROXYPOLLER_PORT = "8080";
  private static final String SENSISION_PROXYPOLLER_PORT = "sensision.poller.proxy.port";

  private static final String SENSISION_PROXYPOLLER_HOST = "sensision.poller.proxy.host";
  
  private static final String DEFAULT_PROXYPOLLER_ACCEPTORS = "0";
  private static final String SENSISION_PROXYPOLLER_ACCEPTORS = "sensision.poller.proxy.acceptors";

  private static final String DEFAULT_PROXYPOLLER_SELECTORS = "0";
  private static final String SENSISION_PROXYPOLLER_SELECTORS = "sensision.poller.proxy.selectors";
  
  private static final String SENSISION_PROXYPOLLER_TOKENS = "sensision.poller.proxy.tokens";

  private final Server server;
  
  private final String uuid;
  
  private final String[] tokens;
  
  public ProxyPoller(Properties config) {
  
    //
    // Generate a UUID for this instance of ProxyPoller
    //
    
    this.uuid = UUID.randomUUID().toString();
    
    //
    // Extract port
    //
    
    int port = Integer.valueOf(config.getProperty(SENSISION_PROXYPOLLER_PORT, DEFAULT_PROXYPOLLER_PORT));
    int acceptors = Integer.valueOf(config.getProperty(SENSISION_PROXYPOLLER_ACCEPTORS, DEFAULT_PROXYPOLLER_ACCEPTORS));
    int selectors = Integer.valueOf(config.getProperty(SENSISION_PROXYPOLLER_SELECTORS, DEFAULT_PROXYPOLLER_SELECTORS));
    
    if (config.containsKey(SENSISION_PROXYPOLLER_TOKENS)) {
      this.tokens = config.getProperty(SENSISION_PROXYPOLLER_TOKENS).split(",");
      Arrays.sort(this.tokens);
    } else {
      this.tokens = null;
    }
    
    //
    // Start Jetty server
    //
    
    server = new Server();
    SocketConnector connector = new SocketConnector();
    if (config.containsKey(SENSISION_PROXYPOLLER_HOST)) {
      connector.setHost(config.getProperty(SENSISION_PROXYPOLLER_HOST));
    }
    connector.setAcceptors(acceptors);
    // (Jetty 9) ServerConnector connector = new ServerConnector(this.server, acceptors, selectors);
    connector.setPort(port);
    connector.setName("SensisionProxyPoller");
    
    server.setConnectors(new Connector[] { connector });

    final ProxyPoller self = this;
    
    server.setHandler(new AbstractHandler() {      
      @Override
      public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
        self.handle(target, baseRequest, request, response);
      }
    });
    
    this.setDaemon(true);
    this.setName("[Sensision Proxy Poller (" + port + ")]");
    this.start();
  }
  
  private void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
    
    if (!"/metrics".equals(target)) {
      baseRequest.setHandled(false);
      return;
    }
    
    baseRequest.setHandled(true);
    
    String token = request.getHeader(Sensision.SENSISION_HTTP_TOKEN_HEADER_DEFAULT);
    
    //
    // Check if the provided token is among the list of authorized ones
    //
    
    if (null != this.tokens) {
      if (null == token || Arrays.binarySearch(this.tokens, token) < 0) {
        response.setStatus(HttpServletResponse.SC_FORBIDDEN);
        return;
      }
    }
    
    String contentType = request.getHeader("Content-Type");
    
    InputStream in = request.getInputStream();
    
    if ("application/gzip".equals(contentType)) {
      in = new GZIPInputStream(in);
    }
    
    StringBuilder sb = new StringBuilder();
    long now = System.currentTimeMillis();
    sb.append(Long.toHexString(Long.MAX_VALUE - now));
    sb.append(".");
    sb.append(Long.toHexString(Long.MAX_VALUE - now));
    sb.append(".");
    sb.append(this.uuid);
    sb.append(Sensision.SENSISION_METRICS_SUFFIX);
    
    File outfile = new File(Sensision.getQueueDir(), sb.toString() + ".new");
    OutputStream out = new FileOutputStream(outfile);
    
    byte[] buf = new byte[8192];

    while(true) {
      int len = in.read(buf);
      
      if (len < 0) {
        break;
      }
      
      out.write(buf, 0, len);
    }
    
    out.close();
    
    // Rename file to strip ".new" suffix
    
    outfile.renameTo(new File(Sensision.getQueueDir(), sb.toString()));
    
    response.setStatus(HttpServletResponse.SC_OK);
  }
  
  @Override
  public void run() {
    try {
      server.start();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    
    try {
      server.join();
    } catch (InterruptedException ie) {
      throw new RuntimeException(ie);
    }
  }  
}
