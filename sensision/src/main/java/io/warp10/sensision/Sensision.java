//
//   Copyright 2018-2023  SenX S.A.S.
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
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Sensision is the main class of the Sensision metrics collection
 * system. It contains methods for setting, updating and reading
 * values.
 *
 * INFO(hbs): sticky bit is needed on 'targets/*', 'metrics', 'queued' directories
 * with owner 'sensision' and mode 1777
 */
public class Sensision {

  /**
   * UUID which uniquely identifies this Sensision class (there might be one
   * different one per class loader).
   */
  private static final String uuid = UUID.randomUUID().toString();

  /**
   * Time at which this Sensision class started to be
   */
  private static long starttime = System.currentTimeMillis();

  private static List<Iterable<Value>> providers = new ArrayList<Iterable<Value>>();

  public static final String SENSISION_DISABLE = "sensision.disable";
  public static final String SENSISION_TIME_UNIT = "sensision.timeunit";
  public static final String SENSISION_INSTANCE = "sensision.instance";

  public static final String SENSISION_QUEUEMANAGER = "sensision.queuemanager";
  public static final String SENSISION_POLLERS = "sensision.pollers";
  public static final String SENSISION_HTTP_TOKEN_HEADER_DEFAULT = "X-Warp10-Token";

  public static final String HTTP_HEADER_UUID = "X-UUID";
  public static final String HTTP_HEADER_TIMESTAMP = "X-Timestamp";
  public static final String HTTP_HEADER_LASTEVENT = "X-Sensision-LastEvent";

  public static final String DEFAULT_SENSISION_HOME = "/var/run/sensision";
  private static final String SENSISION_TARGETS_SUBDIR = "targets";
  public static final String SENSISION_TARGETS_SUFFIX = ".target";

  //public static final Pattern SENSISION_TARGET_PATTERN = Pattern.compile("^([0-9a-f]+)\\.([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\\.([0-9]{1,5}).*$");
  public static final Pattern SENSISION_TARGET_PATTERN = Pattern.compile("^([0-9a-f]+)\\.([0-9]+)\\.([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\\.([0-9]{1,5}).*$");

  /**
   * How often should the MetricDumper dump the metrics
   */
  public static final long DEFAULT_DUMP_PERIOD = 10000L;

  public static final String SENSISION_CONFIG_FILE = "sensision.config";

  private static final String SENSISION_QUEUE_SUBDIR = "queued";
  public static final String SENSISION_QUEUED_SUFFIX = ".queued";

  public static final String SENSISION_METRICS_SUBDIR = "metrics";
  public static final String SENSISION_METRICS_SUFFIX = ".metrics";

  public static final String SENSISION_HOME = "sensision.home";

  /**
   * Number of events in the circular history buffer
   */
  public static final String SENSISION_EVENTS_HISTORY = "sensision.events.history";

  /**
   * Suffix to use for the events files
   */
  public static final String SENSISION_EVENTS_SUFFIX = "sensision.events.suffix";

  /**
   * Directory where events files should be created
   */
  public static final String SENSISION_EVENTS_DIR = "sensision.events.dir";

  public static final String DEFAULT_SENSISION_POLLING_HINT = "60000";

  public static final String DEFAULT_SENSISION_URLDEBUG = "false";

  /**
   * Key to optimize String using String.intern. Should be used solely on JDK7+, otherwise
   * the PermGenSpace is consumed.
   * Should be used when lots of metrics are created with similar labels.
   */
  public static final String SENSISION_STRING_OPTIMIZE = "sensision.string.optimize";

  public static final String SENSISION_HTTPPOLLER_SLEEP = "sensision.poller.http.sleep";
  public static final String SENSISION_HTTPPOLLER_SCANPERIOD = "sensision.poller.http.scanperiod";
  public static final String SENSISION_HTTPPOLLER_FORCEDHINT = "sensision.poller.http.forcedhint";
  public static final String SENSISION_HTTPPOLLER_TIMEOUT = "sensision.poller.http.timeout";

  public static final String SENSISION_SCRIPTRUNNER = "sensision.scriptrunner";
  public static final String SENSISION_SCRIPTRUNNER_ROOT = "sensision.scriptrunner.root";
  public static final String SENSISION_SCRIPTRUNNER_NTHREADS = "sensision.scriptrunner.nthreads";
  public static final String SENSISION_SCRIPTRUNNER_SCANPERIOD = "sensision.scriptrunner.scanperiod";

  public static final String SENSISION_JMX_POLLER = "sensision.jmx.poller";
  public static final String SENSISION_POLLING_HINT = "sensision.polling.hint";
  public static final String SENSISION_POLLING_PERIOD = "sensision.polling.period";
  public static final String SENSISION_DUMP_PERIOD = "sensision.dump.period";
  public static final String SENSISION_DUMP_CURRENTTS = "sensision.dump.currentts";
  public static final String SENSISION_DUMP_ONEXIT = "sensision.dump.onexit";
  public static final String SENSISION_CURATION_PERIOD = "sensision.curation.period";
  public static final String SENSISION_SERVER_PORT = "sensision.server.port";
  public static final String SENSISION_DEFAULT_LABELS = "sensision.default.labels";
  public static final String SENSISION_DEFAULT_LOCATION = "sensision.default.location";
  public static final String SENSISION_DEFAULT_ELEVATION = "sensision.default.elevation";
  public static final String SENSISION_HTTP_NOKEEPALIVE = "sensision.http.nokeepalive";

  public static final String SENSISION_URLDEBUG = "sensision.urldebug";

  static Map<String,String> defaultLabels = new HashMap<String,String>();

  static Double defaultLatitude = null;
  static Double defaultLongitude = null;
  static Long defaultElevation = null;

  /**
   * Handy constants for empty labels
   */
  public static Map<String,String> EMPTY_LABELS = Collections.unmodifiableMap(new HashMap<String, String>());

  static boolean disable = false;

  static boolean useStringIntern = false;

  /**
   * Number of time units per 'ms', defaults to 1000L.
   */
  public static long TIME_UNITS_PER_MS = 1000L;

  static {
    //
    // Read config file if set. This will have the side effect of setting System properties
    //

    if (null != System.getProperty(SENSISION_CONFIG_FILE)) {
      try {
        readConfigFile(System.getProperty(SENSISION_CONFIG_FILE));
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }

    instanceName = System.getProperty(SENSISION_INSTANCE);

    // Extract time unit
    if (null != System.getProperty(SENSISION_TIME_UNIT)) {
      if ("us".equals(System.getProperty(SENSISION_TIME_UNIT))) {
        TIME_UNITS_PER_MS = 1000L;
      } else if ("ns".equals(System.getProperty(SENSISION_TIME_UNIT))) {
        TIME_UNITS_PER_MS = 1000000L;
      } else if ("ms".equals(System.getProperty(SENSISION_TIME_UNIT))) {
        TIME_UNITS_PER_MS = 1L;
      }
    }

    // Extract default labels
    if (null != System.getProperty(SENSISION_DEFAULT_LABELS)) {
      // Split on ','

      String[] tokens = System.getProperty(SENSISION_DEFAULT_LABELS).trim().split(",");

      for (String token: tokens) {
        // Split on '='
        if (!token.contains("=")) {
          throw new RuntimeException("Invalid default labels.");
        }

        String[] subtokens = token.trim().split("=");

        if (2 != subtokens.length) {
          throw new RuntimeException("Invalid default labels.");
        }

        try {
          String name = URLDecoder.decode(subtokens[0], "UTF-8");
          String value = URLDecoder.decode(subtokens[1], "UTF-8");

          if (defaultLabels.containsKey(name)) {
            throw new RuntimeException("Duplicate label '" + name + "' in default labels.");
          } else {
            defaultLabels.put(name, value);
          }
        } catch (UnsupportedEncodingException uee) {
          // Can't happen, we're using UTF-8
        }
      }
    }

    // Extract default location
    if (null != System.getProperty(SENSISION_DEFAULT_LOCATION)) {
      try {
        defaultLatitude = Double.valueOf(System.getProperty(SENSISION_DEFAULT_LOCATION).replaceAll(":.*",""));
        defaultLongitude = Double.valueOf(System.getProperty(SENSISION_DEFAULT_LOCATION).replaceAll(".*:",""));
      } catch (NumberFormatException nfe) {
        throw new RuntimeException(nfe);
      }
    }
    // Extract default elevation
    if (null != System.getProperty(SENSISION_DEFAULT_ELEVATION)) {
      try {
        defaultElevation = Long.valueOf(System.getProperty(SENSISION_DEFAULT_ELEVATION));
      } catch (NumberFormatException nfe) {
        throw new RuntimeException(nfe);
      }
    }

    if (null != System.getProperty(Sensision.SENSISION_SERVER_PORT)) {
      server = new SensisionMetricsServer();
    } else {
      server = null;
    }

    if (null != System.getProperty(Sensision.SENSISION_DUMP_PERIOD)) {
      dumper = new SensisionMetricsDumper();
    } else {
      dumper = null;
    }

    if ("true".equals(System.getProperty(Sensision.SENSISION_DISABLE))) {
      disable = true;
    }

    if ("true".equals(System.getProperty(Sensision.SENSISION_STRING_OPTIMIZE))) {
      useStringIntern = true;
    }

    events = new String[Integer.parseInt(System.getProperty(Sensision.SENSISION_EVENTS_HISTORY, "0"))];

    eventsDir = System.getProperty(Sensision.SENSISION_EVENTS_DIR);

    eventsSuffix = System.getProperty(Sensision.SENSISION_EVENTS_SUFFIX);

    //
    // Register a shutdown hook to flush the current events file
    //

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        if (null != System.getProperty(Sensision.SENSISION_DUMP_ONEXIT)) {
          SensisionMetricsDumper.flushMetrics(null);
        }
        flushEvents();
      }
    });
  }

  /**
   * Server to serve known metrics via HTTP
   */
  private static final SensisionMetricsServer server;

  /**
   * Dumper to periodically dump known metrics
   */
  private static final SensisionMetricsDumper dumper;

  /**
   * Curator to clean expired values
   */
  private static final MetricsCurator curator = new MetricsCurator();

  /**
   * Current sequence number of event
   */
  private static final AtomicLong eventSequence = new AtomicLong(-1);

  /**
   * Circular array of events
   */
  private static final String[] events;

  /**
   * Directory where events files should be created
   */
  private static final String eventsDir;

  /**
   * Current File where events are stored
   */
  private static File eventsFile = null;

  /**
   * Current Writer where events are stored
   */
  private static PrintWriter eventsWriter = null;

  private static final String eventsSuffix;

  /**
   * Name of Sensision instance
   */
  private static String instanceName = null;

  public static final class Value {

    /**
     * Possible types for a metric value
     */
    enum TYPE { LONG, DOUBLE, STRING, BOOLEAN };

    /**
     * Actual type of value
     */
    TYPE type = null;

    /**
     * Class name
     */
    String cls;

    /**
     * Labels
     */
    Map<String,String> labels;

    /**
     * Timestamp of the value.
     */
    long timestamp;

    /**
     * Optional latitude of value
     */
    Float latitude = null;

    /**
     * Optional longitude of value
     */
    Float longitude = null;

    /**
     * Optional elevation of value
     */
    Long elevation = null;

    /**
     * Actual value. Class depends on type
     */
    Object value;

    /**
     * Optional expiration
     */
    Long expire;

    protected Value() {}
    public Value(String name, Map<String,String> labels, long timestamp, Float latitude, Float longitude, Long elevation, Object value) {
      this.cls = name;
      this.labels = labels;
      this.timestamp = timestamp;
      this.latitude = latitude;
      this.longitude = longitude;
      this.elevation = elevation;

      if (value instanceof Long || value instanceof Integer || value instanceof BigInteger) {
        this.type = TYPE.LONG;
        this.value = ((Number) value).longValue();
      } else if (value instanceof Double || value instanceof Float || value instanceof BigDecimal) {
        this.type = TYPE.DOUBLE;
        this.value = ((Number) value).doubleValue();
      } else if (value instanceof Boolean) {
        this.type = TYPE.BOOLEAN;
        this.value = value;
      } else {
        this.type = TYPE.STRING;
        this.value = value.toString();
      }
    }
  };

  /**
   * Thread which will periodically scan the metrics and discard
   * the ones which have expired.
   */
  private static final class MetricsCurator extends Thread {

    private static final long DEFAULT_CURATION_PERIOD = 10000L;

    private long period;

    public MetricsCurator() {
      if (Sensision.disable) {
        return;
      }

      if (null == System.getProperty(SENSISION_CURATION_PERIOD)) {
        return;
      }

      try {
        period = Long.valueOf(System.getProperty(SENSISION_CURATION_PERIOD));
      } catch (NumberFormatException nfe) {
        period = DEFAULT_CURATION_PERIOD;
      }

      this.setDaemon(true);
      this.setName("[Sensision MetricsCurator (" + period + ")]");
      this.start();
    }

    @Override
    public void run() {
      //
      // Periodically scan the metrics and remove expired ones
      //

      while(true) {
        try {
          Thread.sleep(period);
        } catch (InterruptedException ie) {
        }

        if (null == values) {
          continue;
        }

        long now = System.currentTimeMillis();

        for (String cls: values.keySet()) {
          for (Value value: values.get(cls).values()) {
            if (null != value.expire && value.expire <= now) {
              getContainer(value.cls, value.labels, null);
            }
          }
        }
      }
    }
  }


  /**
   * Map of class name to map of label values to metric value.
   */
  private static Map<String, Map<Map<String,String>, Value>> values = new ConcurrentHashMap<String, Map<Map<String,String>,Value>>();

  /**
   * Set the value, location and elevation of the given metric (class + labels).
   *
   *  @param cls Name of the metrics class
   *  @param labels Map of labels uniquely qualifying the metric in its class
   *  @param ts Timestamp of the value, in microseconds since the Epoch
   *  @param latitude Latitude of the value - null if no location
   *  @param longitude Longitude of the value - null if no location
   *  @param elevation Elevation of the value, in mm - null if no elevation
   *  @param value Value to set the metric to, either boolean, long, double or String. Use null to discard a metric.
   *  @param ttl Time to live of the metric. If the next update does not occur within this number of milliseconds, the metric will be discarded - null if no ttl
   */
  public static synchronized final void set(String cls, Map<String,String> labels, long ts, Float latitude, Float longitude, Long elevation, Object value, Long ttl) {

    if (Sensision.disable) {
      return;
    }

    //
    // Check that class name and labels are all non null
    //

    if (null == cls) {
      throw new RuntimeException("Invalid null class name for metric");
    }

    if (labels.containsKey(null) || labels.containsValue(null)) {
      throw new RuntimeException("Invalid null label for metric " + cls + labels);
    }

    //
    // Create a Value container if we don't already know this metric.
    // CAUTION: we do not synchronize call, so we might overwrite an ongoing first update
    // if they occur at very high frequency
    //

    Value container = getContainer(cls, labels, value);

    // null container means we actually removed a Value so return immediately
    if (null == container || null == value) {
      return;
    }

    //
    // Check value type
    //

    if (value instanceof Long || value instanceof Integer || value instanceof Short || value instanceof Byte || value instanceof BigInteger) {
      if (null != container.type && Value.TYPE.LONG != container.type) {
        throw new RuntimeException("Invalid value type for " + cls + labels + ", was " + container.type + ", is now LONG");
      }
      if (null == container.value) {
        container.type = Value.TYPE.LONG;
        container.value = new AtomicLong(((Number) value).longValue());
      } else {
        ((AtomicLong) container.value).set(((Number) value).longValue());
      }
    } else if (value instanceof Double || value instanceof Float || value instanceof BigDecimal) {
      if (null != container.type && Value.TYPE.DOUBLE != container.type) {
        throw new RuntimeException("Invalid value type for " + cls + labels + ", was " + container.type + ", is now DOUBLE");
      }
      if (null == container.value) {
        container.type = Value.TYPE.DOUBLE;
        container.value = new AtomicLong(Double.doubleToRawLongBits(((Number) value).doubleValue()));
      } else {
        ((AtomicLong) container.value).set(Double.doubleToRawLongBits(((Number) value).doubleValue()));
      }
    } else if (value instanceof Boolean) {
      if (null != container.type && Value.TYPE.BOOLEAN != container.type) {
        throw new RuntimeException("Invalid value type for " + cls + labels + ", was " + container.type + ", is now BOOLEAN");
      }
      if (null == container.value) {
        container.type = Value.TYPE.BOOLEAN;
      }
      container.value = (Boolean) value;
    } else if (value instanceof String) {
      if (null != container.type && Value.TYPE.STRING != container.type) {
        throw new RuntimeException("Invalid value type for " + cls + labels + ", was " + container.type + ", is now STRING");
      }
      if (null == container.value) {
        container.type = Value.TYPE.STRING;
      }
      container.value = (String) value;
    } else {
      throw new RuntimeException("Invalid value type (" + value.getClass() + ") for metric " + cls + labels);
    }

    //
    // Set location/elevation
    //

    container.latitude = latitude;
    container.longitude = longitude;
    container.elevation = elevation;

    //
    // Force timestamp
    //

    container.timestamp = ts;

    if (null != ttl) {
      container.expire = System.currentTimeMillis() + ttl;
    } else {
      container.expire = null;
    }
  }

  private static Map<String,String> labelsToMap(String... labels) {
    Map<String,String> labelsmap = new HashMap<String,String>();

    if (null != labels) {
      for (int i = 0; i < labels.length / 2; i++) {
        labelsmap.put(labels[i * 2], labels[i * 2 + 1]);
      }
    }

    return labelsmap;
  }

  public static final void set(String cls, Map<String,String> labels, Object value) {
    set(cls, labels, System.currentTimeMillis() * TIME_UNITS_PER_MS, null, null, null, value, null);
  }

  public static final void set(String cls, Map<String,String> labels, Object value, Long ttl) {
    set(cls, labels, System.currentTimeMillis() * TIME_UNITS_PER_MS, null, null, null, value, ttl);
  }

  //
  // Versions of 'set' with an array of labels as parameter
  //

  public static final void set(String cls, long ts, Float latitude, Float longitude, Long elevation, Object value, Long ttl, String... labels) {
    set(cls, labelsToMap(labels), ts, latitude, longitude, elevation, value, ttl);
  }

  public static final void set(String cls, Object value, String... labels) {
    set(cls, labelsToMap(labels), value);
  }

  /**
   * Remove a metric container
   *
   * @param cls Class of metric
   * @param labels Labels for the metric
   *
   * @return true if there was a container to remove, false otherwise
   */
  public static synchronized final boolean clear(String cls, Map<String,String> labels) {
    Value container = getContainer(cls, labels, null);
    return null != container;
  }

  /**
   * Update a numerical metric with a numerical delta
   *
   * @param cls Name of metric class
   * @param labels Labels of metric
   * @param ts Timestamp of update in microseconds since the epoch
   * @param latitude Optional latitude of update
   * @param longitude Optional longitude of update
   * @param elevation Optional elevatio of update
   * @param delta Delta to apply to the value.
   * @param ttl
   */
  public static synchronized final void update(String cls, Map<String,String> labels, long ts, Float latitude, Float longitude, Long elevation, Number delta, Long ttl) {
    if (Sensision.disable) {
      return;
    }

    //
    // Check that class name and labels are all non null
    //

    if (null == cls) {
      throw new RuntimeException("Invalid null class name for metric");
    }

    if (labels.containsKey(null) || labels.containsValue(null)) {
      throw new RuntimeException("Invalid null label for metric " + cls + labels);
    }

    //
    // Create a Value container if we don't already know this metric.
    // CAUTION: we do not synchronize call, so we might overwrite an ongoing first update
    // if they occur at very high frequency
    //

    Value container = getContainer(cls, labels, delta);

    // null container means we actually removed a Value so return immediately
    if (null == container) {
      return;
    }

    //
    // Check value type
    //

    if (Value.TYPE.LONG == container.type) {
      ((AtomicLong) container.value).addAndGet(((Number) delta).longValue());
    } else if (Value.TYPE.DOUBLE == container.type) {
      ((AtomicLong) container.value).set(Double.doubleToRawLongBits(((Number) delta).doubleValue() + Double.longBitsToDouble(((AtomicLong) container.value).get())));
    } else if (null == container.type) {
      if (delta instanceof Long || delta instanceof Integer || delta instanceof Short || delta instanceof Byte || delta instanceof BigInteger) {
        container.type = Value.TYPE.LONG;
        container.value = new AtomicLong(((Number) delta).longValue());
      } else if (delta instanceof Double || delta instanceof Float || delta instanceof BigDecimal) {
        container.type = Value.TYPE.DOUBLE;
        container.value = new AtomicLong(Double.doubleToRawLongBits(((Number) delta).doubleValue()));
      }
    } else {
      throw new RuntimeException("Can only update metrics of type LONG or DOUBLE (" + cls + labels + ")");
    }

    //
    // Set location/elevation
    //

    container.latitude = latitude;
    container.longitude = longitude;
    container.elevation = elevation;

    //
    // Force timestamp
    //

    container.timestamp = ts;

    if (null != ttl) {
      container.expire = System.currentTimeMillis() + ttl;
    }
  }

  public static final void update(String cls, Map<String,String> labels, Long ttl, Number delta) {
    update(cls, labels, System.currentTimeMillis() * TIME_UNITS_PER_MS, null, null, null, delta, ttl);
  }

  public static final void update(String cls, Map<String,String> labels, Number delta) {
    update(cls, labels, System.currentTimeMillis() * TIME_UNITS_PER_MS, null, null, null, delta, null);
  }

  //
  // Versions of 'update' with an array of labels as parameter
  //

  public static final void update(String cls, long ts, Float latitude, Float longitude, Long elevation, Number delta, Long ttl, String... labels) {
    update(cls, labelsToMap(labels), ts, latitude, longitude, elevation, delta, ttl);
  }

  public static final void update(String cls, Number delta, String... labels) {
    update(cls, labelsToMap(labels), delta);
  }

  public static final void event(String cls, Map<String,String> labels, Object value) {
    event(System.currentTimeMillis() * TIME_UNITS_PER_MS, Double.NaN, Double.NaN, null, cls, labels, value);
  }

  /**
   * Store an event. We do not use a synchronized method, which means that if the rate of events is very important
   * we might overwrite an event with an older one if the older one was longer to encode.
   *
   * This is a risk we tolerate for now.
   */
  public static final void event(Long ts, Double latitude, Double longitude, Long elevation, String cls, Map<String,String> labels, Object value) {

    // Do nothing if we do not keep track of events
    if ((0 == events.length || null == value) && null == eventsDir) {
      return;
    }

    //
    // Increment event sequence
    //

    long seqno = eventSequence.addAndGet(1L);

    //
    // Build event
    //

    StringBuilder sb = buildEvent(ts, latitude, longitude, elevation, cls, labels, value);

    //
    // Store event on disk
    //

    storeEvent(sb);

    //
    // Store event in history
    //

    if (events.length > 0) {
      //
      // Compute array index
      //

      int idx = (int) (seqno % (long) events.length);

      events[idx] = sb.toString();
    }
  }

  private static void storeEvent(StringBuilder sb) {
    try {
      if (null == eventsDir) {
        return;
      }
      synchronized(eventsDir) {
        File root = new File(eventsDir);

        if (!root.exists() || !root.isDirectory()) {
          return;
        }

        if (null == eventsFile) {
          String now = Long.toHexString(Long.MAX_VALUE - System.currentTimeMillis());
          String uuid = UUID.randomUUID().toString();
          // End the name with a '.' so even if eventsSuffix is SENSISION_METRICS_SUFFIX it won't bother
          eventsFile = new File(new File(eventsDir), now + "." + uuid + (null != eventsSuffix ? ("." + eventsSuffix) : ""));

          try {
            eventsWriter = new PrintWriter(eventsFile);
          } catch (FileNotFoundException fnfe) {
            eventsFile = null;
            return;
          }
        }
        if (null != sb && null != eventsWriter) {
          eventsWriter.println(sb.toString());
        }
      }
    } catch (Exception e) {
      if (null != eventsWriter) {
        eventsWriter.close();
        eventsFile = null;
      }
      e.printStackTrace();
    }
  }

  public static void flushEvents() {
    if (null == eventsDir) {
      return;
    }
    synchronized(eventsDir) {
      if (null != eventsWriter) {
        eventsWriter.close();
        eventsFile.renameTo(new File(new File(eventsDir), eventsFile.getName() + SENSISION_METRICS_SUFFIX));
      }
      eventsFile = null;
      eventsWriter = null;
    }
  }

  public static boolean onDisk() {
    return null != eventsDir;
  }

  private static StringBuilder buildEvent(Long ts, Double latitude, Double longitude, Long elevation, String cls, Map<String,String> labels, Object value) {
    //
    // Build representation of event
    //

    StringBuilder sb = new StringBuilder();

    if (null != ts) {
      sb.append(ts);
    } else {
      sb.append(System.currentTimeMillis() * TIME_UNITS_PER_MS);
    }

    sb.append("/");

    if (null != latitude && null != longitude && Double.isFinite(latitude) && Double.isFinite(longitude)) {
      sb.append(latitude);
      sb.append(":");
      sb.append(longitude);
    }

    sb.append("/");

    if (null != elevation) {
      sb.append(elevation);
    }

    sb.append(" ");

    metadataToString(sb, cls, labels);

    sb.append(" ");

    if (value instanceof Number) {
      sb.append(value);
    } else if (value instanceof Boolean) {
      if (Boolean.TRUE.equals(value)) {
        sb.append("T");
      } else {
        sb.append("F");
      }
    } else if (value instanceof String) {
      sb.append("'");
      encodeName(sb, value.toString());
      sb.append("'");
    }

    return sb;
  }
  public static void encodeName(StringBuilder sb, String name) {
    if (null == name) {
      return;
    }
    try {
      sb.append(URLEncoder.encode(name, "UTF-8").replaceAll("\\{", "%7B").replaceAll("\\}", "%7D").replaceAll(",", "%2C").replaceAll("\\+", "%20"));
    } catch (UnsupportedEncodingException uee) {
    }
  }

  public static void metadataToString(StringBuilder sb, String name, Map<String,String> labels) {
    encodeName(sb, name);
    sb.append("{");
    boolean first = true;

    for (Entry<String, String> entry: labels.entrySet()) {
      if (!first) {
        sb.append(",");
      }
      encodeName(sb, entry.getKey());
      sb.append("=");
      encodeName(sb, entry.getValue());
      first = false;
    }
    sb.append("}");
  }

  /**
   * Return the current value of the given Geo Time Series.
   *
   * @param cls Class name of GTS
   * @param labels Labels map of GTS
   * @return the current value of the GTS or null if the GTS is unknown
   */
  public static final Object getValue(String cls, Map<String,String> labels) {
    Map<Map<String,String>, Value> clsValues = values.get(cls);
    if (null == clsValues) {
      return null;
    }
    Value container = clsValues.get(labels);

    if (null == container) {
      return null;
    }

    switch(container.type) {
      case BOOLEAN:
        return container.value;
      case LONG:
        return ((AtomicLong)container.value).get();
      case DOUBLE:
        return Double.longBitsToDouble(((AtomicLong)container.value).get());
      case STRING:
        return container.value;
      default:
        return null;
    }
  }

  /**
   * Return the current location of the given Geo Time Series.
   *
   * @param cls Class name of GTS
   * @param labels Labels map of GTS
   * @return an array of 2 floats (latitude, longitude) or null if either the GTS is unknown or the location undefined
   */
  public static final float[] getLocation(String cls, Map<String,String> labels) {
    Map<Map<String,String>, Value> clsValues = values.get(cls);
    if (null == clsValues) {
      return null;
    }
    Value container = clsValues.get(labels);

    if (null == container) {
      return null;
    }

    if (null == container.latitude || null == container.longitude) {
      return null;
    }

    float[] latlon = new float[2];
    latlon[0] = container.latitude;
    latlon[1] = container.longitude;

    return latlon;
  }

  /**
   * Return the current elevation of the given Geo Time Series.
   *
   * @param cls Class name of GTS
   * @param labels Labels map of GTS
   * @return the set elevation or null if either the GTS is unknown or the elevation undefined
   */
  public static final Long getElevation(String cls, Map<String,String> labels) {
    Map<Map<String,String>, Value> clsValues = values.get(cls);
    if (null == clsValues) {
      return null;
    }
    Value container = clsValues.get(labels);

    if (null == container) {
      return null;
    }

    return container.elevation;
  }

  /**
   * Return the current timestamp of the given Geo Time Series.
   *
   * @param cls Class name of GTS
   * @param labels Labels map of GTS
   * @return the set timestamp or null if the GTS is unknown
   */
  public static final Long getTimestamp(String cls, Map<String,String> labels) {
    Map<Map<String,String>, Value> clsValues = values.get(cls);
    if (null == clsValues) {
      return null;
    }
    Value container = clsValues.get(labels);

    if (null == container) {
      return null;
    }

    return container.timestamp;
  }

  private synchronized static final Value getContainer(String cls, Map<String,String> labels, Object value) {

    Value container = null;

    Map<Map<String,String>, Value> clsValues = values.get(cls);

    if (null == value) {
      //
      // If value was null, discard the given metric
      //
      if (null != clsValues) {
        container = clsValues.remove(labels);
        if (0 == clsValues.size()) {
          values.remove(cls);
        }
      }
    } else if (null == clsValues) {
      values.put(cls, new ConcurrentHashMap<Map<String,String>,Value>());
      container = new Value();
      // Make a clean copy of labels
      Map<String,String> lbls = new HashMap<String, String>();

      //
      // Copy labels using String#intern if instructed to optimize strings
      //

      if (!useStringIntern) {
        lbls.putAll(labels);
      } else {
        for (Entry<String,String> entry: labels.entrySet()) {
          lbls.put(entry.getKey().intern(), entry.getValue().intern());
        }
      }
      container.cls = cls;
      container.labels = lbls;
      values.get(cls).put(lbls, container);
    } else {
      container = clsValues.get(labels);
      if (null == container) {
        container = new Value();
        // Make a clean copy of labels
        Map<String,String> lbls = new HashMap<String, String>();

        //
        // Copy labels using String#intern if instructed to optimize strings
        //

        if (!useStringIntern) {
          lbls.putAll(labels);
        } else {
          for (Entry<String,String> entry: labels.entrySet()) {
            lbls.put(entry.getKey().intern(), entry.getValue().intern());
          }
        }
        container.cls = cls;
        container.labels = lbls;
        values.get(cls).put(lbls, container);
      }
    }

    return container;
  }

  /**
   * Parses a metric line into a Value instance.
   *
   * @param str
   * @return The parsed Value or null if a parse error occurred.
   */
  private static final Pattern MEASUREMENT_RE = Pattern.compile("^([0-9]+)?/(([0-9.-]+):([0-9.-]+))?/([0-9-]+)? +([^ ]+)\\{([^\\}]*)\\} +(.+)$");

  public static final Value parseMetric(String str) {
    Matcher matcher = MEASUREMENT_RE.matcher(str);

    if (!matcher.matches()) {
      return null;
    }

    //
    // Check name
    //

    String name = matcher.group(6);

    if (name.contains("%")) {
      try {
        name = URLDecoder.decode(name, "UTF-8");
      } catch (UnsupportedEncodingException uee) {
        // Can't happen, we're using UTF-8
      }
    }

    //
    // Parse labels.
    // We use a TreeMap so labels will be sorted, which is needed for the DeduplicationManager
    //

    Map<String,String> labels = new TreeMap<String,String>();

    // Split on ','
    String[] tokens = matcher.group(7).split(",");

    for (String token: tokens) {
      // Skip empty token
      if ("".equals(token)) {
        continue;
      }

      String[] subtokens = token.split("=");

      //
      // Ignore labels if there is more than 1 equal sign
      //

      if (subtokens.length > 2) {
        continue;
      }

      try {
        String lname = URLDecoder.decode(subtokens[0], "UTF-8");
        String lvalue = subtokens.length > 1 ? URLDecoder.decode(subtokens[1], "UTF-8") : "";

        labels.put(lname, lvalue);
      } catch (UnsupportedEncodingException uee) {
        // Can't happen, we're using UTF-8 which is one of the 6 standard JVM charsets
      }
    }

    //
    // Extract timestamp, optional location and elevation
    //

    Long timestamp = null;
    Float latitude = null;
    Float longitude = null;
    Long elevation = null;

    try {

      if (null != matcher.group(1)) {
        timestamp = Long.valueOf(matcher.group(1));
      } else {
        // Use a beacon value if timestamp was not specified
        timestamp = Long.MIN_VALUE;
      }

      if (null != matcher.group(2)) {
        latitude = Float.valueOf(matcher.group(3));
        longitude = Float.valueOf(matcher.group(4));
      }

      if (null != matcher.group(5)) {
        elevation = Long.valueOf(matcher.group(5));
      }
    } catch (NumberFormatException nfe) {
      return null;
    }

    //
    // Extract value
    //

    String valuestr = matcher.group(8);

    Object value = parseValue(valuestr);

    if (null == value) {
      return null;
    }

    Value v = new Value(name, labels, timestamp, latitude, longitude, elevation, value);

    return v;
  }

  private static final Pattern STRING_VALUE_RE = Pattern.compile("^['\"].*['\"]$");
  private static final Pattern BOOLEAN_VALUE_RE = Pattern.compile("^(T|F|true|false)$", Pattern.CASE_INSENSITIVE);
  private static final Pattern LONG_VALUE_RE = Pattern.compile("^[+-]?[0-9]+$");
  private static final Pattern DOUBLE_VALUE_RE = Pattern.compile("^[+-]?([0-9]+)\\.([0-9]+)(E\\-?[0-9]+)?$");

  /**
   * Parses a value. This methode is borrowed from continuum's GTSHelper#parseValue
   * @param valuestr
   * @return
   */
  public static Object parseValue(String valuestr) {

    Object value;

    Matcher valuematcher = DOUBLE_VALUE_RE.matcher(valuestr);

    if (valuematcher.matches()) {
      // FIXME(hbs): maybe find a better heuristic to determine if we should
      // create a BigDecimal or a Double. BigDecimal is only meaningful if its encoding
      // will be less than 8 bytes.

      //value = Double.valueOf(valuestr);
      if (valuematcher.group(1).length() < 10 && valuematcher.group(2).length() < 10 && null == valuematcher.group(3)) {
        value = new BigDecimal(valuestr);
      } else {
        value = Double.valueOf(valuestr);
      }
    } else {
      valuematcher = LONG_VALUE_RE.matcher(valuestr);

      if (valuematcher.matches()) {
        value = Long.valueOf(valuestr);
      } else {
        valuematcher = STRING_VALUE_RE.matcher(valuestr);

        if (valuematcher.matches()) {
          value = valuestr.substring(1, valuestr.length() - 1);
        } else {
          valuematcher = BOOLEAN_VALUE_RE.matcher(valuestr);

          if (valuematcher.matches()) {
            if (valuestr.startsWith("t") || valuestr.startsWith("T")) {
              value = Boolean.TRUE;
            } else {
              value = Boolean.FALSE;
            }
          } else {
            return null;
          }
        }
      }
    }

    return value;
  }


  /**
   * Dump last values of all known metrics onto the given PrintWriter.
   *
   * @param out PrintWriter instance to write to.
   * @param useValueTimestamp Whether or not we should use the timestamp associated with the value
   * @param openmetrics Output metrics in OpenMetrics format
   * @throws IOException
   */
  public static final void dump(PrintWriter out, boolean useValueTimestamp, boolean openmetrics) throws IOException {
    List<String> classes = new ArrayList<String>(values.keySet());

    for (String clazz: classes) {
      Map<Map<String,String>,Value> byClass = values.get(clazz);

      if (null == byClass) {
        continue;
      }

      List<Value> vals = new ArrayList<Value>(byClass.values());
      for (Value value: vals) {
        dumpValue(out, value, useValueTimestamp, true, openmetrics);
      }
    }

    //
    // Now dump external providers
    //

    for (Iterable<Value> provider: providers) {
      if (null == provider) {
        continue;
      }

      Iterator<Value> iterator = provider.iterator();

      if (null == iterator) {
        continue;
      }

      while(iterator.hasNext()) {
        dumpValue(out, iterator.next(), useValueTimestamp, true, openmetrics);
      }
    }
  }

  public static final void dump(PrintWriter out) throws IOException {
    dump(out, true, false);
  }

  /**
   * Dump the current values of metrics in the events file
   *
   * @param useValueTimestamp
   * @throws IOException
   */
  public static final void dumpAsEvents(boolean useValueTimestamp) throws IOException {
    if (null == eventsDir) {
      return;
    }
    synchronized(eventsDir) {
      //
      // Ensure we have a writer
      //
      storeEvent(null);

      if (null != eventsWriter) {
        dump(eventsWriter, useValueTimestamp, false);
      }
    }
  }

  /**
   * Outputs a value on a PrintWriter
   *
   * @param out PrintWriter to use
   * @param value Value instance to display
   * @param useValueTimestamp flag indicating whether or not we consider the Value ts or 'now'
   * @throws IOException
   */
  public static final void dumpValue(PrintWriter out, Value value, boolean useValueTimestamp, boolean crlf, boolean openmetrics) throws IOException {
    Object v = null;

    switch(value.type) {
      case LONG:
        if (value.value instanceof AtomicLong) {
          v = ((AtomicLong) value.value).longValue();
        } else {
          v = ((Number) value.value).longValue();
        }
        break;
      case DOUBLE:
        if (value.value instanceof Double || value.value instanceof Float) {
          v = ((Number) value.value).doubleValue();
        } else {
          v = Double.longBitsToDouble(((AtomicLong) value.value).longValue());
        }
        break;
      case BOOLEAN:
      case STRING:
        v = value.value;
        break;
    }
    dumpValue(out, value.cls, value.labels, useValueTimestamp ? value.timestamp : System.currentTimeMillis() * TIME_UNITS_PER_MS, value.latitude, value.longitude, value.elevation, v, crlf, openmetrics);
  }

  private static final void dumpValue(PrintWriter out, Value value) throws IOException {
    dumpValue(out, value, true, true, false);
  }

  private static final void dumpValue(PrintWriter out, String name, Map<String,String> labels, long timestamp, Float latitude, Float longitude, Long elevation, Object value, boolean crlf, boolean openmetrics) throws IOException {
    if (null == out) {
      return;
    }
    if (openmetrics) {
      OpenMetrics.dump(out, name, labels, timestamp, value);
      return;
    }

    if (Long.MIN_VALUE != timestamp) {
      out.print(timestamp);
    }
    out.print("/");
    if (null != latitude && null != longitude) {
      out.print(latitude);
      out.print(":");
      out.print(longitude);
    } else if (null != defaultLatitude && null != defaultLongitude) {
      out.print(defaultLatitude);
      out.print(":");
      out.print(defaultLongitude);
    }
    out.print("/");
    if (null != elevation) {
      out.print(elevation);
    } else if (null != defaultElevation) {
      out.print(defaultElevation);
    }
    out.print(" ");
    out.print(URLEncoder.encode(name, "UTF-8").replaceAll("\\{", "%7B").replaceAll("\\}", "%7D").replaceAll("\\+", "%20"));
    out.print("{");
    boolean first = true;
    //
    // Add default labels
    //
    for (String label: defaultLabels.keySet()) {
      if (!labels.containsKey(label)) {
        if (!first) {
          out.print(",");
        }
        out.print(URLEncoder.encode(label, "UTF-8").replaceAll("\\{", "%7B").replaceAll("\\}", "%7D").replaceAll(",", "%2C").replaceAll("\\+", "%20"));
        out.print("=");
        out.print(URLEncoder.encode(defaultLabels.get(label), "UTF-8").replaceAll("\\{", "%7B").replaceAll("\\}", "%7D").replaceAll(",", "%2C").replaceAll("\\+", "%20"));
        first = false;
      }
    }
    for (String label: labels.keySet()) {
      if (!first) {
        out.print(",");
      }
      out.print(URLEncoder.encode(label, "UTF-8").replaceAll("\\{", "%7B").replaceAll("\\}", "%7D").replaceAll(",", "%2C").replaceAll("\\+", "%20"));
      out.print("=");
      out.print(URLEncoder.encode(labels.get(label), "UTF-8").replaceAll("\\{", "%7B").replaceAll("\\}", "%7D").replaceAll(",", "%2C").replaceAll("\\+", "%20"));
      first = false;
    }
    out.print("}");
    out.print(" ");
    if (value instanceof Long || value instanceof Integer || value instanceof BigInteger) {
      out.print(((Number) value).longValue());
    } else if (value instanceof Double || value instanceof Float || value instanceof BigDecimal) {
      out.print(((Number) value).doubleValue());
    } else if (value instanceof String) {
      out.print("'");
      out.print(URLEncoder.encode((String) value, "UTF-8").replaceAll("'", "%27").replaceAll("\\+", "%20"));
      out.print("'");
    } else if (value instanceof Boolean) {
      if (Boolean.TRUE.equals(value)) {
        out.print("T");
      } else {
        out.print("F");
      }
    }
    if (crlf) {
      out.print("\r\n");
    }
  }

  public static String getUUID() {
    return uuid;
  }

  public static long getStartTime() {
    return starttime;
  }

  public static File getHomeDir() {
    return new File(System.getProperty(Sensision.SENSISION_HOME, Sensision.DEFAULT_SENSISION_HOME));
  }

  public static File getQueueDir() {
    return new File(getHomeDir(), Sensision.SENSISION_QUEUE_SUBDIR);
  }

  public static File getMetricsDir() {
    return new File(getHomeDir(), Sensision.SENSISION_METRICS_SUBDIR);
  }

  public static File getTargetsDir() {
    return new File(getHomeDir(), Sensision.SENSISION_TARGETS_SUBDIR);
  }

  public static Pattern getTargetPattern() {
    return SENSISION_TARGET_PATTERN;
  }

  public static void addDefaultLocation(StringBuilder sb) {
    if (null != defaultLatitude && null != defaultLongitude) {
      sb.append(defaultLatitude);
      sb.append(":");
      sb.append(defaultLongitude);
    }
  }

  public static void addDefaultElevation(StringBuilder sb) {
    if (null != defaultElevation) {
      sb.append(defaultElevation);
    }
  }

  public static void addValueProvider(Iterable<Value> provider) {
    providers.add(provider);
  }

  public static void removeValueProvider(Iterable<Value> provider) {
    providers.remove(provider);
  }

  /**
   * Read a property file and return a Properties instance and set System properties accordingly
   *
   * @param name
   * @return
   * @throws IOException
   */
  public static Properties readConfigFile(String name) throws IOException {

    //
    // Read the configuration
    //

    BufferedReader br = new BufferedReader(new FileReader(name));

    Properties props = new Properties();

    try {

      int lineno = 0;

      while (true) {
        String line = br.readLine();

        if (null == line) {
          break;
        }

        lineno++;
        line = line.trim();

        if ("".equals(line) || line.startsWith("#") || line.startsWith("//") || line.startsWith("--")) {
          continue;
        }

        if (!line.contains("=")) {
          throw new IOException("Syntax error on line " + lineno);
        }

        String key = line.replaceAll("^([^=]+)=.*$", "$1").trim();
        String value = line.replaceAll("^([^=]+)=", "").trim();

        props.setProperty(key, value);

        //
        // Copy k/v to system properties or override the read value with system property
        //

        if (null == System.getProperty(key)) {
          System.setProperty(key, value);
        } else {
          props.setProperty(key, System.getProperty(key));
        }
      }
    } finally {
      if (null != br) { try { br.close(); } catch (Exception e) {} }
    }

    return props;
  }

  public static long getCurrentEvent() {
    return eventSequence.get();
  }

  public static List<String> getEvents() {
    return Collections.unmodifiableList(Arrays.asList(events));
  }

  public static void setInstance(String name) {
    instanceName = name;
  }

  public static String getInstance() {
    return instanceName;
  }

  public static long getTimeUnitsPerMs() {
    return TIME_UNITS_PER_MS;
  }
}
