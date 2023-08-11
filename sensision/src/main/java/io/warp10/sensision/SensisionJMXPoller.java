//
//   Copyright 2018-2022  SenX S.A.S.
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

import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.lang.instrument.Instrumentation;
import java.lang.management.ManagementFactory;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Pattern;

import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.TabularData;

/**
 * Java Agent which reads JMX MBeans and produces Sensision metrics accordingly
 */
public class SensisionJMXPoller {

  private static final String SENSISION_JMX_METRICS_PREFIX = "jmx.";

  /**
   * Array of patterns to match MBean classes which should be included
   */
  private Pattern[] includedMBeans;

  /**
   * Array of patterns to match MBean classes which should be excluded
   */
  private Pattern[] excludedMBeans;

  /**
   * Array of patterns to match name of metrics which should be included
   */
  private Pattern[] includedMetrics;

  /**
   * Array of patterns to match name of metrics which should be excluded
   */
  private Pattern[] excludedMetrics;

  public SensisionJMXPoller(String agentArgs, Instrumentation instrumentation) {
    String[] tokens = null;

    if (null != agentArgs) {
      tokens = agentArgs.split(":");
    } else {
      tokens = new String[0];
    }

    for (String token: tokens) {
      if (token.startsWith("metrics=")) {
        String[] subtokens = token.substring(8).split(",");

        includedMetrics = new Pattern[subtokens.length];
        excludedMetrics = new Pattern[subtokens.length];

        int i = 0;
        for (String subtoken: subtokens) {
          try {
            subtoken = URLDecoder.decode(subtoken, "UTF-8");
          } catch (UnsupportedEncodingException uee) {
          }
          if (subtoken.startsWith("!")) {
            // Skip slot in includedMetrics as this subtoken is an exclusion
            includedMetrics[i] = null;
            if (subtoken.startsWith("~")) {
              excludedMetrics[i] = Pattern.compile(subtoken.substring(1));
            } else if (subtoken.startsWith("=")) {
              excludedMetrics[i] = Pattern.compile(Pattern.quote(subtoken.substring(1)));
            } else {
              excludedMetrics[i] = Pattern.compile(Pattern.quote(subtoken));
            }
          } else {
            // Skip slot in excludedMetrics as this subtoken is an inclusion
            excludedMetrics[i] = null;
            if (subtoken.startsWith("~")) {
              includedMetrics[i] = Pattern.compile(subtoken.substring(1));
            } else if (subtoken.startsWith("=")) {
              includedMetrics[i] = Pattern.compile(Pattern.quote(subtoken.substring(1)));
            } else {
              includedMetrics[i] = Pattern.compile(Pattern.quote(subtoken));
            }
          }
          i++;
        }
      } else if (token.startsWith("mbeans=")) {
        String[] subtokens = token.substring(7).split(",");

        includedMBeans = new Pattern[subtokens.length];
        excludedMBeans = new Pattern[subtokens.length];

        int i = 0;

        for (String subtoken: subtokens) {
          try {
            subtoken = URLDecoder.decode(subtoken, "UTF-8");
          } catch (UnsupportedEncodingException uee) {
          }
          if (subtoken.startsWith("!")) {
            // Skip slot in includedMBeans as this subtoken is an exclusion
            includedMBeans[i] = null;
            if (subtoken.startsWith("~")) {
              excludedMBeans[i] = Pattern.compile(subtoken.substring(1));
            } else if (subtoken.startsWith("=")) {
              excludedMBeans[i] = Pattern.compile(Pattern.quote(subtoken.substring(1)));
            } else {
              excludedMBeans[i] = Pattern.compile(Pattern.quote(subtoken));
            }
          } else {
            // Skip slot in excludedMBeans as this subtoken is an inclusion
            excludedMBeans[i] = null;
            if (subtoken.startsWith("~")) {
              includedMBeans[i] = Pattern.compile(subtoken.substring(1));
            } else if (subtoken.startsWith("=")) {
              includedMBeans[i] = Pattern.compile(Pattern.quote(subtoken.substring(1)));
            } else {
              includedMBeans[i] = Pattern.compile(Pattern.quote(subtoken));
            }
          }
          i++;
        }

      }
    }
  }

  private boolean checkPatterns(String value, Pattern[] included, Pattern[] excluded) {

    if (null == included || null == excluded) {
      return true;
    }

    boolean hasIncludes = false;

    for (int i = 0; i < included.length; i++) {
      if (null != included[i]) {
        hasIncludes = true;
        // Return true if we have a match
        if (included[i].matcher(value).matches()) {
          return true;
        }
      } else if (null != excluded[i]) {
        if (excluded[i].matcher(value).matches()) {
          return false;
        }
      }
    }

    // If there were some include clauses, return false if none matched
    // otherwise there were only exclusions and none matched, so return true
    if (hasIncludes) {
      return false;
    } else {
      return true;
    }
  }

  /**
   * Dump metrics to a PrintWriter
   */
  void dump(PrintWriter pw, boolean openmetrics) {

    Set<MBeanServer> servers = new HashSet<MBeanServer>();

    //
    // Retrieve all MBeanServer instances
    //

    servers.add(ManagementFactory.getPlatformMBeanServer());
    servers.addAll(MBeanServerFactory.findMBeanServer(null));

    for (MBeanServer server: servers) {
      //
      // Extract all MBeans
      //

      Collection<ObjectName> names;

      try {
        names = server.queryNames(new ObjectName("*:*"), null);
      } catch (MalformedObjectNameException mone) {
        return;
      }

      //
      // Loop over the retrieved MBeans
      //

      for (ObjectName name: names) {
        MBeanInfo info = null;
        ObjectInstance instance = null;

        try {
          info = server.getMBeanInfo(name);
          instance = server.getObjectInstance(name);
        } catch (IntrospectionException ie) {
        } catch (ReflectionException re) {
        } catch (InstanceNotFoundException infe) {
        } finally {
          if (null == info || null == instance) {
            continue;
          }
        }

        String className = info.getClassName();

        if (!checkPatterns(className, includedMBeans, excludedMBeans)) {
          continue;
        }

        //
        // Fill labels for the attribute
        //

        Map<String,String> labels = new HashMap<String, String>();

        //
        // Since there may be multiple instances of the same MBean class, we need to find an id for this
        // particular instance. As the call to queryNames returns a set, we can assume that the hashcodes
        // differ in all instances.
        //

        labels.put("jmx.domain", name.getDomain());
        labels.put("jmx.hashcode", Integer.toHexString(name.hashCode()));

        //
        // Extract the property lists of the MBean and store them as labels
        //

        Hashtable<String,String> props = name.getKeyPropertyList();

        for (String propname: props.keySet()) {
          labels.put("jmx." + propname, props.get(propname));
        }

        MBeanAttributeInfo[] attrinfo = info.getAttributes();

        //
        // Loop over the MBean attributes
        //

        for (MBeanAttributeInfo attr: attrinfo) {

          try {
            Object value = server.getAttribute(name, attr.getName());

            //
            // Build the metric name as PREFIX + <className> + ":" + <attributeName>

            StringBuilder sb = new StringBuilder(SENSISION_JMX_METRICS_PREFIX);
            sb.append(className);
            sb.append(":");
            sb.append(attr.getName());

            String metricName = sb.toString();

            if ("long".equals(attr.getType()) || "int".equals(attr.getType())) {
              dumpMetric(pw, metricName, labels, ((Number) value).longValue(), openmetrics);
            } else if ("double".equals(attr.getType()) || "float".equals(attr.getType())) {
              dumpMetric(pw, metricName, labels, ((Number) value).doubleValue(), openmetrics);
            } else if ("java.lang.String".equals(attr.getType())) {
              dumpMetric(pw, metricName, labels, value.toString(), openmetrics);
            } else if ("boolean".equals(attr.getType())) {
              dumpMetric(pw, metricName, labels, ((Boolean) value).booleanValue(), openmetrics);
            } else if ("[Ljava.lang.String;".equals(attr.getType())) {
              sb.append(":stringlist");

              sb.setLength(0);
              for (String s: (String[]) value) {
                if (sb.length() > 0) {
                  sb.append(",");
                }
                sb.append(s);
              }
              dumpMetric(pw, metricName, labels, sb.toString(), openmetrics);
            } else if (value instanceof TabularData) {
              TabularData td = (TabularData) value;
              dumpTabularData(pw, metricName, labels, td, openmetrics);
            } else if (value instanceof javax.management.openmbean.CompositeData || "javax.management.openmbean.CompositeData".equals(attr.getType())) {
              CompositeData cd = (CompositeData) value;
              dumpCompositeData(pw, metricName, labels, cd, openmetrics);
            } else {
            }
          } catch (Exception e) {}
        }
      }
    }
  }

  /**
   * Write a single metric to an output PrintWriter
   *
   * @param pw
   * @param name
   * @param labels
   * @param value
   */
  private final void dumpMetric(PrintWriter pw, String name, Map<String,String> labels, Object value, boolean openmetrics) {

    if (!checkPatterns(name, includedMetrics, excludedMetrics)) {
      return;
    }

    if (openmetrics) {
      OpenMetrics.dump(pw, name, labels, null, value);
      return;
    }

    StringBuilder sb = new StringBuilder();

    try {
      sb.append(System.currentTimeMillis() * Sensision.TIME_UNITS_PER_MS);
      sb.append("/");
      Sensision.addDefaultLocation(sb);
      sb.append("/");
      Sensision.addDefaultLocation(sb);
      sb.append(" ");
      sb.append(URLEncoder.encode(name, "UTF-8").replaceAll("\\{", "%7B").replaceAll("\\}", "%7D"));
      sb.append("{");
      boolean first = true;
      for (Entry<String,String> entry: labels.entrySet()) {
        if (!first) {
          sb.append(",");
        }
        sb.append(URLEncoder.encode(entry.getKey(), "UTF-8").replaceAll("\\{", "%7B").replaceAll("\\}", "%7D").replaceAll(",", "%2C"));
        sb.append("=");
        sb.append(URLEncoder.encode(entry.getValue(), "UTF-8").replaceAll("\\{", "%7B").replaceAll("\\}", "%7D").replaceAll(",", "%2C"));
        first = false;
      }
      sb.append("} ");

      if (value instanceof Long) {
        sb.append(Long.toString((Long) value));
      } else if (value instanceof Double) {
        sb.append(Double.toString((Double) value));
      } else if (value instanceof Boolean) {
        sb.append(((Boolean) value) ? "T" : "F");
      } else {
        sb.append("'");
        sb.append(URLEncoder.encode(value.toString(), "UTF-8").replaceAll("'", "%27"));
        sb.append("'");
      }
      sb.append("\r\n");
      pw.print(sb.toString());
    } catch (UnsupportedEncodingException uee) {
      // Can't happen, we're using UTF-8
    }
  }

  private final void dumpMetric(PrintWriter pw, String name, Map<String,String> labels, Object value) {
    dumpMetric(pw, name, labels, value, false);
  }

  private final void dumpTabularData(PrintWriter pw, String name, Map<String,String> labels, TabularData td, boolean openmetrics) {
    for (Object rowkey: td.keySet()) {

      if (rowkey instanceof List) {
        Map<String,String> rowlabels = new HashMap<String, String>();
        rowlabels.putAll(labels);

        final CompositeData cd = td.get(((List) rowkey).toArray());

        rowlabels.put("row", rowkey.toString());

        //
        // If the composite data has two entries, 'key' and 'value', use the value of 'key' as the key and the value of 'value' as the value
        //

        if (cd.containsKey("key") && cd.containsKey("value") && cd.values().size() == 2) {
          Map<String,Object> values = new HashMap<String, Object>();

          Object k = cd.get("key");
          //if (k instanceof byte[]) {
          //  k = hex((byte[]) k);
          //}
          values.put(k.toString(), cd.get("value"));
          try {
            CompositeType ct = new CompositeType(
                cd.getCompositeType().getTypeName(),
                cd.getCompositeType().getDescription(),
                new String[] { k.toString() },
                new String[] { cd.getCompositeType().getDescription("value") },
                new OpenType[] { cd.getCompositeType().getType("value")});
            CompositeDataSupport cds = new CompositeDataSupport(ct, values);
            dumpCompositeData(pw, name, rowlabels, cds, openmetrics);
          } catch (OpenDataException ode) {
          }
        } else {
          dumpCompositeData(pw, name, rowlabels, cd, openmetrics);
        }
      } else {
        // Weird, this should be impossible!
      }
    }
  }

  private final void dumpCompositeData(PrintWriter pw, String name, Map<String,String> labels, CompositeData cd, boolean openmetrics) {

    StringBuilder sb = new StringBuilder(name);

    int len = sb.length();

    for(String key: cd.getCompositeType().keySet()) {
      sb.setLength(len);

      sb.append(":");
      sb.append(key);

      Object val = cd.get(key);

      if (val instanceof Long || val instanceof Integer || val instanceof Short || val instanceof Byte) {
        dumpMetric(pw, sb.toString(), labels, ((Number) val).longValue(), openmetrics);
      } else if (val instanceof Double || val instanceof Float) {
        dumpMetric(pw, sb.toString(), labels, ((Number) val).doubleValue(), openmetrics);
      } else if (val instanceof Boolean) {
        dumpMetric(pw, sb.toString(), labels, ((Boolean) val).booleanValue(), openmetrics);
      } else if (val instanceof String) {
        dumpMetric(pw, sb.toString(), labels, val.toString(), openmetrics);
      } else if (val instanceof TabularData) {
        dumpTabularData(pw, sb.toString(), labels, (TabularData) val, openmetrics);
      } else if (val instanceof CompositeData) {
        dumpCompositeData(pw, sb.toString(), labels, (CompositeData) val, openmetrics);
      } else {
        dumpMetric(pw, sb.toString(), labels, val.toString(), openmetrics);
      }
    }
  }

  private static final String HEXDIGITS = "0123456789abcdef";
  private final String hex(byte[] bytes) {

    StringBuilder sb = new StringBuilder();
    for (byte b: bytes) {
      sb.append(HEXDIGITS.charAt(b >>> 4));
      sb.append(HEXDIGITS.charAt(b & 0xf));
    }
    return sb.toString();
  }
  public static void premain(String agentArgs, Instrumentation instrumentation) {
    SensisionJMXPoller agent = new SensisionJMXPoller(agentArgs, instrumentation);
  }
}
