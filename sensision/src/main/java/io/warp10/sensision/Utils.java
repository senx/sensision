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
import groovy.lang.Script;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class Utils {
  public static void populateSymbolTable(Script script) {
    Binding binding = script.getBinding();
    
    for (Entry<Object,Object> entry: System.getProperties().entrySet()) {
      String key = entry.getKey().toString();
      
      if (key.startsWith("var.")) {
        binding.setVariable(key.substring(4), entry.getValue());
      }
    }
  }
  
  public static File getMetricsFile(String cls) {
    StringBuilder sb = new StringBuilder();
    sb.append(Long.toHexString(Long.MAX_VALUE - System.currentTimeMillis()));
    sb.append(".");
    sb.append(cls);
    sb.append(Sensision.SENSISION_METRICS_SUFFIX);
    
    return new File(Sensision.getMetricsDir(), sb.toString());
  }
  
  public static void storeMetric(PrintWriter pw, long ts, String cls, Map<String,String> labels, Object value) throws IOException {
    pw.append(Long.toString(ts));
    pw.append("/");
    if (null != Sensision.defaultLatitude && null != Sensision.defaultLongitude) {
      pw.append(Sensision.defaultLatitude.toString());
      pw.append(":");
      pw.append(Sensision.defaultLongitude.toString());
    }
    pw.append("/");
    if (null != Sensision.defaultElevation) {
      pw.append(Sensision.defaultElevation.toString());
    }
    pw.append(" ");
    pw.append(URLEncoder.encode(cls, "UTF-8"));
    pw.append("{");
    boolean first = true;
    Map<String,String> gtslabels = new HashMap<String, String>();
    if (null != Sensision.defaultLabels) {
      gtslabels.putAll(Sensision.defaultLabels);
    }
    gtslabels.putAll(labels);
    for (Entry<String,String> entry: gtslabels.entrySet()) {
      if (!first) {
        pw.append(",");
      }
      pw.append(URLEncoder.encode(entry.getKey(), "UTF-8"));
      pw.append("=");
      pw.append(URLEncoder.encode(entry.getValue(), "UTF-8"));
      first=false;     
    }
    pw.append("} ");
    if (value instanceof Long || value instanceof Integer || value instanceof Short || value instanceof Byte || value instanceof BigInteger) {
      pw.append(Long.toString(((Number) value).longValue()));
    } else if (value instanceof Double || value instanceof Float || value instanceof BigDecimal) {
      pw.append(Double.toString(((Number) value).doubleValue()));
    } else if (value instanceof Boolean) {
      pw.append(Boolean.TRUE.equals(value) ? "T" : "F");
    } else {
      pw.append("'");
      pw.append(URLEncoder.encode(value.toString(), "UTF-8"));
      pw.append("'");
    }
    pw.append("\n");
  }
}
