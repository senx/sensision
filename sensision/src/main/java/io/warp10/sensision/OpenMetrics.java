//
//   Copyright 2022  SenX S.A.S.
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
import java.util.Map;
import java.util.Map.Entry;

public class OpenMetrics {

  public static void dump(PrintWriter out, String name, Map<String,String> labels, Long timestamp, Object value) {

    //
    // OpenMetrics only supports numerical values, ignore Strings and Booleans
    //

    if (!(value instanceof Number)) {
      return;
    }

    out.print(sanitizeClassName(name));

    if (null != labels && !labels.isEmpty()) {
      out.print("{");
      boolean first = true;
      for (Entry<String,String> label: labels.entrySet()) {
        if (!first) {
          out.print(",");
        }
        out.print(sanitizeLabelName(label.getKey()));
        out.print("=");
        out.print("\"");
        out.print(sanitizeLabelValue(label.getValue()));
        out.print("\"");
        first = false;
      }
      out.print("}");
    }

    out.print(" ");
    out.print(value);
    if (null != timestamp) {
      out.print(" ");
      out.print(timestamp);
    }
    // OpenMetrics mandates that line end with a LF and no CR
    out.print("\n");
  }

  private static String sanitizeClassName(String name) {
    //
    // As per OpenMetrics format, metrics name can only contain letters, digits, _ and :
    // and MUST start by a letter, _ or :
    // If that is not the case for class name, adapt it by replacing forbidden chars by '_' and
    // prefixing the name with '_' if it starts with a digit
    //
    // @see https://github.com/OpenObservability/OpenMetrics/blob/main/specification/OpenMetrics.md
    //

    StringBuilder sb = null;

    for (int i = 0; i < name.length(); i++) {
      if (name.charAt(i) >= 'a' && name.charAt(i) <= 'z') {
        if (null != sb) {
          sb.append(name.charAt(i));
        }
        continue;
      }
      if (name.charAt(i) >= 'A' && name.charAt(i) <= 'Z') {
        if (null != sb) {
          sb.append(name.charAt(i));
        }
        continue;
      }
      if (name.charAt(i) >= '0' && name.charAt(i) <= '9') {
        if (null != sb) {
          sb.append(name.charAt(i));
        }
        continue;
      }
      if ('_' == name.charAt(i) || ':' == name.charAt(i)) {
        if (null != sb) {
          sb.append(name.charAt(i));
        }
        continue;
      }
      if (null == sb) {
        sb = new StringBuilder(name.length());
        sb.append(name, 0, i);
      }
      // Replace forbidden char by '_'
      sb.append("_");
    }

    //
    // Check the first char
    //

    if (null == sb) {
      if (name.charAt(0) >= '0' && name.charAt(0) <= '9') {
        return "_" + name;
      } else {
        return name;
      }
    } else {
      if (sb.charAt(0) >= '0' && sb.charAt(0) <= '9') {
        return "_" + sb.toString();
      } else {
        return sb.toString();
      }
    }
  }

  private static final String sanitizeLabelName(String name) {
    //
    // Label names in OpenMetrics can only start by a letter or '_' and can only
    // contain letters, digits and '_'
    //

    StringBuilder sb = null;

    for (int i = 0; i < name.length(); i++) {
      if (name.charAt(i) >= 'a' && name.charAt(i) <= 'z') {
        if (null != sb) {
          sb.append(name.charAt(i));
        }
        continue;
      }
      if (name.charAt(i) >= 'A' && name.charAt(i) <= 'Z') {
        if (null != sb) {
          sb.append(name.charAt(i));
        }
        continue;
      }
      if (name.charAt(i) >= '0' && name.charAt(i) <= '9') {
        if (null != sb) {
          sb.append(name.charAt(i));
        }
        continue;
      }
      if ('_' == name.charAt(i)) {
        if (null != sb) {
          sb.append(name.charAt(i));
        }
        continue;
      }
      if (null == sb) {
        sb = new StringBuilder(name.length());
        sb.append(name, 0, i);
      }
      // Replace forbidden char by '_'
      sb.append("_");
    }

    //
    // Check the first char
    //

    if (null == sb) {
      if (name.charAt(0) >= '0' && name.charAt(0) <= '9') {
        return "_" + name;
      } else {
        return name;
      }
    } else {
      if (sb.charAt(0) >= '0' && sb.charAt(0) <= '9') {
        return "_" + sb.toString();
      } else {
        return sb.toString();
      }
    }
  }

  private static String sanitizeLabelValue(String value) {
    //
    // Label values must have \n, \ and " escaped by \
    //

    value = value.replaceAll("\\\\", "\\\\\\\\");
    value = value.replaceAll("\n", "\\n");
    value = value.replaceAll("\"", "\\\"");

    return value;
  }
}
