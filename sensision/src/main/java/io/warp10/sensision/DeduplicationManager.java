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

import java.io.UnsupportedEncodingException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

/**
 * This class maintains a cache of recently seen values and timestamps.
 * It attempts its best to deduplicate values, but as metrics might be
 * input out of order, there might be times when a value is considered
 * a duplicate even though it's not.
 * 
 * This should be a very rare situation though.
 * 
 * Shit happens....
 */
public class DeduplicationManager {
  
  /**
   * Maximum size of dedeuplication cache
   */
  public static final String DEDUP_MAXSIZE = "sensision.dedup.maxsize";
  
  /**
   * Maximum age of cached values
   */
  public static final String DEDUP_MAXAGE = "sensision.dedup.maxage";
  
  /**
   * Map of className{labels} to last location/elevation/value (their fingerprint)
   */
  Map<Long,Long> valueCache = new LinkedHashMap<Long,Long>() {
    protected boolean removeEldestEntry(Map.Entry<Long,Long> eldest) {
      return size() > maxsize;      
    };
  };
  
  /**
   * Map of className{labels} to most recent timestamp
   */
  Map<Long,Long> tsCache = new LinkedHashMap<Long,Long>() {
    protected boolean removeEldestEntry(Map.Entry<Long,Long> eldest) {
      return size() > maxsize;
    };
  };
  
  /**
   * Maximum number of cached values/ts
   */
  private final int maxsize;
  
  /**
   * Maximum age in microsecond of a cached value 
   */
  private final long maxage;
  
  public DeduplicationManager(String queue,Properties config) {
    this.maxsize = Integer.valueOf(config.getProperty(DEDUP_MAXSIZE + "." + queue, "0"));
    this.maxage = Long.valueOf(config.getProperty(DEDUP_MAXAGE + "." + queue, "0"));
  }
  
  public boolean isDuplicate(String metric) {
    
    //
    // If size is '0' or maxage is '0', all metrics are new ones
    //
    
    if (0 == this.maxsize || 0L == this.maxage) {
      return false;
    }
    
    //
    // Extract location,elevation,timestamp,value,class{labels}
    //
    
    int slash = metric.indexOf("/");
    // wsp is the index of the start of the class name
    int wsp = metric.indexOf(" ");
    // owsp is the index of the start of the value
    int owsp = metric.indexOf(" ", wsp + 1);
    
    // Could not find some separators, assume it's not a duplicate
    if (-1 == slash || -1 == wsp || -1 == owsp) {
      return false;
    }
    
    long ts = Long.valueOf(metric.substring(0, slash));
    String clsLabels = metric.substring(wsp + 1, owsp);
    long clsLabelsHash = 0L;
    
    try {
      hash24(42L,42L, clsLabels.getBytes("UTF-8"));
    } catch (UnsupportedEncodingException uee) {
      // Can't happen, we're using UTF-8.
    }
    
    // 'value' is really location+elevation+value
    String value = metric.substring(slash + 1, wsp) + "/" + metric.substring(owsp + 1);
        
    // Replace value with its SipHash
    long hash = 0L;
    
    try {
      hash = hash24(42L,42L,value.getBytes("UTF-8"));
    } catch (UnsupportedEncodingException uee) {
      // Can't happen, we're using UTF-8...
    }
    
    if (!valueCache.containsKey(clsLabelsHash) || !tsCache.containsKey(clsLabelsHash)) {
      // This key was not knwown for either value or timestamp
      // store value/ts and return false
      valueCache.put(clsLabelsHash, hash);
      tsCache.put(clsLabelsHash, ts);
      return false;
    } else if (ts < tsCache.get(clsLabelsHash)) {
      // Timestamp we just saw is earlier than latest ts we recorded, assume it's
      // not a duplicate, but don't update cache with that value
      return false;      
    } else if (hash == valueCache.get(clsLabelsHash)) {
      //
      // Value hash is identical to the previous one, update cache so eviction is coherent between
      // the two maps
      //
      
      valueCache.put(clsLabelsHash, hash);
      long prevts = tsCache.put(clsLabelsHash, tsCache.get(clsLabelsHash));
      
      //
      // Consider it's a duplicate only if ts is less than 'heartbeat' microseconds from previous ts
      //
      
      if (ts - prevts <= this.maxage) {
        return true;
      } else {
        tsCache.put(clsLabelsHash, ts);
        return false;
      }
    } else {
      valueCache.put(clsLabelsHash, hash);
      tsCache.put(clsLabelsHash, ts);
      return false;
    }
  }
  
  /**
   * SipHash-2-4 implementation
   * Adapted from https://github.com/hbs/siphash-java-inline/tree/hbs/negative-byte-values
   */
  private static long hash24(long k0, long k1, byte[] data) {
    long v0 = 0x736f6d6570736575L ^ k0;
    long v1 = 0x646f72616e646f6dL ^ k1;
    long v2 = 0x6c7967656e657261L ^ k0;
    long v3 = 0x7465646279746573L ^ k1;
    long m;
    int last = data.length / 8 * 8;
    
    int i = 0;

    // processing 8 bytes blocks in data
    while (i < last) {
      // pack a block to long, as LE 8 bytes
      m = (data[i++] & 0xffL) | (data[i++] & 0xffL) << 8 | (data[i++] & 0xffL) << 16
          | (data[i++] & 0xffL) << 24 | (data[i++] & 0xffL) << 32
          | (data[i++] & 0xffL) << 40 | (data[i++] & 0xffL) << 48
          | (data[i++] & 0xffL) << 56;
      // MSGROUND {
      v3 ^= m;

      /*
       * SIPROUND wih hand reordering
       * 
       * SIPROUND in siphash24.c:
       * A: v0 += v1;
       * B: v1=ROTL(v1,13);
       * C: v1 ^= v0;
       * D: v0=ROTL(v0,32);
       * E: v2 += v3;
       * F: v3=ROTL(v3,16);
       * G: v3 ^= v2;
       * H: v0 += v3;
       * I: v3=ROTL(v3,21);
       * J: v3 ^= v0;
       * K: v2 += v1;
       * L: v1=ROTL(v1,17);
       * M: v1 ^= v2;
       * N: v2=ROTL(v2,32);
       * 
       * Each dependency:
       * B -> A
       * C -> A, B
       * D -> C
       * F -> E
       * G -> E, F
       * H -> D, G
       * I -> H
       * J -> H, I
       * K -> C, G
       * L -> K
       * M -> K, L
       * N -> M
       * 
       * Dependency graph:
       * D -> C -> B -> A
       * G -> F -> E
       * J -> I -> H -> D, G
       * N -> M -> L -> K -> C, G
       * 
       * Resulting parallel friendly execution order:
       * -> ABCDHIJ
       * -> EFGKLMN
       */

      // SIPROUND {
      v0 += v1;
      v2 += v3;
      v1 = (v1 << 13) | v1 >>> 51;
      v3 = (v3 << 16) | v3 >>> 48;
      v1 ^= v0;
      v3 ^= v2;
      v0 = (v0 << 32) | v0 >>> 32;
      v2 += v1;
      v0 += v3;
      v1 = (v1 << 17) | v1 >>> 47;
      v3 = (v3 << 21) | v3 >>> 43;
      v1 ^= v2;
      v3 ^= v0;
      v2 = (v2 << 32) | v2 >>> 32;
      // }
      // SIPROUND {
      v0 += v1;
      v2 += v3;
      v1 = (v1 << 13) | v1 >>> 51;
      v3 = (v3 << 16) | v3 >>> 48;
      v1 ^= v0;
      v3 ^= v2;
      v0 = (v0 << 32) | v0 >>> 32;
      v2 += v1;
      v0 += v3;
      v1 = (v1 << 17) | v1 >>> 47;
      v3 = (v3 << 21) | v3 >>> 43;
      v1 ^= v2;
      v3 ^= v0;
      v2 = (v2 << 32) | v2 >>> 32;
      // }
      v0 ^= m;
      // }
    }

    // packing the last block to long, as LE 0-7 bytes + the length in the top
    // byte
    m = 0;
    for (i = data.length - 1; i >= last; --i) {
      m <<= 8;
      m |= data[i] & 0xffL;
    }
    m |= (long) data.length << 56;
    
    // MSGROUND {
    v3 ^= m;
    // SIPROUND {
    v0 += v1;
    v2 += v3;
    v1 = (v1 << 13) | v1 >>> 51;
    v3 = (v3 << 16) | v3 >>> 48;
    v1 ^= v0;
    v3 ^= v2;
    v0 = (v0 << 32) | v0 >>> 32;
    v2 += v1;
    v0 += v3;
    v1 = (v1 << 17) | v1 >>> 47;
    v3 = (v3 << 21) | v3 >>> 43;
    v1 ^= v2;
    v3 ^= v0;
    v2 = (v2 << 32) | v2 >>> 32;
    // }
    // SIPROUND {
    v0 += v1;
    v2 += v3;
    v1 = (v1 << 13) | v1 >>> 51;
    v3 = (v3 << 16) | v3 >>> 48;
    v1 ^= v0;
    v3 ^= v2;
    v0 = (v0 << 32) | v0 >>> 32;
    v2 += v1;
    v0 += v3;
    v1 = (v1 << 17) | v1 >>> 47;
    v3 = (v3 << 21) | v3 >>> 43;
    v1 ^= v2;
    v3 ^= v0;
    v2 = (v2 << 32) | v2 >>> 32;
    // }
    v0 ^= m;
    // }

    // finishing...
    v2 ^= 0xff;
    // SIPROUND {
    v0 += v1;
    v2 += v3;
    v1 = (v1 << 13) | v1 >>> 51;
    v3 = (v3 << 16) | v3 >>> 48;
    v1 ^= v0;
    v3 ^= v2;
    v0 = (v0 << 32) | v0 >>> 32;
    v2 += v1;
    v0 += v3;
    v1 = (v1 << 17) | v1 >>> 47;
    v3 = (v3 << 21) | v3 >>> 43;
    v1 ^= v2;
    v3 ^= v0;
    v2 = (v2 << 32) | v2 >>> 32;
    // }
    // SIPROUND {
    v0 += v1;
    v2 += v3;
    v1 = (v1 << 13) | v1 >>> 51;
    v3 = (v3 << 16) | v3 >>> 48;
    v1 ^= v0;
    v3 ^= v2;
    v0 = (v0 << 32) | v0 >>> 32;
    v2 += v1;
    v0 += v3;
    v1 = (v1 << 17) | v1 >>> 47;
    v3 = (v3 << 21) | v3 >>> 43;
    v1 ^= v2;
    v3 ^= v0;
    v2 = (v2 << 32) | v2 >>> 32;
    // }
    // SIPROUND {
    v0 += v1;
    v2 += v3;
    v1 = (v1 << 13) | v1 >>> 51;
    v3 = (v3 << 16) | v3 >>> 48;
    v1 ^= v0;
    v3 ^= v2;
    v0 = (v0 << 32) | v0 >>> 32;
    v2 += v1;
    v0 += v3;
    v1 = (v1 << 17) | v1 >>> 47;
    v3 = (v3 << 21) | v3 >>> 43;
    v1 ^= v2;
    v3 ^= v0;
    v2 = (v2 << 32) | v2 >>> 32;
    // }
    // SIPROUND {
    v0 += v1;
    v2 += v3;
    v1 = (v1 << 13) | v1 >>> 51;
    v3 = (v3 << 16) | v3 >>> 48;
    v1 ^= v0;
    v3 ^= v2;
    v0 = (v0 << 32) | v0 >>> 32;
    v2 += v1;
    v0 += v3;
    v1 = (v1 << 17) | v1 >>> 47;
    v3 = (v3 << 21) | v3 >>> 43;
    v1 ^= v2;
    v3 ^= v0;
    v2 = (v2 << 32) | v2 >>> 32;
    // }
    return v0 ^ v1 ^ v2 ^ v3;
  }

}
