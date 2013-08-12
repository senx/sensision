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

import io.warp10.sensision.Sensision;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

public class SensisionTest {
  @Test
  public void testGetValue() {
    String cls = "class";
    Map<String,String> labels = new HashMap<String,String>();
    
    Assert.assertNull(Sensision.getValue(cls, labels));
    
    Sensision.set(cls + ".boolean", labels, true);
    Assert.assertEquals(true, Sensision.getValue(cls + ".boolean", labels));
    
    long nano = System.nanoTime();
    Sensision.set(cls + ".long", labels, nano);
    Assert.assertEquals(nano, Sensision.getValue(cls + ".long", labels));
    Sensision.update(cls + ".long", labels, 42);
    Assert.assertEquals(42 + nano, Sensision.getValue(cls + ".long", labels));
    
    double d = Math.random();
    Sensision.set(cls + ".double", labels, d);
    Assert.assertEquals(d, (Double) Sensision.getValue(cls + ".double", labels), 0.00000000001D);
    Sensision.update(cls + ".double", labels, 0.42);
    Assert.assertEquals(d + 0.42D, (Double) Sensision.getValue(cls + ".double", labels), 0.00000000001D);
    
    Sensision.set(cls + ".string", labels, "foo");
    Assert.assertEquals("foo", Sensision.getValue(cls + ".string", labels));
  }
  
  @Test
  public void testParseVallue() {
    Object o = Sensision.parseValue("10.1111111111111E8");
    o = Sensision.parseValue("10.111111111E-2");
	  
	  Assert.assertTrue(o instanceof Double);
  }  
 }
