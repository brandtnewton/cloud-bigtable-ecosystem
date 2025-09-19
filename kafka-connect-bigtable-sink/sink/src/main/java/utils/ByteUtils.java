/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Copyright 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package utils;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;

/**
 * This code is copied from hbase-common using Apache 2.0 License. This code has been modified to
 * remove functions that we don't need.
 *
 * @see <a
 *     href="https://github.com/apache/hbase/blob/master/hbase-common/src/main/java/org/apache/hadoop/hbase/util/Bytes.java">Bytes.java</a>
 */
public class ByteUtils {

  private static final String UTF8_CSN;

  static {
    UTF8_CSN = StandardCharsets.UTF_8.name();
  }

  public static byte[] toBytes(double d) {
    return toBytes(Double.doubleToRawLongBits(d));
  }

  public static byte[] toBytes(float f) {
    return toBytes(Float.floatToRawIntBits(f));
  }

  public static byte[] toBytes(int val) {
    byte[] b = new byte[4];

    for (int i = 3; i > 0; --i) {
      b[i] = (byte) val;
      val >>>= 8;
    }

    b[0] = (byte) val;
    return b;
  }

  public static byte[] toBytes(short val) {
    byte[] b = new byte[]{0, (byte) val};
    val = (short) (val >> 8);
    b[0] = (byte) val;
    return b;
  }

  public static byte[] toBytes(BigDecimal val) {
    byte[] valueBytes = val.unscaledValue().toByteArray();
    byte[] result = new byte[valueBytes.length + 4];
    int offset = putInt(result, 0, val.scale());
    System.arraycopy(valueBytes, 0, result, offset, valueBytes.length);
    return result;
  }

  public static int putInt(byte[] bytes, int offset, int val) {
    if (bytes.length - offset < 4) {
      throw new IllegalArgumentException(
          "Not enough room to put an int at offset " + offset + " in a " + bytes.length
              + " byte array");
    } else {
      for (int i = offset + 3; i > offset; --i) {
        bytes[i] = (byte) val;
        val >>>= 8;
      }

      bytes[offset] = (byte) val;
      return offset + 4;
    }
  }

  public static byte[] toBytes(String s) {
    try {
      return s.getBytes(UTF8_CSN);
    } catch (UnsupportedEncodingException var2) {
      throw new IllegalArgumentException("UTF8 decoding is not supported", var2);
    }
  }

  public static byte[] toBytes(boolean b) {
    return new byte[]{(byte) (b ? -1 : 0)};
  }

  public static byte[] toBytes(long val) {
    byte[] b = new byte[8];

    for (int i = 7; i > 0; --i) {
      b[i] = (byte) ((int) val);
      val >>>= 8;
    }

    b[0] = (byte) ((int) val);
    return b;
  }
}
