/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.record;

import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * A byte buffer backed output outputStream
 */
public class ByteBufferOutputStream extends OutputStream {

    private static float REALLOCATION_FACTOR = 1.1f;

    private ByteBuffer buffer;

    public ByteBufferOutputStream(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    //写入一个byte到输出流中
    public void write(int b) {
        if (buffer.remaining() < 1)
            expandBuffer(buffer.capacity() + 1);
        buffer.put((byte) b);
    }

    //将字节数组bytes写入到输出流中,从数组的off开始写入,写入长度为len
    public void write(byte[] bytes, int off, int len) {
        if (buffer.remaining() < len)
            expandBuffer(buffer.capacity() + len);
        buffer.put(bytes, off, len);
    }

    public ByteBuffer buffer() {
        return buffer;
    }

    //扩容,线程不安全
    private void expandBuffer(int size) {
        int expandSize = Math.max((int) (buffer.capacity() * REALLOCATION_FACTOR), size);
        ByteBuffer temp = ByteBuffer.allocate(expandSize);//创建新的缓冲池
        temp.put(buffer.array(), buffer.arrayOffset(), buffer.position());//复制到新的缓冲池中
        buffer = temp;//替换缓冲池
    }
}
