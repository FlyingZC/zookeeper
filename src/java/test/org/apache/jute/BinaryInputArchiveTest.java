/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jute;

import junit.framework.Assert;
import org.junit.Test;

import java.io.*;
import java.util.Set;
import java.util.TreeMap;


public class BinaryInputArchiveTest {

    @Test
    public void testReadStringCheckLength() {
        byte[] buf = new byte[]{
                Byte.MAX_VALUE, Byte.MAX_VALUE, Byte.MAX_VALUE, Byte.MAX_VALUE};
        ByteArrayInputStream is = new ByteArrayInputStream(buf);
        BinaryInputArchive ia = BinaryInputArchive.getArchive(is);
        try {
            ia.readString("");
            Assert.fail("Should have thrown an IOException");
        } catch (IOException e) {
            Assert.assertTrue("Not 'Unreasonable length' exception: " + e,
                    e.getMessage().startsWith(BinaryInputArchive.UNREASONBLE_LENGTH));
        }
    }

    public static void main( String[] args ) throws IOException {
        String path = "D:\\test.txt";

        // write operation
        OutputStream outputStream = new FileOutputStream(new File(path));
        BinaryOutputArchive binaryOutputArchive = BinaryOutputArchive.getArchive(outputStream);

        binaryOutputArchive.writeBool(true, "boolean");
        
        byte[] bytes = "zc".getBytes();
        binaryOutputArchive.writeBuffer(bytes, "buffer");
        
        binaryOutputArchive.writeDouble(13.14, "double");
        binaryOutputArchive.writeFloat(5.20f, "float");
        binaryOutputArchive.writeInt(520, "int");
        
        Person person = new Person(25, "zc");
        binaryOutputArchive.writeRecord(person, "zc");

        TreeMap<String, Integer> map = new TreeMap<String, Integer>();
        map.put("zc", 25);
        map.put("dyd", 25);
        Set<String> keys = map.keySet();
        binaryOutputArchive.startMap(map, "map");
        int i = 0;
        for (String key: keys) {
            String tag = i + "";
            binaryOutputArchive.writeString(key, tag);
            binaryOutputArchive.writeInt(map.get(key), tag);
            i++;
        }

        binaryOutputArchive.endMap(map, "map");

        // read operation
        InputStream inputStream = new FileInputStream(new File(path));
        BinaryInputArchive binaryInputArchive = BinaryInputArchive.getArchive(inputStream);

        System.out.println(binaryInputArchive.readBool("boolean"));
        System.out.println(new String(binaryInputArchive.readBuffer("buffer")));
        System.out.println(binaryInputArchive.readDouble("double"));
        System.out.println(binaryInputArchive.readFloat("float"));
        System.out.println(binaryInputArchive.readInt("int"));
        Person person2 = new Person();
        binaryInputArchive.readRecord(person2, "zc");
        System.out.println(person2);

        Index index = binaryInputArchive.startMap("map");
        int j = 0;
        while (!index.done()) {
            String tag = j + "";
            System.out.println("key = " + binaryInputArchive.readString(tag)
                    + ", value = " + binaryInputArchive.readInt(tag));
            index.incr();
            j++;
        }
    }

    static class Person implements Record {
        private int age;
        private String name;

        public Person() {

        }

        public Person(int age, String name) {
            this.age = age;
            this.name = name;
        }

        public void serialize(OutputArchive archive, String tag) throws IOException {
            archive.startRecord(this, tag);
            archive.writeInt(age, "age");
            archive.writeString(name, "name");
            archive.endRecord(this, tag);
        }

        public void deserialize(InputArchive archive, String tag) throws IOException {
            archive.startRecord(tag);
            age = archive.readInt("age");
            name = archive.readString("name");
            archive.endRecord(tag);
        }

        public String toString() {
            return "age = " + age + ", name = " + name;
        }
    }
}
