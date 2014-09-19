/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.db.commitlog;

import java.io.DataInput;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ParametrizedClass;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.PureJavaCrc32;
import org.json.simple.JSONValue;

import com.google.common.base.Objects;

public class CommitLogDescriptor
{
    private static final String SEPARATOR = "-";
    private static final String FILENAME_PREFIX = "CommitLog" + SEPARATOR;
    private static final String FILENAME_EXTENSION = ".log";
    // match both legacy and new version of commitlogs Ex: CommitLog-12345.log and CommitLog-4-12345.log.
    private static final Pattern COMMIT_LOG_FILE_PATTERN = Pattern.compile(FILENAME_PREFIX + "((\\d+)(" + SEPARATOR + "\\d+)?)" + FILENAME_EXTENSION);

    public static final int VERSION_12 = 2;
    public static final int VERSION_20 = 3;
    public static final int VERSION_21 = 4;
    public static final int VERSION_22 = 5;
    /**
     * Increment this number if there is a changes in the commit log disc layout or MessagingVersion changes.
     * Note: make sure to handle {@link #getMessagingVersion()}
     */
    public static final int current_version = VERSION_22;

    final int version;
    public final long id;
    public final ParametrizedClass compression;

    public CommitLogDescriptor(int version, long id, ParametrizedClass compression)
    {
        this.version = version;
        this.id = id;
        this.compression = compression;
    }

    public CommitLogDescriptor(long id)
    {
        this(current_version, id, DatabaseDescriptor.getCommitLogCompression());
    }

    public static void writeHeader(ByteBuffer out, CommitLogDescriptor descriptor)
    {
        PureJavaCrc32 crc = new PureJavaCrc32();
        out.putInt(descriptor.version);
        crc.updateInt(descriptor.version);
        out.putLong(descriptor.id);
        crc.updateInt((int) (descriptor.id & 0xFFFFFFFFL));
        crc.updateInt((int) (descriptor.id >>> 32));
        String compressionString = constructCompressionString(descriptor.compression);
        if (descriptor.version >= VERSION_22) {
            byte[] compressionBytes = compressionString.getBytes(StandardCharsets.UTF_8);
            out.putShort((short) compressionBytes.length);
            crc.updateInt(compressionBytes.length);
            out.put(compressionBytes);
            crc.update(compressionBytes, 0, compressionBytes.length);
        }
        out.putInt(crc.getCrc());
    }

    private static String constructCompressionString(ParametrizedClass compression)
    {
        if (compression == null)
            return "";
        String params = "";
        if (compression.parameters != null)
            params = JSONValue.toJSONString(compression.parameters);
        return compression.class_name + params;
    }

    public static CommitLogDescriptor fromHeader(File file)
    {
        try (RandomAccessFile raf = new RandomAccessFile(file, "r"))
        {
            assert raf.getFilePointer() == 0;
            return readHeader(raf);
        }
        catch (EOFException e)
        {
            throw new RuntimeException(e);
        }
        catch (IOException e)
        {
            throw new FSReadError(e, file);
        }
    }

    public static CommitLogDescriptor readHeader(DataInput input) throws IOException
    {
        PureJavaCrc32 checkcrc = new PureJavaCrc32();
        int version = input.readInt();
        checkcrc.updateInt(version);
        long id = input.readLong();
        checkcrc.updateInt((int) (id & 0xFFFFFFFFL));
        checkcrc.updateInt((int) (id >>> 32));
        int compressionLength = 0;
        if (version >= VERSION_22) {
            compressionLength = input.readShort() & 0xFFFF;
            checkcrc.updateInt(compressionLength);
        }
        // This should always succeed as compressionLength cannot be too long even for a
        // corrupt segment file.
        byte[] compressionBytes = new byte[compressionLength];
        input.readFully(compressionBytes);
        checkcrc.update(compressionBytes, 0, compressionBytes.length);
        int crc = input.readInt();
        if (crc == checkcrc.getCrc())
            return new CommitLogDescriptor(version, id,
                    parseCompressionString(new String(compressionBytes, StandardCharsets.UTF_8)));
        return null;
    }

    @SuppressWarnings("unchecked")
    private static ParametrizedClass parseCompressionString(String string)
    {
        if (string.isEmpty())
            return null;
        int split = string.indexOf('{');
        if (split < 0) {
            return new ParametrizedClass(string, null);
        }
        String className = string.substring(0, split);
        String optionsString = string.substring(split);
        return new ParametrizedClass(className, (Map<String, String>) JSONValue.parse(optionsString));
    }

    public static CommitLogDescriptor fromFileName(String name)
    {
        Matcher matcher;
        if (!(matcher = COMMIT_LOG_FILE_PATTERN.matcher(name)).matches())
            throw new RuntimeException("Cannot parse the version of the file: " + name);

        if (matcher.group(3) == null)
            throw new UnsupportedOperationException("Commitlog segment is too old to open; upgrade to 1.2.5+ first");

        long id = Long.parseLong(matcher.group(3).split(SEPARATOR)[1]);
        return new CommitLogDescriptor(Integer.parseInt(matcher.group(2)), id, null);
    }

    public int getMessagingVersion()
    {
        switch (version)
        {
            case VERSION_12:
                return MessagingService.VERSION_12;
            case VERSION_20:
                return MessagingService.VERSION_20;
            case VERSION_21:
                return MessagingService.VERSION_21;
            case VERSION_22:
                return MessagingService.VERSION_21;
            default:
                throw new IllegalStateException("Unknown commitlog version " + version);
        }
    }

    public String fileName()
    {
        return FILENAME_PREFIX + version + SEPARATOR + id + FILENAME_EXTENSION;
    }

    /**
     * @param   filename  the filename to check
     * @return true if filename could be a commit log based on it's filename
     */
    public static boolean isValid(String filename)
    {
        return COMMIT_LOG_FILE_PATTERN.matcher(filename).matches();
    }

    public String toString()
    {
        return "(" + version + "," + id + (compression != null ? "," + compression : "") + ")";
    }

    public boolean equals(Object that)
    {
        return that instanceof CommitLogDescriptor && equals((CommitLogDescriptor) that);
    }

    public boolean equalsIgnoringCompression(CommitLogDescriptor that)
    {
        return this.version == that.version && this.id == that.id;
    }

    public boolean equals(CommitLogDescriptor that)
    {
        return equalsIgnoringCompression(that) && Objects.equal(this.compression, that.compression);
    }

}
