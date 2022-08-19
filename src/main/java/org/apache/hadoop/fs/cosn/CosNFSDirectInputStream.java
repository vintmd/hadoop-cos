package org.apache.hadoop.fs.cosn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class CosNFSDirectInputStream extends FSInputStream{
    public static final Logger LOG =
            LoggerFactory.getLogger(CosNFSDirectInputStream.class);

    // read buffer direct used the CosNFSInputStream

    private FileSystem.Statistics statistics;
    private final Configuration conf;
    private final NativeFileSystemStore store;
    private final String key;
    private long position = 0;
    private long nextPos = 0;
    private long lastByteStart = -1;
    private long partRemaining;
    private long fileSize;
    private long bufferStart;
    private long bufferEnd;
    private byte[] buffer;
    private boolean closed = false;
    private final int socketErrMaxRetryTimes;

    private final ExecutorService readAheadExecutorService;

    /**
     * Input Stream
     *
     * @param conf config
     * @param store native file system
     * @param statistics statis
     * @param key cos key
     * @param fileSize file size
     * @param readAheadExecutorService thread executor
     */
    public CosNFSDirectInputStream(
            Configuration conf,
            NativeFileSystemStore store,
            FileSystem.Statistics statistics,
            String key,
            long fileSize,
            ExecutorService readAheadExecutorService) {
        super();
        this.conf = conf;
        this.store = store;
        this.statistics = statistics;
        this.key = key;
        this.fileSize = fileSize;
        this.bufferStart = -1;
        this.bufferEnd = -1;

        this.socketErrMaxRetryTimes = conf.getInt(
                CosNConfigKeys.CLIENT_SOCKET_ERROR_MAX_RETRIES,
                CosNConfigKeys.DEFAULT_CLIENT_SOCKET_ERROR_MAX_RETRIES);
        this.readAheadExecutorService = readAheadExecutorService;
        this.closed = false;
    }

    private synchronized void reopen(long pos) throws IOException {
    }

    @Override
    public void seek(long pos) throws IOException {
        this.checkOpened();

        if (pos < 0) {
            throw new EOFException(FSExceptionMessages.NEGATIVE_SEEK);
        }
        if (pos > this.fileSize) {
            throw new EOFException(FSExceptionMessages.CANNOT_SEEK_PAST_EOF);
        }

        if (this.position == pos) {
            return;
        }

        this.position = pos;
    }

    @Override
    public long getPos() throws IOException {
        return this.position;
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
        return false;
    }

    @Override
    public int read() throws IOException {
        byte[] oneByteBuff = new byte[1];
        int ret = read(oneByteBuff, 0, 1);

        if (this.statistics != null) {
            this.statistics.incrementBytesRead(1);
        }

        return (ret <= 0) ? -1 : (oneByteBuff[0] & 0xff);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        this.checkOpened();
        // LOG.info("begin read object [{}], offset [{}], len [{}], pos [{}]", this.key, off, len, this.position);
        long start = System.currentTimeMillis();
        if (len == 0) {
            return 0;
        }

        if (off < 0 || len < 0 || len > b.length) {
            throw new IndexOutOfBoundsException();
        }

        long byteStart = this.position + off;
        long byteEnd = this.position + off + len-1;
        if (byteEnd >= this.fileSize) {
            byteEnd = this.fileSize - 1;
        }

        CosNFSInputStream.ReadBuffer readBuffer = new CosNFSInputStream.ReadBuffer(byteStart, byteEnd);
        if (readBuffer.getBuffer().length == 0) {
            readBuffer.setStatus(CosNFSInputStream.ReadBuffer.SUCCESS);
        } else {
            this.readAheadExecutorService.execute(
                    new CosNFileReadTask(this.conf, this.key, this.store,
                            readBuffer, this.socketErrMaxRetryTimes));
        }

        IOException innerException = null;

        // LOG.info("push read object task [{}], offset [{}], len [{}], range start [{}], range end [{}]",
          //      this.key, off, len, readBuffer.getStart(), readBuffer.getEnd());
        readBuffer.lock();
        //LOG.info("get lock of read object task [{}], offset [{}], len [{}]", this.key, off, len);
        try {
            readBuffer.await(CosNFSInputStream.ReadBuffer.INIT);
            // LOG.info("after await read object task [{}], offset [{}], len [{}]", this.key, off, len);
            if (readBuffer.getStatus() == CosNFSInputStream.ReadBuffer.ERROR) {
                innerException = readBuffer.getException();
                this.buffer = null;
                this.bufferStart = -1;
                this.bufferEnd = -1;
            } else {
                this.buffer = readBuffer.getBuffer();
                this.bufferStart = readBuffer.getStart();
                this.bufferEnd = readBuffer.getEnd();
            }

        } catch (InterruptedException e) {
            LOG.warn("interrupted exception occurs when wait a read buffer.");
        } finally {
            readBuffer.unLock();
        }

        if (null == this.buffer) {
            LOG.error(String.format("Null IO stream key:%s", this.key), innerException);
            throw new IOException("Null IO stream.", innerException);
        }

        int bytesRead = 0;
        while (position < fileSize && bytesRead < len) {
            int bytes = 0;
            for (int i = 0; i < this.buffer.length; i++) {
                b[off + bytesRead] = this.buffer[i];
                bytes++;
                bytesRead++;
                if (off + bytesRead >= len) {
                    break;
                }
            }

            if (bytes > 0) {
                this.position += bytes;
            }
        }

        this.buffer = null;
        if (null != this.statistics && bytesRead > 0) {
            this.statistics.incrementBytesRead(bytesRead);
        }
        long costMs = (System.currentTimeMillis() - start);
        LOG.info("read object [{}], offset [{}], len [{}], costMs [{}], range start [{}], range end [{}]",
                this.key, off, len, costMs, readBuffer.getStart(), readBuffer.getEnd());

        return bytesRead == 0 ? -1 : bytesRead;
    }


    @Override
    public int available() throws IOException {
        this.checkOpened();

        long remaining = this.fileSize - this.position;
        if(remaining > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        }

        return (int) remaining;
    }

    @Override
    public void close() throws IOException {
        if (this.closed) {
            return;
        }

        this.closed = true;
        this.buffer = null;
    }

    private void checkOpened() throws IOException {
        if(this.closed) {
            throw new IOException(FSExceptionMessages.STREAM_IS_CLOSED);
        }
    }
}
