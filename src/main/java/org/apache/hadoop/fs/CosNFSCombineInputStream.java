package org.apache.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
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


public class CosNFSCombineInputStream extends FSInputStream {
    public static final Logger LOG =
            LoggerFactory.getLogger(CosNFSCombineInputStream.class);

    private FileSystem.Statistics statistics;
    private final Configuration conf;
    private final NativeFileSystemStore store;
    private final String key;
    private long position = 0;
    private long nextPos = 0;
    private long lastByteStart = -1;
    private long fileSize;
    private long partRemaining;
    private long bufferStart;
    private long bufferEnd;
    private final long preReadPartSize;
    private final int maxReadPartNumber;
    private byte[] buffer;
    private byte[] directBuffer;
    private boolean closed = false;
    private boolean firstRead = true;
    private final int socketErrMaxRetryTimes;

    private final ExecutorService readAheadExecutorService;
    private final Queue<CosNFSInputStream.ReadBuffer> readBufferQueue;

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
    public CosNFSCombineInputStream(
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
        this.preReadPartSize = conf.getLong(
                CosNConfigKeys.READ_AHEAD_BLOCK_SIZE_KEY,
                CosNConfigKeys.DEFAULT_READ_AHEAD_BLOCK_SIZE);
        this.maxReadPartNumber = conf.getInt(
                CosNConfigKeys.READ_AHEAD_QUEUE_SIZE,
                CosNConfigKeys.DEFAULT_READ_AHEAD_QUEUE_SIZE);
        this.socketErrMaxRetryTimes = conf.getInt(
                CosNConfigKeys.CLIENT_SOCKET_ERROR_MAX_RETRIES,
                CosNConfigKeys.DEFAULT_CLIENT_SOCKET_ERROR_MAX_RETRIES);
        this.readAheadExecutorService = readAheadExecutorService;
        this.readBufferQueue =
                new ArrayDeque<>(this.maxReadPartNumber);
        this.closed = false;
    }

    private synchronized void reopen(long pos) throws IOException {
        long partSize = 0;

        if (pos < 0) {
            throw new EOFException(FSExceptionMessages.NEGATIVE_SEEK);
        } else if (pos > this.fileSize) {
            throw new EOFException(FSExceptionMessages.CANNOT_SEEK_PAST_EOF);
        } else {
            if (pos + this.preReadPartSize > this.fileSize) {
                partSize = this.fileSize - pos;
            } else {
                partSize = this.preReadPartSize;
            }
        }

        this.buffer = null;
        this.bufferStart = -1;
        this.bufferEnd = -1;

        boolean isRandomIO = true;
        if (pos == this.nextPos) {
            isRandomIO = false;
        } else {
            while (this.readBufferQueue.size() != 0) {
                if (this.readBufferQueue.element().getStart() != pos) {
                    this.readBufferQueue.poll();
                } else {
                    break;
                }
            }
        }

        this.nextPos = pos + partSize;

        int currentBufferQueueSize = this.readBufferQueue.size();
        if (currentBufferQueueSize == 0) {
            this.lastByteStart = pos - partSize;
        } else {
            CosNFSInputStream.ReadBuffer[] readBuffers =
                    this.readBufferQueue.toArray(new CosNFSInputStream.ReadBuffer[currentBufferQueueSize]);
            this.lastByteStart =
                    readBuffers[currentBufferQueueSize - 1].getStart();
        }

        int maxLen = this.maxReadPartNumber - currentBufferQueueSize;
        for (int i = 0; i < maxLen && i < (currentBufferQueueSize + 1) * 2; i++) {
            if (this.lastByteStart + partSize * (i + 1) > this.fileSize) {
                break;
            }

            long byteStart = this.lastByteStart + partSize * (i + 1);
            long byteEnd = byteStart + partSize - 1;
            if (byteEnd >= this.fileSize) {
                byteEnd = this.fileSize - 1;
            }

            CosNFSInputStream.ReadBuffer readBuffer =
                    new CosNFSInputStream.ReadBuffer(byteStart, byteEnd);
            if (readBuffer.getBuffer().length == 0) {
                readBuffer.setStatus(CosNFSInputStream.ReadBuffer.SUCCESS);
            } else {
                this.readAheadExecutorService.execute(
                        new CosNFileReadTask(this.conf, this.key, this.store,
                                readBuffer, this.socketErrMaxRetryTimes));
            }

            this.readBufferQueue.add(readBuffer);
            if (isRandomIO) {
                break;
            }
        }

        CosNFSInputStream.ReadBuffer readBuffer = this.readBufferQueue.poll();
        IOException innerException = null;
        readBuffer.lock();
        try {
            readBuffer.await(CosNFSInputStream.ReadBuffer.INIT);
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

        this.position = pos;
        this.partRemaining = partSize;
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
        if (pos >= this.bufferStart &&  pos <= this.bufferEnd) {
            LOG.debug("seek cache hit lastpos {}, pos {}, this buffer start {}, end {}",
                    this.position, pos, this.bufferStart, this.bufferEnd);
            this.position = pos;
            this.partRemaining = this.bufferEnd - pos + 1;
        } else {
            this.position = pos;
            this.partRemaining = -1;
        }
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

        long start = System.currentTimeMillis();

        if (len == 0) {
            return 0;
        }

        if (off < 0 || len < 0 || len > b.length) {
            throw new IndexOutOfBoundsException();
        }

        // judge whether first read or random read
        if (this.firstRead) {
            this.firstRead = false;
            return directRead(b, off, len);
        }

        if (this.position != this.nextPos) {
            // is random io, use the direct read way
            return directRead(b, off, len);
        }

        // sequence io, use the past read way
        int bytesRead = 0;
        while (position < fileSize && bytesRead < len) {
            if (partRemaining <= 0) {
                reopen(position);
            }

            int bytes = 0;
            for (int i = this.buffer.length - (int) partRemaining;
                 i < this.buffer.length; i++) {
                b[off + bytesRead] = this.buffer[i];
                bytes++;
                bytesRead++;
                if (off + bytesRead >= len) {
                    break;
                }
            }

            if (bytes > 0) {
                this.position += bytes;
                this.partRemaining -= bytes;
            } else if (this.partRemaining != 0) {
                throw new IOException("Failed to read from stream. Remaining:" +
                        " " + this.partRemaining);
            }
        }
        if (null != this.statistics && bytesRead > 0) {
            this.statistics.incrementBytesRead(bytesRead);
        }

        long costMs = (System.currentTimeMillis() - start);
        LOG.info("sequence read object [{}], pos [{}], offset [{}], len [{}], costMs [{}]",
                this.key, this.position, off, len, costMs);

        return bytesRead == 0 ? -1 : bytesRead;
    }

    private int directRead(byte []b, int off, int len) throws IOException {
        long start = System.currentTimeMillis();
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
                this.directBuffer = null;
                this.bufferStart = -1;
                this.bufferEnd = -1;
            } else {
                this.directBuffer = readBuffer.getBuffer();
                this.bufferStart = readBuffer.getStart();
                this.bufferEnd = readBuffer.getEnd();
            }

        } catch (InterruptedException e) {
            LOG.warn("interrupted exception occurs when wait a read buffer.");
        } finally {
            readBuffer.unLock();
        }

        if (null == this.directBuffer) {
            LOG.error(String.format("Null IO stream key:%s", this.key), innerException);
            throw new IOException("Null IO stream.", innerException);
        }

        int bytesRead = 0;
        while (position < fileSize && bytesRead < len) {
            int bytes = 0;
            for (int i = 0; i < this.directBuffer.length; i++) {
                b[off + bytesRead] = this.directBuffer[i];
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

        this.directBuffer = null;
        if (null != this.statistics && bytesRead > 0) {
            this.statistics.incrementBytesRead(bytesRead);
        }
        long costMs = (System.currentTimeMillis() - start);
        LOG.info("direct read object [{}], offset [{}], len [{}], costMs [{}], range start [{}], range end [{}]",
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
