// package codecrafters_kafka_java; // Uncomment if using package

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * ByteParser class for parsing binary data, mimicking parser.py's ByteParser.
 */
public class ByteParser {
    private final byte[] data;
    private int index;
    private boolean finished;

    // Constants for VARINT parsing
    private static final int MSB_SET_MASK = 0x80;
    private static final int REMOVE_MSB_MASK = 0x7F;

    public ByteParser(byte[] data) {
        this.data = data;
        this.index = 0;
        this.finished = false;
    }

    public boolean eof() {
        return index == data.length;
    }

    private void checkIsFinished() {
        finished = index == data.length;
    }

    public byte[] read(int numBytes) {
        if (index + numBytes > data.length) {
            System.err.println("Not enough bytes to read: need " + numBytes + ", available " + (data.length - index));
            throw new IllegalArgumentException("Not enough bytes to read");
        }
        return Arrays.copyOfRange(data, index, index + numBytes);
    }

    public byte[] consume(int numBytes) {
        if (index + numBytes > data.length) {
            System.err.println("Not enough bytes to consume: need " + numBytes + ", available " + (data.length - index));
            throw new IllegalArgumentException("Not enough bytes to consume");
        }
        byte[] result = Arrays.copyOfRange(data, index, index + numBytes);
        index += numBytes;
        checkIsFinished();
        return result;
    }

    public void skip(int numBytes) {
        if (index + numBytes > data.length) {
            System.err.println("Not enough bytes to skip: need " + numBytes + ", available " + (data.length - index));
            throw new IllegalArgumentException("Not enough bytes to skip");
        }
        index += numBytes;
        checkIsFinished();
    }

    public int remaining() {
        return data.length - index;
    }

    public void reset() {
        index = 0;
        finished = false;
    }

    public int consumeVarInt(boolean signed) {
        int shift = 0;
        int value = 0;
        int aux = MSB_SET_MASK;
        int startIndex = index;

        while ((aux & MSB_SET_MASK) != 0) {
            if (index >= data.length) {
                System.err.println("Not enough bytes to read VARINT at index " + startIndex);
                throw new IllegalArgumentException("Not enough bytes to read VARINT");
            }
            aux = data[index] & 0xFF;
            value += (aux & REMOVE_MSB_MASK) << shift;
            index++;
            shift += 7;
            if (shift > 35) {
                System.err.println("VARINT too long at index " + startIndex);
                throw new IllegalArgumentException("VARINT too long");
            }
        }

        if (signed) {
            int lsb = value & 0x01;
            if (lsb != 0) {
                value = -1 * ((value + 1) >> 1);
            } else {
                value = value >> 1;
            }
        }

        checkIsFinished();
        return value;
    }

    public int consumeInt() {
        byte[] bytes = consume(4);
        return ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).getInt();
    }
}