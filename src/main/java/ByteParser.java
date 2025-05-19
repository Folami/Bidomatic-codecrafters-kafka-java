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

    /**
     * Checks if the end of the data has been reached.
     */
    public boolean eof() {
        return index == data.length;
    }

    /**
     * Updates the finished flag based on the current index.
     */
    private void checkIsFinished() {
        finished = index == data.length;
    }

    /**
     * Reads numBytes from the current position without advancing the index.
     */
    public byte[] read(int numBytes) {
        if (index + numBytes > data.length) {
            throw new IllegalArgumentException("Not enough bytes to read: need " + numBytes + ", available " + (data.length - index));
        }
        return Arrays.copyOfRange(data, index, index + numBytes);
    }

    /**
     * Consumes numBytes from the current position, advancing the index.
     */
    public byte[] consume(int numBytes) {
        if (index + numBytes > data.length) {
            throw new IllegalArgumentException("Not enough bytes to consume: need " + numBytes + ", available " + (data.length - index));
        }
        byte[] result = Arrays.copyOfRange(data, index, index + numBytes);
        index += numBytes;
        checkIsFinished();
        return result;
    }

    /**
     * Skips numBytes in the stream by advancing the index.
     */
    public void skip(int numBytes) {
        if (index + numBytes > data.length) {
            throw new IllegalArgumentException("Not enough bytes to skip: need " + numBytes + ", available " + (data.length - index));
        }
        index += numBytes;
        checkIsFinished();
    }

    /**
     * Returns the number of remaining bytes.
     */
    public int remaining() {
        return data.length - index;
    }

    /**
     * Resets the index to the start.
     */
    public void reset() {
        index = 0;
        finished = false;
    }

    /**
     * Consumes a variable-length integer (VARINT), optionally signed.
     */
    public int consumeVarInt(boolean signed) {
        int shift = 0;
        int value = 0;
        int aux = MSB_SET_MASK;
        int startIndex = index;

        while ((aux & MSB_SET_MASK) != 0) {
            if (index >= data.length) {
                throw new IllegalArgumentException("Not enough bytes to read VARINT at index " + startIndex);
            }
            aux = data[index] & 0xFF;
            value += (aux & REMOVE_MSB_MASK) << shift;
            index++;
            shift += 7;
            if (shift > 35) { // Prevent overflow (5 bytes max for 32-bit int)
                throw new IllegalArgumentException("VARINT too long at index " + startIndex);
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

    /**
     * Consumes a 4-byte integer.
     */
    public int consumeInt() {
        byte[] bytes = consume(4);
        return ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).getInt();
    }
}