import java.util.Arrays;

/**
 * ByteParser class for parsing binary data.
 * This class mimics the functionality of the Python ByteParser class.
 */
public class ByteParser {
    private static final int MSB_SET_MASK = 0b10000000;
    private static final int REMOVE_MSB_MASK = 0b01111111;

    private final byte[] data;
    private int index;
    private boolean finished;

    /**
     * Constructor for ByteParser.
     * @param data The byte array to parse.
     */
    public ByteParser(byte[] data) {
        this.data = data;
        this.index = 0;
        this.finished = false;
    }

    /**
     * Check if we've reached the end of the data.
     * @return true if we've reached the end of the data.
     */
    public boolean eof() {
        return this.index == this.data.length;
    }

    /**
     * Update the finished flag based on the current index.
     */
    public void checkIsFinished() {
        this.finished = this.index == this.data.length;
    }

    /**
     * Read bytes without advancing the index.
     * @param num_bytes The number of bytes to read.
     * @return The bytes read.
     */
    public byte[] read(int num_bytes) {
        if (this.index + num_bytes > this.data.length) {
            throw new IllegalArgumentException("Not enough bytes to read");
        }
        return Arrays.copyOfRange(this.data, this.index, this.index + num_bytes);
    }

    /**
     * Read bytes and advance the index.
     * @param num_bytes The number of bytes to read.
     * @return The bytes read.
     */
    public byte[] consume(int num_bytes) {
        if (this.index + num_bytes > this.data.length) {
            throw new IllegalArgumentException("Not enough bytes to read");
        }
        byte[] result = Arrays.copyOfRange(this.data, this.index, this.index + num_bytes);
        this.index += num_bytes;
        this.checkIsFinished();
        return result;
    }

    /**
     * Skip bytes by advancing the index.
     * @param num_bytes The number of bytes to skip.
     */
    public void skip(int num_bytes) {
        if (this.index + num_bytes > this.data.length) {
            throw new IllegalArgumentException("Not enough bytes to skip");
        }
        this.index += num_bytes;
        this.checkIsFinished();
    }

    /**
     * Get the number of remaining bytes.
     * @return The number of remaining bytes.
     */
    public int remaining() {
        return this.data.length - this.index;
    }

    /**
     * Reset the index to the start.
     */
    public void reset() {
        this.index = 0;
        this.finished = false;
    }

    /**
     * Read a variable-length integer.
     * @param signed Whether the integer is signed.
     * @return The integer value.
     */
    public int consumeVarInt(boolean signed) {
        int shift = 0;
        int value = 0;
        int aux = MSB_SET_MASK;
        int index = this.index;
        byte[] record = new byte[0]; // Not used in Java but mimics Python

        while ((aux & MSB_SET_MASK) != 0) {
            aux = this.data[index] & 0xFF;
            // In Python: record += aux.to_bytes() - not needed in Java
            value += (aux & REMOVE_MSB_MASK) << shift;
            index += 1;
            shift += 7;
        }

        if (signed) {
            int lsb = value & 0x01;
            if (lsb != 0) {
                value = -1 * ((value + 1) >> 1);
            } else {
                value = value >> 1;
            }
        }

        this.index = index;
        this.checkIsFinished();
        return value;
    }
}