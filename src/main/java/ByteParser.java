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
        return index >= data.length;
    }

    /**
     * Update the finished flag based on the current index.
     */
    private void checkIsFinished() {
        this.finished = index >= data.length;
    }

    /**
     * Read bytes without advancing the index.
     * @param numBytes The number of bytes to read.
     * @return The bytes read.
     */
    public byte[] read(int numBytes) {
        if (index + numBytes > data.length) {
            throw new IllegalArgumentException("Not enough bytes to read");
        }
        byte[] result = new byte[numBytes];
        System.arraycopy(data, index, result, 0, numBytes);
        return result;
    }

    /**
     * Read bytes and advance the index.
     * @param numBytes The number of bytes to read.
     * @return The bytes read.
     */
    public byte[] consume(int numBytes) {
        if (index + numBytes > data.length) {
            throw new IllegalArgumentException("Not enough bytes to read");
        }
        byte[] result = new byte[numBytes];
        System.arraycopy(data, index, result, 0, numBytes);
        index += numBytes;
        checkIsFinished();
        return result;
    }

    /**
     * Skip bytes by advancing the index.
     * @param numBytes The number of bytes to skip.
     */
    public void skip(int numBytes) {
        if (index + numBytes > data.length) {
            throw new IllegalArgumentException("Not enough bytes to skip");
        }
        index += numBytes;
        checkIsFinished();
    }

    /**
     * Get the number of remaining bytes.
     * @return The number of remaining bytes.
     */
    public int remaining() {
        return data.length - index;
    }

    /**
     * Reset the index to the start.
     */
    public void reset() {
        index = 0;
        finished = false;
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
        int currentIndex = index;
        while ((aux & MSB_SET_MASK) != 0) {
            if (currentIndex >= data.length) {
                throw new IllegalArgumentException("Not enough bytes to read variable int");
            }

            aux = data[currentIndex] & 0xFF;
            value += (aux & REMOVE_MSB_MASK) << shift;
            currentIndex++;
            shift += 7;
        }

        index = currentIndex;
        checkIsFinished();

        if (signed) {
            int lsb = value & 0x01;
            if (lsb != 0) {
                value = -1 * ((value + 1) >> 1);
            } else {
                value = value >> 1;
            }
        }

        return value;
    }

    /**
     * Check if parsing is finished.
     * @return true if parsing is finished.
     */
    public boolean isFinished() {
        return finished;
    }
}
