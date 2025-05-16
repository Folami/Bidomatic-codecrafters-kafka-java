import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * ByteParser class for parsing binary data, similar to the Python implementation.
 */
public class ByteParser {
    private final byte[] data;
    private int index;
    private boolean finished;
    
    private static final int MSB_SET_MASK = 0b10000000;
    private static final int REMOVE_MSB_MASK = 0b01111111;

    public ByteParser(byte[] data) {
        this.data = data;
        this.index = 0;
        this.finished = false;
    }

    public boolean isFinished() {
        return finished;
    }

    public boolean eof() {
        return index >= data.length;
    }

    private void checkIsFinished() {
        finished = index >= data.length;
    }

    public byte[] read(int numBytes) {
        if (index + numBytes > data.length) {
            throw new IllegalArgumentException("Not enough bytes to read");
        }
        byte[] result = new byte[numBytes];
        System.arraycopy(data, index, result, 0, numBytes);
        return result;
    }

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

    public void skip(int numBytes) {
        if (index + numBytes > data.length) {
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
}
