
/*
 * Members (Section 1)
 * 1. Kittinun Aukkapinyo	5888006
 * 2. Chatchawan Kotarasu	5888084
 * 3. Thatchapon Unprasert	5888220
 */

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.Vector;

public class BasicIndex implements BaseIndex {

	public PostingList readPosting(FileChannel fc) {
		try {
			// We know in advance 8 bytes are of every posting (termId and document frequency as integers, 4 bytes each)
			ByteBuffer buffer = ByteBuffer.allocate(8);
			// If we can't read that, it means EOF and we have to stop
			if (fc.read(buffer) == -1)
				return null;
			buffer.rewind();
			// Get termId and document frequency, respectively
			int termId = buffer.getInt();
			int size = buffer.getInt();
			// At this point, we figure out document frequency, we then allocate in the amount that just fits all docIds
			buffer = ByteBuffer.allocate(size * 4);
			fc.read(buffer);
			buffer.rewind();
			List<Integer> docIds = new Vector<Integer>(size);
			while (size-- != 0)
				docIds.add(buffer.getInt());
			buffer.clear();
			buffer = null;
			return new PostingList(termId, docIds);
		} catch (IOException e) {
			return null;
		}
	}

	public void writePosting(FileChannel fc, PostingList p) {
		List<Integer> docIds = p.getList();
		int size = docIds.size();
		// Every posting is of size 8 + 4 * (document frequency)
		// Put all necessary values into the buffer then write to the file channel, then we are done
		ByteBuffer buffer = ByteBuffer.allocate(8 + size * 4);
		buffer.putInt(p.getTermId());
		buffer.putInt(size);
		for (int docId : docIds)
			buffer.putInt(docId);
		buffer.flip();
		try {
			fc.write(buffer);
			buffer.clear();
			buffer = null;
		} catch (IOException e) {
		}
	}
}
