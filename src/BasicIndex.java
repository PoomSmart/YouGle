import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.Vector;

public class BasicIndex implements BaseIndex {

	@Override
	public PostingList readPosting(FileChannel fc) {
		try {
			ByteBuffer buffer = ByteBuffer.allocate(4);
			if (fc.read(buffer) == -1)
				return null;
			buffer.rewind();
			int termId = buffer.getInt();
			buffer.clear();
			fc.read(buffer);
			buffer.rewind();
			int size = buffer.getInt();
			buffer = ByteBuffer.allocate(size * 4);
			fc.read(buffer);
			buffer.rewind();
			List<Integer> docIds = new Vector<Integer>();
			for (int i = 0; i < size; i++)
				docIds.add(buffer.getInt());
			return new PostingList(termId, docIds);
		} catch (IOException e) {
			return null;
		}
	}

	@Override
	public void writePosting(FileChannel fc, PostingList p) {
		List<Integer> docIds = p.getList();
		int size = docIds.size();
		ByteBuffer buffer = ByteBuffer.allocate(8 + size * 4);
		buffer.putInt(p.getTermId());
		buffer.putInt(size);
		for (int docId : docIds)
			buffer.putInt(docId);
		buffer.flip();
		try {
			fc.write(buffer);
		} catch (IOException e) {
			e.printStackTrace();
		}
		buffer = null;
	}
}
