import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;


public class BasicIndex implements BaseIndex {

	@Override
	public PostingList readPosting(FileChannel fc) {
		/*
		 * TODO: Your code here
		 *       Read and return the postings list from the given file.
		 */
		ByteBuffer buffer = ByteBuffer.allocate(8);
		PostingList postingList = null;
		try {
			if(fc.read(buffer) == -1) return null;
			buffer.clear();
			int termId = buffer.getInt();
			int docFreq = buffer.getInt();
			List<Integer> postings = new ArrayList<Integer>(docFreq);
			buffer = ByteBuffer.allocate(docFreq * 4);
			fc.read(buffer);
			buffer.clear();
			for(int i = 0; i < docFreq; i++) {
				postings.add(buffer.getInt());
			}
			postingList = new PostingList(termId, postings);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return postingList;
	}

	@Override
	public void writePosting(FileChannel fc, PostingList posting) {
		/*
		 * TODO: Your code here
		 *       Write the given postings list to the given file.
		 */
		List<Integer> docIds = posting.getList();
		int size = docIds.size();
		ByteBuffer buffer = ByteBuffer.allocate(8 + size * 4);
		buffer.putInt(posting.getTermId());
		buffer.putInt(size);
		for (int docId : docIds)
			buffer.putInt(docId);
		buffer.flip();
		try {
			fc.write(buffer);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		buffer = null;
	}
}

