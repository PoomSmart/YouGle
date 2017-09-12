import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.LinkedList;
import java.util.List;
import java.util.Vector;

public class VBIndex implements BaseIndex {
	
	@Override
	public PostingList readPosting(FileChannel fc) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void writePosting(FileChannel fc, PostingList p) {
		// TODO Auto-generated method stub
		List<Integer> intStream = new Vector<Integer>();
		intStream.add(p.getTermId()); // add first termId
		intStream.addAll(docGap(p.getList())); // extend intStream with postings in term of doc-gap
		List<Byte> byteStream = VBEncode(intStream); // encode intStream into byteStream via VBEncode algorithm
		
		/*
		 * Write byteStream into fc
		 */
		
		ByteBuffer byteBuffer = ByteBuffer.allocate(byteStream.size());
		byteBuffer.clear();
		for(Byte b : byteStream) {
			byteBuffer.put(b);
		}
		byteBuffer.flip();
		try {
			fc.write(byteBuffer);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private static List<Byte> VBEncode(List<Integer> numbers) {
		List<Byte> byteStream = new Vector<Byte>();
		for(Integer number:numbers) {
			List<Byte> bytes = VBEncodeNumber(number);
			byteStream.addAll(bytes);
		}
		return byteStream;
	}
	
	private static List<Byte> VBEncodeNumber(int number) {
		LinkedList<Byte> bytes = new LinkedList<Byte>();
		while(true) {
			bytes.addFirst((byte) (number % 128));
			if(number < 128) break;
			number /= 128;
		}
		byte b = (byte) (bytes.getLast() + (byte) 128);
		bytes.removeLast();
		bytes.addLast(b);
		return bytes;
	}
	
	/**
	 * change in-order list to be gap list
	 * for example
	 * docIds 824 829 215406
	 * gap 824 5 214577
	 * 
	 * @param postings
	 * @return list
	 */
	private static List<Integer> docGap(List<Integer> postings) {
		Vector<Integer> list = new Vector<Integer>();
		for(Integer i : postings) {
			if(list.isEmpty()) list.add(i);
			list.add(i - list.lastElement());
		}
		return list;
	}
}
