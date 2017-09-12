import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
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
		System.out.println(intStream);
		List<Byte> byteStream = VBEncode(intStream); // encode intStream into byteStream via VBEncode algorithm
		System.out.println(byteStream);
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
		List<Byte> byteStream = new ArrayList<Byte>();
		for(Integer number:numbers) {
			List<Byte> bytes = VBEncodeNumber(number);
			byteStream.addAll(bytes);
		}
		return byteStream;
	}
	
	private static List<Byte> VBEncodeNumber(int number) {
		LinkedList<Byte> bytes = new LinkedList<Byte>();
		while(true) {
			bytes.addFirst((byte)(number % 128));
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
		list.addElement(postings.get(0));
		for(int i = 1; i < postings.size(); i++)
			list.addElement(postings.get(i) - postings.get(i-1));
		return list;
	}
	
	/**
	 * To test the correctness of writePosting
	 * 
	 * @param args
	 * @throws FileNotFoundException 
	 */
	public static void main(String[] args) throws FileNotFoundException {
		File file = new File("./index/small/corpus2.index");
		RandomAccessFile raf = new RandomAccessFile(file, "rw");
		VBIndex index = new VBIndex();
		List<Integer> l = new ArrayList<Integer>();
		l.add(824);
		l.add(829);
		l.add(215406);
		PostingList p = new PostingList(1, l);
		index.writePosting(raf.getChannel(), p);
	}
}
