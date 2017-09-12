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
	private static List<Integer> VBDecode(List<Byte> byteStream) {
		LinkedList<Integer> numbers = new LinkedList<Integer>();
		int n = 0;
		for (Byte b : byteStream) {
			if ((b & 0xff) < 128) {
				n = 128 * n + b;
			} else {
				n = 128 * n + ((b & 0xff) - 128);
				numbers.addLast(n);
				n = 0;
			}
		}
		return numbers;
	}

	@Override
	public PostingList readPosting(FileChannel fc) {
		// TODO Auto-generated method stub
		try {
			ByteBuffer buffer = ByteBuffer.allocate(1);
			LinkedList<Integer> numbers = new LinkedList<Integer>();
			List<Integer> postings = new ArrayList<Integer>();
			int n = 0;
			int size = 0;
			while (fc.read(buffer) >= 0) {
				buffer.flip();
				Byte b = buffer.get();
				if ((b & 0xff) < 128) {
					n = 128 * n + b;
				} else {
					n = 128 * n + ((b & 0xff) - 128);
					numbers.addLast(n);
					n = 0;
					if (numbers.size() == 2) {
						size = numbers.getLast(); // doc freq
					}
					if (numbers.size() - 2 == size)
						break;
				}
				buffer.clear();
			}
			postings = inverseDocGap(numbers.subList(2, numbers.size()));
			return new PostingList(numbers.getFirst(), postings);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			return null;
		}
		
	}

	@Override
	public void writePosting(FileChannel fc, PostingList p) {
		// TODO Auto-generated method stub
		List<Integer> intStream = new Vector<Integer>();
		int termId = p.getTermId();
		List<Integer> postings = p.getList();
		intStream.add(termId); // add first termId
		intStream.add(postings.size());
		intStream.addAll(docGap(postings)); // extend intStream with postings in term of doc-gap
		List<Byte> byteStream = VBEncode(intStream); // encode intStream into byteStream via VBEncode algorithm
		/*
		 * Write byteStream into fc
		 */

		ByteBuffer byteBuffer = ByteBuffer.allocate(byteStream.size());
		byteBuffer.clear();
		for (Byte b : byteStream)
			byteBuffer.put(b);
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
		for (Integer number : numbers) {
			List<Byte> bytes = VBEncodeNumber(number);
			byteStream.addAll(bytes);
		}
		return byteStream;
	}

	private static List<Byte> VBEncodeNumber(int number) {
		LinkedList<Byte> bytes = new LinkedList<Byte>();
		while (true) {
			bytes.addFirst((byte) (number % 128));
			if (number < 128)
				break;
			number /= 128;
		}
		byte b = (byte) (bytes.getLast() + (byte) 128);
		bytes.removeLast();
		bytes.addLast(b);
		return bytes;
	}

	/**
	 * change in-order list to be gap list for example docIds 824 829 215406 gap 824
	 * 5 214577
	 * 
	 * @param postings
	 * @return list
	 */
	private static List<Integer> docGap(List<Integer> postings) {
		Vector<Integer> list = new Vector<Integer>();
		list.addElement(postings.get(0));
		for (int i = 1; i < postings.size(); i++)
			list.addElement(postings.get(i) - postings.get(i - 1));
		return list;
	}

	private static List<Integer> inverseDocGap(List<Integer> docGap) {
		Vector<Integer> list = new Vector<Integer>();
		list.addElement(docGap.get(0));
		for (int i = 1; i < docGap.size(); i++)
			list.addElement(docGap.get(i) + list.get(i - 1));
		return list;
	}

	/**
	 * To test the correctness of writePosting
	 * 
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		File file = new File("./index/small/corpus2.index");
		file.delete();
		file = new File("./index/small/corpus2.index");
		RandomAccessFile raf = new RandomAccessFile(file, "rw");
		FileChannel fc = raf.getChannel();
		VBIndex index = new VBIndex();
		int i = 0;
		while(i++ < 3) {
			List<Integer> l = new ArrayList<Integer>();
			l.add(824 + i*100);
			l.add(829 + i*9000);
			l.add(215406 + i*4);
			PostingList p = new PostingList(i, l);
			index.writePosting(fc, p);
		}
		
		fc.position(0);
		PostingList posting = null;
		while((posting = index.readPosting(fc)) != null) {
			System.out.println("Term id: " + posting.getTermId());
			System.out.println("List: " + posting.getList());
		}
		
		raf.close();
	}
}
