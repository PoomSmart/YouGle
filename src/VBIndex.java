import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.LinkedList;
import java.util.List;
import java.util.Vector;

public class VBIndex implements BaseIndex {

	@Override
	public PostingList readPosting(FileChannel fc) {
		try {
			ByteBuffer buffer = ByteBuffer.allocate(1); // TODO: possible overkill
			Vector<Integer> numbers = new Vector<Integer>();
			int n = 0, docFreq = 0;
			while (fc.read(buffer) != -1) {
				buffer.rewind();
				Byte b = buffer.get();
				if ((b & 0xff) < 128) {
					n = 128 * n + b;
				} else {
					n = 128 * n + ((b & 0xff) - 128);
					numbers.add(n);
					n = 0;
					if (numbers.size() == 2)
						docFreq = numbers.lastElement();
					if (numbers.size() - 2 == docFreq)
						break;
				}
				buffer.clear();
			}
			if (!numbers.isEmpty()) {
				numbers.trimToSize();
				List<Integer> postings = docGap(numbers.subList(2, numbers.size()), true);
				return new PostingList(numbers.get(0), postings);
			}
			return null;
		} catch (IOException e) {
			return null;
		}

	}

	@Override
	public void writePosting(FileChannel fc, PostingList p) {
		List<Byte> encodedTermId = VBEncodeNumber(p.getTermId());
		List<Byte> encodedDocFreq = VBEncodeNumber(p.getList().size());
		List<Byte> encodedDocIds = VBEncode(docGap(p.getList(), false));
		ByteBuffer byteBuffer = ByteBuffer
				.allocate(encodedTermId.size() + encodedDocFreq.size() + encodedDocIds.size());
		for (Byte b : encodedTermId)
			byteBuffer.put(b);
		for (Byte b : encodedDocFreq)
			byteBuffer.put(b);
		for (Byte b : encodedDocIds)
			byteBuffer.put(b);
		byteBuffer.flip();
		try {
			fc.write(byteBuffer);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static List<Byte> VBEncode(List<Integer> numbers) {
		Vector<Byte> byteStream = new Vector<Byte>();
		for (Integer number : numbers) {
			List<Byte> bytes = VBEncodeNumber(number);
			byteStream.addAll(bytes);
		}
		byteStream.trimToSize();
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
	 * change in-order list to be gap list for example docIds 824 829 215406 gap 824 5 214577
	 * 
	 * @param postings
	 * @param inverse
	 * @return
	 */
	private static List<Integer> docGap(List<Integer> postings, boolean inverse) {
		if (inverse) {
			for (int i = 1; i < postings.size(); i++)
				postings.set(i, postings.get(i) + postings.get(i - 1));
		} else {
			for (int i = postings.size() - 1; i > 0; i--)
				postings.set(i, postings.get(i) - postings.get(i - 1));
		}
		return postings;
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
		while (i++ < 3) {
			List<Integer> l = new Vector<Integer>();
			l.add(824 + i * 100);
			l.add(829 + i * 9000);
			l.add(215406 + i * 4);
			PostingList p = new PostingList(i, l);
			index.writePosting(fc, p);
		}

		fc.position(0);
		PostingList posting = null;
		while ((posting = index.readPosting(fc)) != null) {
			System.out.println("Term id: " + posting.getTermId());
			System.out.println("List: " + posting.getList());
		}

		raf.close();
	}
}
