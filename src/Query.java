
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Vector;

public class Query {

	// Term id -> position in index file
	private Map<Integer, Long> posDict = new TreeMap<Integer, Long>();
	// Term id -> document frequency
	private Map<Integer, Integer> freqDict = new TreeMap<Integer, Integer>();
	// Doc id -> doc name dictionary
	private Map<Integer, String> docDict = new TreeMap<Integer, String>();
	// Term -> term id dictionary
	private Map<String, Integer> termDict = new TreeMap<String, Integer>();
	// Index
	private BaseIndex index = null;

	// indicate whether the query service is running or not
	private boolean running = false;
	private RandomAccessFile indexFile = null;

	/*
	 * Read a posting list with a given termID from the file You should seek to the file position of this specific
	 * posting list and read it back.
	 */
	private PostingList readPosting(FileChannel fc, int termId) throws IOException {
		long position = posDict.get(termId);
		int size = freqDict.get(termId);
		ByteBuffer buffer = ByteBuffer.allocate(size * 4);
		fc.read(buffer, position + 8);
		buffer.rewind();
		List<Integer> docIds = new ArrayList<Integer>();
		while (size-- != 0)
			docIds.add(buffer.getInt());
		return new PostingList(termId, docIds);
	}

	public void runQueryService(String indexMode, String indexDirname) throws IOException {
		// Get the index reader
		try {
			Class<?> indexClass = Class.forName(indexMode + "Index");
			index = (BaseIndex) indexClass.newInstance();
		} catch (Exception e) {
			System.err.println("Index method must be \"Basic\", \"VB\", or \"Gamma\"");
			throw new RuntimeException(e);
		}

		// Get Index file
		File inputdir = new File(indexDirname);
		if (!inputdir.exists() || !inputdir.isDirectory()) {
			System.err.println("Invalid index directory: " + indexDirname);
			return;
		}

		/* Index file */
		indexFile = new RandomAccessFile(new File(indexDirname, "corpus.index"), "r");

		String line = null;
		/* Term dictionary */
		BufferedReader termReader = new BufferedReader(new FileReader(new File(indexDirname, "term.dict")));
		while ((line = termReader.readLine()) != null) {
			String[] tokens = line.split("\t");
			termDict.put(tokens[0], Integer.parseInt(tokens[1]));
		}
		termReader.close();

		/* Doc dictionary */
		BufferedReader docReader = new BufferedReader(new FileReader(new File(indexDirname, "doc.dict")));
		while ((line = docReader.readLine()) != null) {
			String[] tokens = line.split("\t");
			docDict.put(Integer.parseInt(tokens[1]), tokens[0]);
		}
		docReader.close();

		/* Posting dictionary */
		BufferedReader postReader = new BufferedReader(new FileReader(new File(indexDirname, "posting.dict")));
		while ((line = postReader.readLine()) != null) {
			String[] tokens = line.split("\t");
			posDict.put(Integer.parseInt(tokens[0]), Long.parseLong(tokens[1]));
			freqDict.put(Integer.parseInt(tokens[0]), Integer.parseInt(tokens[2]));
		}
		postReader.close();

		this.running = true;
	}

	public List<Integer> retrieve(String query) throws IOException {
		if (!running) {
			System.err.println("Error: Query service must be initiated");
		}
		String[] tokens = query.trim().split("\\s+");
		Arrays.sort(tokens, new Comparator<String>() {
			@Override
		    public int compare(String t1, String t2) {
		        return freqDict.get(termDict.get(t1)) - freqDict.get(termDict.get(t2));
		    }
		});
		int termId = termDict.getOrDefault(tokens[0], -1);
		if (termId == -1)
			return null;
		PostingList prevList = readPosting(indexFile.getChannel(), termId);
		PostingList currList = prevList;
		List<Integer> list = prevList.getList();
		for (int i = 1; i < tokens.length; i++) {
			if ((termId = termDict.getOrDefault(tokens[i], -1)) == -1)
				return null;
			currList = readPosting(indexFile.getChannel(), termId);
			if ((list = intersect(list, currList.getList())).isEmpty())
				return null;
		}
		return list;
	}

	public static List<Integer> intersect(List<Integer> list, List<Integer> next) {
		List<Integer> newList = new Vector<Integer>();
		Iterator<Integer> iterA = list.iterator();
		Iterator<Integer> iterB = next.iterator();
		Integer elementA = iterA.next();
		Integer elementB = iterB.next();
		while (elementA != null && elementB != null) {
			if (elementA == elementB) {
				newList.add(elementA);
				elementA = iterA.hasNext() ? iterA.next() : null;
				elementB = iterB.hasNext() ? iterB.next() : null;
			} else if (elementA < elementB) {
				elementA = iterA.hasNext() ? iterA.next() : null;
			} else {
				elementB = iterB.hasNext() ? iterB.next() : null;
			}
		}
		if (elementA == elementB && elementA != null && elementB != null)
			newList.add(elementA);
		return newList;
	}

	String outputQueryResult(List<Integer> res) {
		if (res == null || res.isEmpty())
			return "no results found";
		List<String> fileNames = new Vector<String>();
		for (Integer docId : res) {
			String fileName = docDict.get(docId);
			fileNames.add(fileName);
		}
		Collections.sort(fileNames);
		return String.join("\n", fileNames) + "\n";
	}

	public static void main(String[] args) throws IOException {
		/* Parse command line */
		if (args.length != 2) {
			System.err.println("Usage: java Query [Basic|VB|Gamma] index_dir");
			return;
		}

		/* Get index */
		String className = null;
		try {
			className = args[0];
		} catch (Exception e) {
			System.err.println("Index method must be \"Basic\", \"VB\", or \"Gamma\"");
			throw new RuntimeException(e);
		}

		/* Get index directory */
		String input = args[1];

		Query queryService = new Query();
		queryService.runQueryService(className, input);

		/* Processing queries */
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

		/* For each query */
		String line = null;
		while ((line = br.readLine()) != null) {
			List<Integer> hitDocs = queryService.retrieve(line);
			queryService.outputQueryResult(hitDocs);
		}

		br.close();
	}

	protected void finalize() {
		try {
			if (indexFile != null)
				indexFile.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
