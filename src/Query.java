
/*
 * Members (Section 1)
 * 1. Kittinun Aukkapinyo	5888006
 * 2. Chatchawan Kotarasu	5888084
 * 3. Thatchapon Unprasert	5888220
 */

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
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
		// here, we obtain term position from posDict for seeking by FileChannel.position(long) that also returns
		// FileChannel object reference to be used in the standard readPosting() of such BaseIndex indexer
		return index.readPosting(fc.position(posDict.get(termId)));
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
		String[] tokens = query.split("\\s+"); // split the query into tokens (terms)
		List<PostingList> postings = new Vector<PostingList>(); // we have the list of posting list for all proper terms
		Integer termId;
		for (String token : tokens) {
			// It is possible that any term in the query does not exist in termDict (or our index file)
			// We can conclude without hesitation that none of documents contain this particular term, and in effect, no
			// document results to be printed
			if ((termId = termDict.get(token)) == null)
				return null;
			// otherwise, read the posting using the termId we have from above and add that to the "posting list" list
			postings.add(readPosting(indexFile.getChannel(), termId));
		}
		// For better posting lists intersection performance, we sort the list first by increasing document frequency,
		// also known as the size of a posting
		Collections.sort(postings, new Comparator<PostingList>() {
			@Override
			public int compare(PostingList p1, PostingList p2) {
				return p1.getList().size() - p2.getList().size();
			}
		});
		// Now we intersect from the beginning posting lists we have
		// We can even abort the operation whenever the intersection result is empty
		List<Integer> list = postings.get(0).getList();
		for (int i = 1; i < tokens.length; i++) {
			if ((list = intersection(list, postings.get(i).getList())).isEmpty())
				return null;
		}
		return list;
	}

	/**
	 * Intersect two document ID lists
	 * 
	 * @param list
	 * @param next
	 * @return
	 */
	public static List<Integer> intersection(List<Integer> list, List<Integer> next) {
		/*
		 * Similar to merging algorithm, we have a list to hold the docIds result. But we don't always add an element to
		 * the list
		 */
		Vector<Integer> newList = new Vector<Integer>();
		Iterator<Integer> iterA = list.iterator();
		Iterator<Integer> iterB = next.iterator();
		Integer elementA = iterA.next();
		Integer elementB = iterB.next();
		while (elementA != null && elementB != null) {
			if (elementA == elementB) {
				newList.add(elementA); // Added to the result only if this element occurs on both list
				elementA = iterA.hasNext() ? iterA.next() : null;
				elementB = iterB.hasNext() ? iterB.next() : null;
			} else if (elementA < elementB)
				elementA = iterA.hasNext() ? iterA.next() : null; // Increment the list A
			else
				elementB = iterB.hasNext() ? iterB.next() : null; // Increment the list B
		}
		return newList;
	}

	String outputQueryResult(List<Integer> res) {
		// This is when none of documents is matched with the query, thus no results found
		if (res == null || res.isEmpty())
			return "no results found";
		// Now the easy part, iterate the docId list to get document names via docDict, add them to the docName list
		// Sort that resulting list in lexicon orders and join all the elements to the answer string using newline
		// character
		List<String> fileNames = new Vector<String>(res.size());
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
