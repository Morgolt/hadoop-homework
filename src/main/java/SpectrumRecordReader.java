import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Scanner;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

public class SpectrumRecordReader
    extends RecordReader<Text, RawSpectrum> {

  // line example: 2012 01 01 00 00   0.00   0.00   0.00   0.00   0.00   0.00   0.00   0.00   ...
  private static final Pattern LINE_PATTERN = Pattern.compile("([0-9]{4} [0-9]{2} [0-9]{2} [0-9]{2} [0-9]{2})(.*)");
  private ArrayList<HashMap<String, float[]>> data;
  private Iterator<String> dateIterator;
  private Path[] files;
  private Text key;
  private RawSpectrum value;

  private String[] readNextLines(Scanner[] scanners) {
    String[] lines = new String[scanners.length];
    for (int i = 0; i < lines.length; ++i) {
      if (scanners[i].hasNextLine()) {
        lines[i] = scanners[i].nextLine();
      } else {
        return null;
      }
    }
    return lines;
  }

  private Pair<String, float[]> parseLine(String line) {
    String dateTime = line.substring(0, 16);

    String[] tokens = line.substring(16).trim().split("\\s+");
    float[] frequences = new float[tokens.length];
    for (int j = 0; j < tokens.length; ++j) {
      frequences[j] = Float.parseFloat(tokens[j]);
    }

    return new Pair<>(dateTime, frequences);
  }

  @Override
  public void close() {

  }

  @Override
  public Text getCurrentKey() {
    return key;
  }

  @Override
  public RawSpectrum getCurrentValue() {
    return value;
  }

  @Override
  public float getProgress() {
    return 0;
  }

  @Override
  public void
  initialize(InputSplit split, TaskAttemptContext context)
      throws IOException {

    files = ((CombineFileSplit) split).getPaths();
    Scanner[] scanners = new Scanner[files.length];

    data = new ArrayList<>();
    for (Scanner scanner : scanners) {
      data.add(new HashMap<>());
    }

    for (int i = 0; i < scanners.length; ++i) {
      FileSystem fileSystem = files[i].getFileSystem(context.getConfiguration());
      FSDataInputStream inputStream = fileSystem.open(files[i]);
      InputStream gzipStream = new GZIPInputStream(inputStream);
      scanners[i] = new Scanner(gzipStream);
      if (scanners[i].hasNextLine()) {
        scanners[i].nextLine();
      }

      while (scanners[i].hasNextLine()) {
        String line = scanners[i].nextLine();

        Pair<String, float[]> pair = parseLine(line);
        String date = pair.getKey();
        float[] frequencies = pair.getValue();

        data.get(i).put(date, frequencies);
      }
    }

    dateIterator = data.get(0).keySet().iterator();
  }

  @Override
  public boolean nextKeyValue() {
    if (!dateIterator.hasNext()) {
      return false;
    }

    String curDateTime;
    boolean hasSameDateTime;

    do {
      hasSameDateTime = true;
      curDateTime = dateIterator.next();

      for (HashMap<String, float[]> aData : data) {
        if (!aData.containsKey(curDateTime)) {
          hasSameDateTime = false;
          break;
        }
      }
    } while (!hasSameDateTime && dateIterator.hasNext());

    if (!hasSameDateTime) {
      return false;
    }

    key = new Text(curDateTime);
    value = new RawSpectrum();
    for (int i = 0; i < data.size(); ++i) {
      value.setField(files[i].getName(), data.get(i).get(curDateTime));
    }

    return true;
  }

  class Pair<T1, T2> {

    private final T1 key;
    private final T2 value;

    public Pair(T1 key, T2 value) {
      this.key = key;
      this.value = value;
    }

    public T1 getKey() {
      return key;
    }

    public T2 getValue() {
      return value;
    }
  }

}
