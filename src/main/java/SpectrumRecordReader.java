import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.util.regex.Pattern;

public class SpectrumRecordReader extends RecordReader<Text,RawSpectrum>
{

	// line example: 2012 01 01 00 00   0.00   0.00   0.00   0.00   0.00   0.00   0.00   0.00   ...
	private static final Pattern
	LINE_PATTERN = Pattern.compile("([0-9]{4} [0-9]{2} [0-9]{2} [0-9]{2} [0-9]{2})(.*)");

	@Override
	public void close() {
        // TODO
	}

	@Override
	public Text getCurrentKey() {
        return null;
	}

	@Override
	public RawSpectrum getCurrentValue() {
        return null;
	}

	@Override
	public float getProgress() {
        return 0.0f;
	}

	@Override
	public void
	initialize(InputSplit split, TaskAttemptContext context) {

	}

	@Override
	public boolean nextKeyValue() {
        return false;
	}

}
