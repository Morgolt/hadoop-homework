import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;


public class SpectrumRecordWriter
extends RecordWriter<Text,RawSpectrum>
{

	@Override
	public void	write(Text key, RawSpectrum value) {
        // TODO
	}

	@Override
	public void close(TaskAttemptContext context) {
        // TODO
	}

}
