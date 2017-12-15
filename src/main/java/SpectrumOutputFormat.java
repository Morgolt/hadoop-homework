import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class SpectrumOutputFormat
extends FileOutputFormat<Text,RawSpectrum>
{
	private static RecordWriter<Text, RawSpectrum> recordWriter;

	@Override
	public RecordWriter<Text,RawSpectrum> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException
	{
        if (recordWriter == null) {
            recordWriter = new SpectrumRecordWriter();
        }
        return recordWriter;
	}

}
