import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SpectrumOutputFormat extends FileOutputFormat<Text, RawSpectrum> {

  private static RecordWriter<Text, RawSpectrum> recordWriter;

  @Override
  public RecordWriter<Text, RawSpectrum> getRecordWriter(TaskAttemptContext context) {
    if (recordWriter == null) {
      recordWriter = new SpectrumRecordWriter();
    }
    return recordWriter;
  }

}
