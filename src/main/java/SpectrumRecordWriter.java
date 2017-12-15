import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import ucar.ma2.ArrayFloat.D2;
import ucar.ma2.DataType;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.Dimension;
import ucar.nc2.NetcdfFileWriter;
import ucar.nc2.Variable;


enum Variables {
  I("i", 0),
  J("j", 0),
  K("k", 0),
  W("w", 0),
  D("d", 0);

  private final String name;
  private int offset;

  Variables(String name, int offset) {
    this.name = name;
    this.offset = offset;
  }

  public int getOffset() {
    return offset;
  }

  public void incrementOffset() {
    this.offset += 1;
  }

  public String getName() {
    return name;
  }
}

public class SpectrumRecordWriter extends RecordWriter<Text, RawSpectrum> {

  private static final String FILENAME_PATTERN = "/gfs/home/zuenok.chris/output-1/%s.ncd";

  private Map<String, NetcdfFileWriterState> fileStates = new HashMap<>();

  @Override
  public void write(Text key, RawSpectrum value) {
    String station = key.toString();

    if (fileStates.containsKey(station)) {
      NetcdfFileWriterState fileState = fileStates.get(station);

      // no loop to explicitly specify the variables written
      fileState.writeVariable(value.getI(), Variables.I);
      fileState.writeVariable(value.getJ(), Variables.J);
      fileState.writeVariable(value.getK(), Variables.K);
      fileState.writeVariable(value.getW(), Variables.W);
      fileState.writeVariable(value.getD(), Variables.D);
    } else {
      String filename = String.format(FILENAME_PATTERN, station);
      fileStates.put(filename, new NetcdfFileWriterState(filename, value));
    }
  }

  @Override
  public void close(TaskAttemptContext context) {
    try {
      for (Map.Entry<String, NetcdfFileWriterState> entry : fileStates.entrySet()) {
        entry.getValue().writer.close();
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  static class NetcdfFileWriterState {

    private static final String TIME_DIMENSION = "time";
    private static final String FREQUENCY_DIMENSION = "frequency";
    private static final String VARIABLE_DIMENSIONS = TIME_DIMENSION + " " + FREQUENCY_DIMENSION;

    NetcdfFileWriter writer;
    Dimension time;
    Dimension frequency;
    Map<Variables, Variable> names;

    NetcdfFileWriterState(
        String filename,
        RawSpectrum spectrum) {
      try {
        this.writer = NetcdfFileWriter.createNew(NetcdfFileWriter.Version.netcdf3, filename);
        this.time = writer.addUnlimitedDimension(TIME_DIMENSION);
        this.frequency = writer.addDimension(
            null,
            FREQUENCY_DIMENSION,
            spectrum.getD().length);

        // initial names
        this.names = new HashMap<>();
        for (Variables variable : Variables.values()) {
          names.put(
              variable,
              writer.addVariable(null, variable.getName(), DataType.FLOAT, VARIABLE_DIMENSIONS)
          );
        }
        writer.create();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

    }

    void writeVariable(float[] param, Variables variable) {
      D2 data = new D2(1, param.length);
      for (int i = 0; i < param.length; i++) {
        data.set(0, i, param[i]);
      }
      int[] origin = new int[]{variable.getOffset(), 0};
      try {
        writer.write(names.get(variable), origin, data);
        variable.incrementOffset();
      } catch (IOException | InvalidRangeException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
