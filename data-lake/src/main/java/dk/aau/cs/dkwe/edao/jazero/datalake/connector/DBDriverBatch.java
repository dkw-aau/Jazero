package dk.aau.cs.dkwe.edao.jazero.datalake.connector;

import dk.aau.cs.dkwe.edao.jazero.datalake.connector.DBDriver;

import java.util.List;
import java.util.Map;

public interface DBDriverBatch<R, Q> extends DBDriver<R, Q>
{
    boolean batchInsert(List<String> iris, List<List<Float>> vectors);
    Map<String, List<Double>> batchSelect(List<String> iris);
    boolean clear();
}
