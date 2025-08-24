package dk.aau.cs.dkwe.edao.jazero.datalake.parser;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import dk.aau.cs.dkwe.edao.jazero.datalake.structures.table.DynamicTable;
import dk.aau.cs.dkwe.edao.jazero.datalake.structures.table.Table;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class TableParser
{
    public static Table<String> toTable(List<List<String>> matrix)
    {
        return new DynamicTable<>(matrix);
    }

    public static Table<String> parse(File file)
    {
        try (FileReader reader = new FileReader(file))
        {
            CSVReader csvReader = new CSVReader(reader);
            String[] record, columnLabels = null;
            List<List<String>> rows = new ArrayList<>();
            boolean isHeader = true;

            while ((record = csvReader.readNext()) != null)
            {
                if (isHeader)
                {
                    isHeader = false;
                    columnLabels = record;
                }

                rows.add(List.of(record));
            }

            if (!isHeader)
            {
                return new DynamicTable<>(file.getName(), rows, columnLabels);
            }

            return new DynamicTable<>(file.getName(), rows);
        }

        catch (IOException | CsvValidationException e)
        {
            return null;
        }
    }
}
