package dk.aau.cs.dkwe.edao.jazero.datalake.search;

import dk.aau.cs.dkwe.edao.jazero.datalake.structures.table.Table;

import java.io.File;

public interface Ranker
{
    Double score(Table<String> query, File table);
}
