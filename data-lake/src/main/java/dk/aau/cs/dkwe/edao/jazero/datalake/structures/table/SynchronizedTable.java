package dk.aau.cs.dkwe.edao.jazero.datalake.structures.table;

import java.util.List;

public class SynchronizedTable<T> extends SimpleTable<T> implements Table<T>
{
    public SynchronizedTable(String ... columnLabels)
    {
        super(columnLabels);
    }

    public SynchronizedTable(List<List<T>> table, String ... columnLabels)
    {
        super(table, columnLabels);
    }

    public SynchronizedTable(String id, String ... columnLabels)
    {
        super(id, columnLabels);
    }

    public SynchronizedTable(String id, List<List<T>> table, String ... columnLabels)
    {
        super(id, table, columnLabels);
    }

    @Override
    public String getId()
    {
        return getId();
    }

    @Override
    public synchronized Row<T> getRow(int index)
    {
        return super.getRow(index);
    }

    @Override
    public synchronized Column<T> getColumn(int index)
    {
        return super.getColumn(index);
    }

    @Override
    public synchronized Column<T> getColumn(String label)
    {
        return super.getColumn(label);
    }

    @Override
    public synchronized String[] getColumnLabels()
    {
        return super.getColumnLabels();
    }

    @Override
    public synchronized void addRow(Row<T> row)
    {
        super.addRow(row);
    }

    @Override
    public synchronized int rowCount()
    {
        return super.rowCount();
    }

    // Does not need to be synchronized as table attributes cannot be modified
    @Override
    public int columnCount()
    {
        return super.columnCount();
    }

    @Override
    public synchronized void removeRow(int index)
    {
        super.removeRow(index);
    }

    @Override
    public boolean equals(Object o)
    {
        return super.equals(o);
    }

    @Override
    public int compareTo(Table<String> other)
    {
        return super.compareTo(other);
    }

    @Override
    public List<List<T>> toList()
    {
        return super.toList();
    }
}
