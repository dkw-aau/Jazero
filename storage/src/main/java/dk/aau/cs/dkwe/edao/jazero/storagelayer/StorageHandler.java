package dk.aau.cs.dkwe.edao.jazero.storagelayer;

import dk.aau.cs.dkwe.edao.jazero.storagelayer.layer.Disk;
import dk.aau.cs.dkwe.edao.jazero.storagelayer.layer.HDFS;
import dk.aau.cs.dkwe.edao.jazero.storagelayer.layer.Storage;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.util.Iterator;
import java.util.Set;
import java.util.function.Predicate;

public class StorageHandler implements Storage<File>
{
    public enum StorageType
    {
        NATIVE,
        HDFS
    }

    private final StorageType type;
    private final Storage<File> storage;
    private final File tableDir;

    public StorageHandler(StorageType storageType, String tableDir)
    {
        this.type = storageType;

        if (this.type == StorageType.NATIVE)
        {
            this.tableDir = new File(tableDir);
            this.storage = new Disk(this.tableDir);
        }

        else
        {
            throw new IllegalArgumentException("Storage type must be '" + storageType.toString() + "'");
        }
    }

    public StorageHandler(String hdfsTableDir, StorageType storageType, String coreSitePath, String hdfsSitePath)
    {
        Path coreSite = new Path(coreSitePath), hdfsSite = new Path(hdfsSitePath);
        this.type = storageType;
        this.tableDir = new File(hdfsSitePath);

        if (this.type == StorageType.HDFS)
        {
            this.storage = new HDFS(hdfsTableDir, coreSite, hdfsSite);
        }

        else
        {
            throw new IllegalArgumentException("Storage cannot be '" + storageType.toString() + "' when HDFS configuration files are passed");
        }
    }

    public StorageType getStorageType()
    {
        return this.type;
    }

    public File getStorageDirectory()
    {
        return this.tableDir;
    }

    @Override
    public boolean insert(File file)
    {
        return this.storage.insert(file);
    }

    @Override
    public int count()
    {
        return this.storage.count();
    }

    // TODO: Maybe we should implement our own iterator that only stores the file names without the path to the file
    // TODO: When calling .next(), we prepend the path
    // TODO: This will work as we require all files to be in the same directory
    @Override
    public Iterator<File> iterator()
    {
        return this.storage.iterator();
    }

    @Override
    public Set<File> elements()
    {
        return this.storage.elements();
    }

    @Override
    public Set<File> elements(Predicate<File> predicate)
    {
        return this.storage.elements(predicate);
    }

    @Override
    public boolean clear()
    {
        return this.storage.clear();
    }

    @Override
    public boolean delete(File element)
    {
        return this.storage.delete(element);
    }
}
