package dk.aau.cs.dkwe.edao.jazero.storagelayer.layer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.function.Predicate;

public class HDFS implements Storage<File>, Closeable
{
    private final FileSystem fs;
    private final Path hdfsDir;
    private static final int BATCH_SIZE = 1000;
    private static final Path LOCAL_DIR = new Path("/tmp/tables/");

    public HDFS(String dir, Path coreSite, Path hdfsSite)
    {
        Configuration config = new Configuration();
        config.addResource(coreSite);
        config.addResource(hdfsSite);
        config.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        config.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

        try
        {
            File localDir = new File(LOCAL_DIR.toString());
            this.hdfsDir = new Path(dir);
            this.fs = FileSystem.get(config);

            if (!localDir.isDirectory() && !localDir.mkdirs())
            {
                throw new IOException("Failed to create local directory '" + localDir + "'");
            }
        }

        catch (IOException e)
        {
            throw new RuntimeException("Failed to initialize HDFS", e);
        }
    }

    public static String localDirectory()
    {
        return LOCAL_DIR.toUri().getPath();
    }

    @Override
    public boolean insert(File element)
    {
        try
        {
            Path src = new Path(element.getAbsolutePath());
            this.fs.copyToLocalFile(src, this.hdfsDir);
            return true;
        }

        catch (IOException e)
        {
            return false;
        }
    }

    @Override
    public int count()
    {
        try
        {
            int count = 0;
            RemoteIterator<?> iterator = this.fs.listFiles(this.hdfsDir, false);

            while (iterator.hasNext())
            {
                count++;
                iterator.next();
            }

            return count;
        }

        catch (IOException e)
        {
            return -1;
        }
    }

    @Override
    public Iterator<File> iterator()
    {
        try
        {
            RemoteIterator<LocatedFileStatus> iterator = this.fs.listFiles(this.hdfsDir, false);
            return new HDFSIterator(iterator);
        }

        catch (IOException e)
        {
            throw new RuntimeException("Failed to iterate HDFS directory", e);
        }
    }

    @Override
    public Set<File> elements()
    {
        Set<File> files = new HashSet<>();

        for (File file : this)
        {
            files.add(file);
        }

        return files;
    }

    @Override
    public Set<File> elements(Predicate<File> predicate)
    {
        try
        {
            Set<File> files = new HashSet<>();
            Iterator<File> iterator = new HDFSIterator(this.fs.listFiles(this.hdfsDir, false), predicate);

            while (iterator.hasNext())
            {
                files.add(iterator.next());
            }

            return files;
        }

        catch (IOException e)
        {
            throw new RuntimeException("Failed to retrieve HDFS files", e);
        }
    }

    @Override
    public boolean clear()
    {
        try
        {
            RemoteIterator<LocatedFileStatus> iterator = this.fs.listFiles(this.hdfsDir, false);

            while (iterator.hasNext())
            {
                Path path = iterator.next().getPath();
                this.fs.delete(path, true);
            }

            return true;
        }

        catch (IOException e)
        {
            return false;
        }
    }

    @Override
    public boolean delete(File element)
    {
        try
        {
            Path path = new Path(this.hdfsDir.toString() + "/" + element.getName());
            this.fs.delete(path, true);
            return true;
        }

        catch (IOException e)
        {
            return false;
        }
    }

    @Override
    public void close()
    {
        try
        {
            this.fs.close();
        }

        catch (IOException ignored) {}
    }

    private class HDFSIterator implements Iterator<File>
    {
        private final RemoteIterator<LocatedFileStatus> iterator;
        private final Queue<File> downloadQueue = new LinkedList<>();
        private Predicate<File> filter = null;

        private HDFSIterator(RemoteIterator<LocatedFileStatus> iterator)
        {
            this.iterator = iterator;
        }

        private HDFSIterator(RemoteIterator<LocatedFileStatus> iterator, Predicate<File> filter)
        {
            this(iterator);
            this.filter = filter;
        }

        @Override
        public boolean hasNext()
        {
            try
            {
                return !this.downloadQueue.isEmpty() || this.iterator.hasNext();
            }

            catch (IOException e)
            {
                throw new RuntimeException("Failed iterating HDFS directory", e);
            }
        }

        /*
            It's important to use the file immediately after calling next(),
            as all of the files in the batch are deleted before retrieving the next batch
         */
        @Override
        public File next()
        {
            try
            {
                if (this.downloadQueue.isEmpty())
                {
                    int i = 0;
                    clearDownloadDir();

                    while (this.iterator.hasNext() && i++ < BATCH_SIZE)
                    {
                        Path path = this.iterator.next().getPath();
                        fs.copyToLocalFile(path, LOCAL_DIR);

                        File file = new File(LOCAL_DIR + "/" + path.getName());

                        if (this.filter == null)
                        {
                            this.downloadQueue.add(file);
                        }

                        else
                        {
                            if (this.filter.test(file))
                            {
                                this.downloadQueue.add(file);
                            }

                            else
                            {
                                file.delete();
                            }
                        }
                    }
                }

                return this.downloadQueue.poll();
            }

            catch (IOException e)
            {
                throw new RuntimeException("Failed iterating HDFS directory", e);
            }
        }

        private void clearDownloadDir()
        {
            File dir = new File(LOCAL_DIR.toString());

            for (File f : Objects.requireNonNull(dir.listFiles()))
            {
                f.delete();
            }
        }
    }
}
