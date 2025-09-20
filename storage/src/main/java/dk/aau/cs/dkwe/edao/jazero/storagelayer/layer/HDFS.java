package dk.aau.cs.dkwe.edao.jazero.storagelayer.layer;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

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
            List<FileStatus> fileStatuses = List.of(this.fs.listStatus(this.hdfsDir));
            return new HDFSIterator(fileStatuses.iterator());
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
            List<FileStatus> fileStatuses = List.of(this.fs.listStatus(this.hdfsDir));
            Iterator<File> iterator = new HDFSIterator(fileStatuses.iterator(), predicate);

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
        private final Iterator<FileStatus> iterator;
        private final Queue<File> downloadQueue = new LinkedList<>();
        private Predicate<File> filter = null;
        private static final Path TMP_DOWNLOAD_DIR = new Path("/tmp_files/");

        private HDFSIterator(Iterator<FileStatus> iterator)
        {
            try
            {
                this.iterator = iterator;
                fs.mkdirs(TMP_DOWNLOAD_DIR);
            }

            catch (IOException e)
            {
                throw new RuntimeException("IOException: " + e.getMessage());
            }
        }

        private HDFSIterator(Iterator<FileStatus> iterator, Predicate<File> filter)
        {
            this(iterator);
            this.filter = filter;
        }

        @Override
        public boolean hasNext()
        {
            return !this.downloadQueue.isEmpty() || this.iterator.hasNext();
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
                    clearTmpDir();

                    while (this.iterator.hasNext() && i++ < BATCH_SIZE)
                    {
                        Path srcPath = this.iterator.next().getPath(), destPath = new Path(TMP_DOWNLOAD_DIR, srcPath.getName());
                        fs.rename(srcPath, destPath);
                    }

                    fs.copyToLocalFile(TMP_DOWNLOAD_DIR, LOCAL_DIR);
                    this.downloadQueue.addAll(Files.list(new File(LOCAL_DIR + TMP_DOWNLOAD_DIR.toString().substring(1)).toPath())
                            .map(java.nio.file.Path::toFile)
                            .collect(Collectors.toSet()));
                }

                File nextFile = this.downloadQueue.poll();

                if (this.filter != null)
                {
                    while (!this.filter.test(nextFile))
                    {
                        nextFile.delete();
                        nextFile = this.downloadQueue.poll();
                    }
                }

                return nextFile;
            }

            catch (IOException e)
            {
                throw new RuntimeException("Failed iterating HDFS directory", e);
            }
        }

        private void clearDownloadDir() throws IOException
        {
            File dir = new File(LOCAL_DIR + TMP_DOWNLOAD_DIR.toString().substring(1));
            FileUtils.deleteDirectory(dir);
        }

        private void clearTmpDir() throws IOException
        {
            for (FileStatus status : fs.listStatus(TMP_DOWNLOAD_DIR))
            {
                fs.delete(status.getPath());
            }
        }
    }
}
