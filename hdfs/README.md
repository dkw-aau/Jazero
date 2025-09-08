# Jazero HDFS
To start a standalone, single-node cluster, run the following command:

```bash
docker-compose up
```

Once this is up and running after 1-2 minutes, you can start operating.

When using this cluster in Jazero, you can either populate a directry in HDFS with the data lake tables or you can let Jazero and its connector handle this.

## HDFS Operations
You can perform any basic bash command in HDFS using the following template:

```bash
docker exec jazero_namenode bash hdfs dfs -<COMMAND>
```

For example, to list all files in the directory `/user/path/`, you can run the following command:

```bash
docker exec jazero_namenode bash hdfs dfs -ls /user/path/
```

### Uploading Content To HDFS
Given a local file `file.txt` you wish to upload, create a directory in the namenode (e.g., `/user`) using the following commands:

```bash
docker exec jazero_namenode hdfs dfs -mkdir /user
docker cp file.txt jazero_namenode:/tmp
```

Now, upload the file:

```bash
docker exec jazero_namenode hdfs dfs -put /tmp/file.txt /user
```

### Downloading From HDFS
Run the following command to download a file `/user/file.txt` to the directory `/tmp` on the local namenode file system:

```bash
docker exec jazero_namenode hdfs dfs -get /user/some_file.txt /tmp
```

You can fetch the file to your local file system with the following command:

```bash
docker cp jazero_namenode:/tmp/file.txt .
```