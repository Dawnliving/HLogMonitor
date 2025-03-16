# HLogMonitor
Listen the HDFS log to check the file whether it is modified

***

Golang based

***

### Guidance

```shell
go build .
```

```shell
bash ./sample_log/generate_logs.sh ./sample_log/hdfs_sample.log
```

```shell
./monitor -file sample_log/hdfs_sample.log -interval 3
```





