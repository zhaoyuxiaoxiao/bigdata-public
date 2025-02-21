package cn.bigdata.project;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class DailyCompressedTopicFileSink extends AbstractSink implements Configurable {
    private final static Logger log = LoggerFactory.getLogger(DailyCompressedTopicFileSink.class);
    private int diskUsageThreshold;
    private List<String> pathList;
    private HashMap<String, ZipOutputStream> zipOutputStreamMap = new HashMap<>();
    String date;
    private SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");

    @Override
    public Status process() throws EventDeliveryException {

        Status status;
        Channel channel = getChannel();
        // channel 支持事务
        Transaction thx = channel.getTransaction();
        String currentDate = getDate();

        if (!currentDate.equals(date)) {
            date = currentDate;
            zipOutputStreamMap.forEach((topicName, zipOutputStream) -> {
                closeOutputStream(topicName);
            });
            zipOutputStreamMap.forEach((topicName, zipOutputStream) -> {
                putZipOutputStream(topicName);
            });
        }
        //开始事务
        thx.begin();
        try {

            Event event = channel.take();
            String topicName = event.getHeaders().get("topic");
            String massage = new String(event.getBody()) + "\n";
            getZipOutputStream(topicName).write(massage.getBytes());
            thx.commit();    //提交事务
            status = Status.READY; //设置状态

        } catch (Exception e) {
            status = Status.BACKOFF; //设置回滚状态
            thx.rollback(); //事务回滚
        } finally {
            thx.close();  //关闭事务
        }
        return status;
    }

    @Override
    public void configure(Context context) {
        this.pathList = new ArrayList<>(Arrays.asList(context.getString("path").split(",")));
        this.date = getDate();
        this.diskUsageThreshold = context.getInteger("disk_usage_threshold", 90);
    }

    @Override
    public synchronized void stop() {
        zipOutputStreamMap.forEach((topicName, zipOutputStream) -> {
            closeOutputStream(topicName);
        });
        super.stop();
    }

    public ZipOutputStream getZipOutputStream(String topicName) {

        if (zipOutputStreamMap.containsKey(topicName)) {
            return zipOutputStreamMap.get(topicName);
        } else {
            putZipOutputStream(topicName);
            return zipOutputStreamMap.get(topicName);
        }
    }

    public void putZipOutputStream(String topicName) {
        try {
            String filePath = getFilePath(topicName);
            ZipOutputStream zipOutputStream = new ZipOutputStream(new FileOutputStream(filePath, true));
            zipOutputStream.putNextEntry(new ZipEntry(topicName));
            zipOutputStreamMap.put(topicName, zipOutputStream);
            log.info(String.format("The topic %s has  created an output stream. path : %s", topicName,filePath));
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public String getDate() {
        return formatter.format(new Date(System.currentTimeMillis()));
    }

    public String getFilePath(String topicName) {
        String selectedPath = getAvailablePath();
        if (selectedPath == null) {
            log.error(
                    "\n-------------------------------------------------" +
                    "\nNo available path to write data. All paths are full." +
                    "\n-------------------------------------------------"
            );
            System.exit(0);
            return null;
        } else {
            String rootPath = selectedPath + File.separator + formatter.format(new Date(System.currentTimeMillis()));
            if (!new java.io.File(rootPath).exists()) {
                new java.io.File(rootPath).mkdirs();
            }
            return rootPath + File.separator + topicName + ".zip";
        }
    }

    public void closeOutputStream(String topicName) {
        try {
            zipOutputStreamMap.get(topicName).flush();
            zipOutputStreamMap.get(topicName).close();
            log.info("close " + topicName + " OutputStream ......");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private String getAvailablePath() {
        for (String path : pathList) {
            if (isDiskSpaceAvailable(path)) {
                return path;
            }
        }
        return null;
    }

    private boolean isDiskSpaceAvailable(String path) {
        File file = new File(path);
        if (!file.exists()) {
            file.mkdirs();
        }
        try {
            FileStore fileStore = Files.getFileStore(file.toPath());
            long totalSpace = fileStore.getTotalSpace();
            long usableSpace = fileStore.getUsableSpace();
            long usedSpace = totalSpace - usableSpace;
            double usedPercentage = ((double) usedSpace / totalSpace) * 100;
            return usedPercentage < diskUsageThreshold;
        } catch (IOException e) {
            log.error("Error checking disk space for path: " + path, e);
            return false;
        }
    }

}
