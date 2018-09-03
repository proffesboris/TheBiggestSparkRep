package ru.sberbank.sdcb.k7m.core.pack;

import com.sbt.tfs.integration.SendFileInfoNf;
import com.sbt.tfs.integration.SendFileStatusNf;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Main класс интеграции с ТФС
 */
public class IntegrationMain {

    private static String STAGE_ONE = "1";
    private static String STAGE_TWO = "2";

    public static void main(String[] args) {
        try {
            innerMain(args);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
            System.exit(1);
        }
    }

    private static void innerMain(String[] args) {
        String sys = args[0];
        String kafkaBroker = args[1];
        String stage = args[2];
        ExternalSystemConfig externalSystemConfig = ExternalSystemConfig.valueOf(sys.toUpperCase());

        if (STAGE_ONE.equals(stage)) {
            stageOne(sys, externalSystemConfig);
        } else if (STAGE_TWO.equals(stage)) {
            stageTwo(sys, kafkaBroker, externalSystemConfig);
        } else {
            throw new IllegalStateException("Неверное использование параметра STAGE! (Возможные значения: 1 и 2)");
        }
    }


    private static void stageOne(String sys, ExternalSystemConfig externalSystemConfig) {
        List<File> files = FileUtils.obtainFiles(sys);
        LogUtils.logTitle("Copy files to TFS dir");
        CliUtils.rsync(files, externalSystemConfig.getDestinationFolder());
    }

    private static void stageTwo(String sys, String kafkaBroker, ExternalSystemConfig externalSystemConfig) {
        List<File> files = FileUtils.obtainFiles(sys);
        SendFileInfoNf rq = SendFileInfoNfBuilder.build(externalSystemConfig, files);

        String rqMarshaled = JaxbUtils.marshal(rq);
        LogUtils.log("Message formed", rqMarshaled);
        String rqUID = rq.getRqUID();
        LogUtils.log("RQ_UID", rqUID);

        String groupId = "k7m-" + sys;
        TfsKafkaClient tfsKafkaClient = new TfsKafkaClient(kafkaBroker, groupId);
        LogUtils.log("Send message to Kafka", "Start sending message");
        tfsKafkaClient.sendMessage(rqMarshaled);
        LogUtils.log("Message sent");

        LogUtils.logTitle("Waiting for answer");
        String answer = tfsKafkaClient.receiveAnswer(rqUID);
        LogUtils.log("Message received", answer);
        SendFileStatusNf fileStatusNf = JaxbUtils.unmarshal(answer);

        long errorsCount = fileStatusNf.getFileStatuses()
                .stream()
                .filter(status -> status.getStatus().getStatusCode() != 0)
                .count();
        LogUtils.log("Errors: " + errorsCount);

        if (errorsCount > 0) {
            throw new RuntimeException("Получено сообщение с ошибками!");
        }
    }
}
