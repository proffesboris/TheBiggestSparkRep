package ru.sberbank.sdcb.k7m.core.pack;

import com.sbt.tfs.integration.*;

import java.io.File;
import java.util.List;
import java.util.UUID;

/**
 * Построитель запроса отправки файла в ТФС
 */
public final class SendFileInfoNfBuilder {

    public static final String DATA_CLOUD_SPNAME = "urn:sbrfsystems:99-dfdc_alpha";
    public static final String TFS_SYSTEMID = "urn:sbrfsystems:99-tfs_alpha";

    private SendFileInfoNfBuilder() {
    }

    public static SendFileInfoNf build(ExternalSystemConfig externalSystemConfig, List<File> files) {
        SendFileInfoNf rq = new SendFileInfoNf();
        rq.setRqUID(UUID.randomUUID().toString().replace("-", ""));
        rq.setRqTm(IntegrationUtils.currentTimestamp());
        rq.setOperUID(UUID.randomUUID().toString().replace("-", ""));
        rq.setSPName(DATA_CLOUD_SPNAME);
        rq.setSystemId(TFS_SYSTEMID);
        DestinationType destination = new DestinationType();
        destination.setName(externalSystemConfig.getDestinationName());
        destination.setVersion("3.0");
        rq.setDestination(destination);
        fillFiles(rq, externalSystemConfig, files);
        return rq;
    }

    private static void fillFiles(SendFileInfoNf rq, ExternalSystemConfig externalSystemConfig, List<File> files) {
        for (File file : files) {
            FileType fileType = new FileType();

            FileInfoType fileInfoType = new FileInfoType();
            fileInfoType.setName(file.getName());
            fileInfoType.setBusinessName(externalSystemConfig.getBusinessName());
            fileInfoType.setSize(file.length());
            fileInfoType.setCheckSum(String.valueOf(IntegrationUtils.crc(file)));
            fileType.setFileInfo(fileInfoType);

            FolderType folderType = new FolderType();
            folderType.setFolderSource(externalSystemConfig.getFolderSource());
            folderType.setFolderTarget(externalSystemConfig.getFolderTarget());
            fileType.setFolder(folderType);

            ScenarioInfoType scenarioInfoType = new ScenarioInfoType();
            scenarioInfoType.setScenarioId(externalSystemConfig.getScenarioId());
            fileType.setScenarioInfo(scenarioInfoType);

            rq.getFiles().add(fileType);
        }
    }

}
