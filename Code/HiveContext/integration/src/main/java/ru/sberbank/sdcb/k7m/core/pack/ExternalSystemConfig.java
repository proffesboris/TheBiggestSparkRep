package ru.sberbank.sdcb.k7m.core.pack;

/**
 * Конфигурация внешних системя для выгрузки в ТФС
 */
public enum ExternalSystemConfig {

    /**
     * ОКР
     */
    OKR("tfs_alpha", "/mnt/IKTFS/from/OKR/List/", "49141700",
            "Файлы в ОКР", "/DFDC/from/OKR/List/List", "OKR/to/DFDC/List"),

    /**
     * ММЗ
     */
    MMZ("tfs_alpha", "/mnt/IKTFS/from/MMZ/Client_date_PIM", "48957500",
            "Файлы в ММЗ", "/mnt/IKTFS/from/MMZ/Client_date_PIM/List", "/mnt/IKTFS/from/MMZ/Client_date_PIM"),

    /**
     * СРМ
     */
    CRM("crmcorp_alpha", "/mnt/IKTFS/from/CRMCORP/Clients_Lists", "49346600",
            "", "/mnt/IKTFS/from/CRMCORP/Clients_Lists/List", "/mnt/IKTFS/from/CRMCORP/Clients_Lists"),

    /**
     * Робот Юрист
     */
    ROB("tfs_alpha", "/mnt/IKTFS/from/BPMPO/CustomerList", "48151000",
            "", "/DFDC/from/BPMPO/CustomerList/List", "/BPMPO/to/DFDC/CustomerList");


    private final String destinationName;
    private final String destinationFolder;
    private final String scenarioId;
    private final String businessName;
    private final String folderSource;
    private final String folderTarget;

    ExternalSystemConfig(String destinationName, String destinationFolder, String scenarioId,
                         String businessName, String folderSource, String folderTarget) {
        this.destinationName = destinationName;
        this.destinationFolder = destinationFolder;
        this.scenarioId = scenarioId;
        this.businessName = businessName;
        this.folderSource = folderSource;
        this.folderTarget = folderTarget;
    }

    public String getDestinationName() {
        return destinationName;
    }

    public String getDestinationFolder() {
        return destinationFolder;
    }

    public String getScenarioId() {
        return scenarioId;
    }

    public String getBusinessName() {
        return businessName;
    }

    public String getFolderSource() {
        return folderSource;
    }

    public String getFolderTarget() {
        return folderTarget;
    }
}
