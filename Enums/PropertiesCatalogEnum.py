from enum import Enum

class PropertiesCatalogEnum(Enum):
    RPF = "ReadPathsFrom.properties"                         # Place From
    RPT = "ReadPathsTo.properties"                           # Place To
    HP = "HiveProperties.properties"                         # Hive properties for same environment
    IP = "ImpalaProperties.properties"                       # Impala properties for same environment
    SP = "SqoopProperties.properties"                        # Sqoop properties for same environment
    MDP = "MongoDBProperties.properties"                     # MongoDB properties for same environment
    MDBP = "MariaDBProperties.properties"                    # Mysql/MariaDB properties for same environment
    TP = "TeradataProperties.properties"                     # Teradata properties for same environment
    OP = "OracleProperties.properties"                       # Oracle properties for same environment
    LFP = "LocalFileProperties.properties"                   # Local files
    SBCP = "SparkSubmitProperties.properties"                # Spark Submit config options