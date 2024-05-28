# secrisk-raven-embedded
Requirement: Python version 3.8

## Build Solution
```powershell
& cd <Path to solution>/secrisk-raven-embedded/

& .\setup_eRaven.ps1 -TargetDatabase [Target database name] -AppName [Application name] -FlagBuildRaven [$true | $false]

Example:
& .\setup_eRaven.ps1 -TargetDatabase [Target database name] -AppName [Application name] -FlagBuildRaven [$true | $false]

```

# Metadata Dictionary

## Source File
|       | Not NULL  | Fied Name                 | Description | Default |
| :---  | :---      |:---                       | :---        | :---    |
| PK    | TRUE      | SOURCE_SYSTEM_CODE        | Code for the file source system - system that generates the file. | |
| PK    | TRUE      | SOURCE_FEED_CODE          | Code for a specific file generated. | |
|       | FALSE     | STAGE_NAME                | Snowflake stage name where the files is stored. [SCHEMA NAME].[STAGE NAME]. NULL value ONLY it is disabled | |
|       | FALSE     | SOURCE_FILE_PATH          | On-premises path. It is used to upload file from On-Premises into cloud storage. | |
|       | FALSE     | SOURCE_FILE_NAME_PATTERN  | File name used when it will be via trigger file or PIPE.  | |
|       | TRUE      | DESTINATION_SCHEMA_NAME   | Schema destination name. | |
|       | TRUE      | DESTINATION_TABLE_NAME    | Table destination name. If schema name is equal to STAGING, the SOURCE_SYSTEM_CODE will be added at the beginning of the name. Example >> When schema is staging: STAGING.[SOURCE_SYSTEM_CODE]_[DESTINATION_TABLE_NAME]; When schema is NOT staging: [DESTINATION_SCHEMA_NAME].[DESTINATION_TABLE_NAME]| |
|       | TRUE      | IS_ENABLED                | Flag.  | TRUE |
|       | TRUE      | FILE_FORMAT               | Name of the file format. It has to be in the list available in RAVEN solution. | |
|       | TRUE      | DATE_INPUT_FORMAT         | Date format present in the source file. | DD/MM/YYYY |
|       | TRUE      | TIMESTAMP_INPUT_FORMAT    | Date and time format present in the source file. | DD/MM/YYYY HH24:MI:SS.FF3 |
|       | TRUE      | SKIP_ROW_ON_ERROR         | Number use in the COPY option  ON_ERROR = SKIP_FILE_[num]. Skip a file when the number of error rows found in the file is equal to or exceeds the specified number. If value equal ZERO, abort the load operation if any error is found in a data file. | 0 |
|       | TRUE      | DELETE_STAGE_BY_FILE_NAME | Used for reloaded file. Equal FALSE the data in staging table is deleted by RAVEN_DATASET_NAME. If set to TRUE it s deleted by RAVEN_FILENAME. |FALSE|

## Source Field

|       | Not NULL  | Fied Name                 | Description | Default |
| :---  | :---      | :---                      | :---        | :---    |
| PK,FK | TRUE      | SOURCE_SYSTEM_CODE        | FK METADATA_SOURCE_FILE. | |
| PK,FK | TRUE      | SOURCE_FEED_CODE          | FK METADATA_SOURCE_FILE. | |
| PK    | TRUE      | SOURCE_FIELD_NAME         | Source field name in the file if there is header. | |
|       | TRUE      | TARGET_FIELD_NAME         | Source field name in the target table. | |
|       | TRUE      | FIELD_ORDINAL             | Field Position in the file. Starts with number 1. Used for CSV files. | |
|       | TRUE      | DATA_TYPE_NAME            | Type of the column. | |
|       | TRUE      | DATA_TYPE_LENGTH          | Length of a data type determines the size of a column. | |
|       | TRUE      | DATA_TYPE_PRECISION       | Precision of a numeric column.| |
|       | TRUE      | DATA_TYPE_SCALE           | Scale of a numeric column. | |
|       | TRUE      | IS_IN_SOURCE              | Flag. Indicates if the field is present in source file. | |
|       | TRUE      | IS_IN_TARGET              | Flag. Indicates if the field will be upload into destination table. | |
|       | TRUE      | IS_DELETED                | Flag. Indicates if the field is not in used.| |
|       | FALSE     | DERIVED_EXPRESSION        | Expression to add or modify a column in the destination table. | |

## Stage Me Parameters

|       | Not NULL  | Fied Name                 | Description | Default |
| :---  | :---      | :---                      | :---        | :---    |
| PK    | TRUE      | DATASET_NAME              | Unique identification for the file. RAPTOR uses SOURCE_SYSTEM_CODE + '_' + FILE_NAME | |
|       | FALSE     | CONTAINER_NAME            | Container source name. It can ONLY be NULL for internal stage. | |
|       | TRUE      | FILE_PATH                 | File path in the stage. When based on cob, example: /[folder name]/&#124;:YYYYMMDD:&#124;/ | |
|       | TRUE      | FILE_NAME                 | File name that will be in the stage. When base on COB, example: [file name]&#124;:YYYYMMDD:&#124;.[extension] | |
| FK    | TRUE      | SOURCE_SYSTEM_CODE        | FK METADATA_SOURCE_FILE. | |
| FK    | TRUE      | SOURCE_FEED_CODE          | FK METADATA_SOURCE_FILE. | |
|       | TRUE      | IS_ENABLED                | Flag. | TRUE  |
|       | TRUE      | IS_TRIGGER_FILE           | Flag. | FALSE |
|       | TRUE      | ALLOW_RELOAD              | Flag. Use in COPY option: FORCE = TRUE | TRUE |
|       | FALSE     | ENTITY_CODE               | File scope. It will be in the VARIANT field (RAVEN_STAGE_SCOPE_FIELDS) in the destination staging table. | |
|       | FALSE     | DEPARTMENT_CODE           | File scope. It will be in the VARIANT field (RAVEN_STAGE_SCOPE_FIELDS) in the destination staging table. | |
|       | FALSE     | REGION                    | File scope. It will be in the VARIANT field (RAVEN_STAGE_SCOPE_FIELDS) in the destination staging table. | |
|       | FALSE     | MARKET                    | File scope. It will be in the VARIANT field (RAVEN_STAGE_SCOPE_FIELDS) in the destination staging table. | |
|       | TRUE      | IS_CLOUD_COPY             | Flag. Indicates if the file is upload into data lake. | FALSE |
|       | FALSE     | EXPECTED_STAGE_TIME       | Expected staging time for the file. | |
|       | FALSE     | WAREHOUSE_SIZE            | If empty, the staging will use the smaller. | |
