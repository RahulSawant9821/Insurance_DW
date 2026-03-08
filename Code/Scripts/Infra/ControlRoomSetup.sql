-- Lets start with building the control room of our data warehouse

USE Insurance_DW;

-- Creating a table to store pipelines metadata [pipeline_run_log]

CREATE TABLE METADATA.PIPELINE_LOG (

	runId INT IDENTITY(1,1) PRIMARY KEY,
	pipelineName VARCHAR(200),
	sourceSystem VARCHAR(200),

	startTime DATETIME2,
	endTime DATETIME2,

	pipelineStatus VARCHAR(50),
	recordsProcessed INT,
	errorMessage VARCHAR(MAX),

	createdAt DATETIME2 DEFAULT GETDATE()
);


-- Creating a template table to store validation failures [data quality log]. Mostly needed in Silver layer.

CREATE TABLE METADATA.DATA_QUALITY_LOGS (

	dqId INT Identity(1,1) PRIMARY KEY,
	tableName VARCHAR(200),
	columnName VARCHAR(200),

	failedValue VARCHAR(500),
	ruleViolated VARCHAR(500),

	detected_at DATETIME2 DEFAULT GETDATE()

);


-- Creating a table for storing data catalog

CREATE TABLE METADATA.OBJECT_METADATA (

    object_id INT IDENTITY(1,1) PRIMARY KEY,

    schema_name VARCHAR(100),
    table_name VARCHAR(200),

    object_type VARCHAR(50),

    source_system VARCHAR(100),
    source_table VARCHAR(200),

    load_frequency VARCHAR(50),

	 contains_pii BIT,
    gdpr_sensitive BIT,

    created_at DATETIME2 DEFAULT GETDATE()
);


