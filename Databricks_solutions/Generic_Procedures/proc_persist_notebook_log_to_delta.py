# Ler o ficheiro de log gerado pela execução do notebook, transforma em DataFrame, adiciona metadados da execução e grava em Delta (ABFSS). Regista a tabela no metastore se necessário.
def proc_persist_notebook_log_to_delta( env : str,
                                        spark_session: SparkSession,
                                        log_file_path: str,
                                        delta_abfss_path: str,
                                        valid_environments,
                                        valid_schemas,
                                        valid_write_modes,
                                        notebook_path: str = None,
                                        run_id: str = None,
                                        parse_fn = None,
                                        dbutils = None,
                                        logger = None
    ) -> dict:
    """
    Última versão: 15/01/2026
    Testado em: 

    Ler o ficheiro de log gerado pela execução do notebook, transforma em Spark DataFrame,
    adiciona metadados da execução e grava em Delta (ABFSS). Regista a tabela no metastore se necessário.

    Parâmetros:
    -----------
        env: str 
            Ambiente de execução.
        spark_session: SpakSession 
            SparkSession ativa.
        log_file_path: str 
            Caminho do ficheiro de log no DBFS (ex: /dbfs/logs/IngestaoDataDrive_2025-10-06_13-00-00.log).
        delta_abfss_path: str
            Caminho ABFSS para salvar a tabela Delta (ex: abfss://log-dev@.../tables/audit_logs).
        valid_environments : Interable
            Interável com os ambientes válidos.
        valid_schemas : Interable
            Interável com os schemas válidos.
        valid_write_modes : Interable
            Interável com os modos de escrita válidos.
        notebook_path: str, opcional 
            Caminho lógico do notebook (p.ex. dbutils.notebook.entry_point...).
        run_id: int, opcional
            Identificador da execução; se None será gerado um UUID.
        parse_fn: 
            Função opcional para parse personalizado de cada linha; recebe (line: str) e devolve dict com keys correspondentes.
        dbutils: DBUtils, opcional
            DBUtils para registar a tabela no metastore.
        logger : Logger, opcional
            Logger para registar eventos.

    Retorno:
    --------
        dict com status da operação.

    Use esta função se:
    -------------------
        - O ficheiro de log é um ficheiro de texto simples.
        - O ficheiro de log tem uma linha por evento.
        - Cada linha tem uma estrutura padrão (datetime, level, logger, message)
    
    Exemplo de uso:
    ---------------
        proc_persist_notebook_log_to_delta( env = "dev",
                                            spark_session = spark_session,
                                            log_file_path = "/dbfs/logs/IngestaoDataDrive_2025-10-06_13-00-00.log",
                                            delta_abfss_path = "abfss://log-dev@.../tables/audit_logs",
                                            valid_environments = valid_domains_dict.get("environment"),
                                            valid_schemas = valid_domains_dict.get("schema"),
                                            valid_write_modes,
                                            notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get(),
                                            run_id = None,
                                            parse_fn = None
        )  
    """
    # Valida interáveis
    for name, value in [("valid_environments", valid_environments),
                        ("valid_schemas", valid_schemas),
                        ("valid_write_modes", valid_write_modes)]:
        if not hasattr(value, "__iter__"):
            raise TypeError(f"Esperava um iterável para {name}, mas recebeu {type(value)}")

    # Validar o ambiente de execução e garantir uma SparkSession válida.
    spark_session = chk.chk_validate_environment(env = env, spark_session = spark_session, valid_environments = valid_environments, logger = logger)

    metastore_schema = "audit"
    metastore_table = "notebook_logs"

    # Normaliza path para Spark
    if log_file_path.startswith("/dbfs/"):
        log_file_path_spark = "dbfs:" + log_file_path[5:]

    elif not log_file_path.startswith("dbfs:/"):
        log_file_path_spark = "dbfs:" + log_file_path

    else:
        log_file_path_spark = log_file_path

    # Feche explicitamente todos os FileHandlers
    if logger:
        for handler in logger.handlers:
            if hasattr(handler, "flush"):
                handler.flush()

            if hasattr(handler, "close"):
                handler.close()

        # remove todos os handlers para evitar double flush
        logger.handlers = [h for h in logger.handlers if not isinstance(h, logging.FileHandler)]

    timeout_secs = 300
    wait_interval = 2
    total_waited = 0
    file_ready = False
    last_exc = None

    # Identificador da execução
    run_id = run_id or str(uuid.uuid4())
    ingestion_ts = datetime.utcnow()

    while total_waited < timeout_secs:
        try:
            dbutils.fs.ls("dbfs:/logs/")  # força refresh do DBFS
            _ = spark_session.read.text(log_file_path_spark).limit(1).collect()
            file_ready = True
            break
        
        except Exception as e:
            last_exc = e
            time.sleep(wait_interval)
            total_waited += wait_interval

    if not file_ready:
        msg = f"Log file not found ou inacessível após {timeout_secs}s: {log_file_path_spark}. Erro: {str(last_exc)}"
        if logger:
            logger.warning(msg)
        
        return {"status": "timeout", "run_id": run_id, 
                "records": 0, "path": log_file_path_spark}


    raw_df = spark_session.read.text(log_file_path_spark)
    raw_df.persist()  # Evita recomputação
    lines = raw_df.rdd.map(lambda r: r[0]).collect()

    log_entry_pattern = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3} ")

    raw_entries = []
    current_entry_lines = []

    for line in lines:
        if log_entry_pattern.match(line):
            if current_entry_lines:
                raw_entries.append(current_entry_lines)
            current_entry_lines = [line]

        else:
            if current_entry_lines:
                current_entry_lines.append(line)

            else:
                raw_entries.append([line])

    if current_entry_lines:
        raw_entries.append(current_entry_lines)

    def default_parse(entry_lines):
        first_line = entry_lines[0]
        parts = first_line.strip().split(" ", 4)
        if len(parts) < 5:
            return {
                "Log_Date": None,
                "Level": None,
                "Logger": None,
                "Message": first_line,
                "Exception_Message": None,
                "Stack_Trace": None
            }

        log_datetime = parts[0] + " " + parts[1]
        level = parts[2]
        logger_name = parts[3]
        message = parts[4]

        exception_message = None
        stack_trace = None
        if level.upper() in {"ERROR", "CRITICAL"} and len(entry_lines) > 1:
            exception_message = message
            stack_trace = "\n".join(entry_lines[1:])

        return {
            "Log_Date": log_datetime,
            "Level": level,
            "Logger": logger_name,
            "Message": message,
            "Exception_Message": exception_message,
            "Stack_Trace": stack_trace
        }

    parser = parse_fn or default_parse
    parsed = []

    for entry_lines in raw_entries:
        try:
            p = parser(entry_lines) if parse_fn else default_parse(entry_lines)
            for k in ["Log_Date", "Level", "Logger", "Message", "Exception_Message", "Stack_Trace"]:
                if k not in p:
                    p[k] = None

            p.update({
                "Run_ID": run_id,
                "Notebook_Path": notebook_path or "",
                "Ingestion_UTC_TS": ingestion_ts.isoformat(),
                "Raw_Line": "\n".join(entry_lines),
                "Env": env,
                "Spark_APP_ID": spark_session.sparkContext.applicationId if spark_session else None,
                "Job_Type": "notebook",
                "Error_Code": None
            })
            parsed.append(p)
            
        except Exception as e:
            parsed.append({
                "Log_Date": None,
                "Level": "PARSE_ERROR",
                "Logger": None,
                "Message": f"PARSE_FAIL: {str(e)}",
                "Exception_Message": None,
                "Stack_Trace": None,
                "Run_ID": run_id,
                "Notebook_Path": notebook_path or "",
                "Ingestion_UTC_TS": ingestion_ts.isoformat(),
                "Raw_Line": "\n".join(entry_lines),
                "Env": env,
                "Spark_APP_ID": spark_session.sparkContext.applicationId if spark_session else None,
                "Job_Type": "notebook",
                "Error_Code": None
            })

    if not parsed:
        raw_df.unpersist(blocking=True)
        return {"status": "no_data", "run_id": run_id, "records": 0}
    
    try:
        schema = StructType([
            StructField("Env", StringType(), True),
            StructField("Log_Date", StringType(), True),
            StructField("Level", StringType(), True),
            StructField("Logger", StringType(), True),
            StructField("Message", StringType(), True),
            StructField("Run_ID", StringType(), True),
            StructField("Notebook_Path", StringType(), True),
            StructField("Ingestion_UTC_TS", StringType(), True),
            StructField("Raw_Line", StringType(), True),
            StructField("Spark_APP_ID", StringType(), True),
            StructField("Job_Type", StringType(), True),
            StructField("Exception_Message", StringType(), True),
            StructField("Stack_Trace", StringType(), True),
            StructField("Error_Code", StringType(), True)
        ])

        fields_order = [
            "Env", "Log_Date", "Level", "Logger", "Message", "Run_ID", "Notebook_Path",
            "Ingestion_UTC_TS", "Raw_Line", "Spark_APP_ID", "Job_Type", "Exception_Message", "Stack_Trace", "Error_Code"
        ]

        rows = [
            tuple(d.get(k) for k in fields_order)
            for d in parsed
        ]

        audit_df = spark_session.createDataFrame(rows, schema)
        audit_df.persist()  # Otimiza escrita
        audit_df.count() # força materialização

        userMetadata = f"Salvo por proc_persist_notebook_log_to_delta para {metastore_table} no ambiente {env}"

        # Escrita Delta
        flg_success = proc_save_df_to_table_on_metastore(
                                    env = env,
                                    spark_session = spark_session,
                                    df = audit_df,
                                    schema_name = metastore_schema, 
                                    table_name = metastore_table,
                                    valid_environments = valid_environments, 
                                    valid_schemas = valid_schemas,
                                    valid_write_modes = valid_write_modes,
                                    target_type = "path",
                                    full_delta_path = delta_abfss_path,  # ← Caminho físico
                                    mode = "append",
                                    merge_schema = False,
                                    overwrite_schema = False,
                                    user_metadata = userMetadata,
                                    partition_columns_lst = (),
                                    logger = logger
        )
        if not flg_success:
            msg = f"Não foi possível salvar dataframe da tabela {metastore_table} no caminho {delta_abfss_path}."
            if logger:
                logger.error(msg)
            raise Exception(msg)

        # Registro no metastore
        result = proc_register_table_in_metastore(
            env = env, spark_session = spark_session, schema_name = metastore_schema,
            table_name = metastore_table, full_schema_delta_path = delta_abfss_path, mode = "append",
            df = audit_df, valid_environments = valid_environments,
            valid_schemas = valid_schemas, valid_write_modes = valid_write_modes, logger = logger
        )

        return {"status": "ok", "run_id": run_id, "records": len(parsed), "table": metastore_table, "path": delta_abfss_path}
    
    except Exception as e:
        msg = f"Erro ao gravar dados no delta de audit para a tabela {metastore_table}: {e}"
        if logger:
            logger.exception(msg)
        raise Exception(msg)
    
    finally:
        try:
            # Limpa dataframe da memória imediatamente após uso
            flg_ok_clear = utl.util_delete_dataframe_variable_from_scope(
                ['raw_df','audit_df'],
                "both",
                logger
            )
        except Exception:
            pass 
