# Padronizar o processo de ingestão de tabelas de uma fonte relacional (via JDBC) para a camada Bronze no Data Lake (Delta Lake + Metastore).
def proc_pattern_ingest_data_to_bronze_layer(env: str,
                                            spark_session : SparkSession,
                                            jdbc_url: str,
                                            sql_server_driver : str,
                                            tables_map: dict,
                                            source_value: str,
                                            azure_configs_dict: dict,
                                            valid_domains_dict: dict,
                                            dbutils = None,
                                            logger = None
) -> bool:
    """
    Última versão: 15/01/2026


    Função para padronizar o processo de ingestão de tabelas de uma fonte relacional 
    (via JDBC) para a camada Bronze no Data Lake (Delta Lake + Metastore).

    O fluxo inclui leitura da tabela no SQL Server, validações, tratamento de colunas sensíveis,
    padronização de nomenclatura, escrita em Delta Lake e registro no Metastore, além da 
    atualização da tabela de controle de ingestão.

    Parâmetros:
    ----------
        env : str
            Ambiente atual (ex.: "dev", "prod").
        spark_session : SparkSession
            Sessão Spark ativa.
        jdbc_url : str
            String de conexão JDBC para a base de origem (SQL Server).
        sql_server_driver : str
            Driver JDBC para a base de origem (SQL Server).
        tables_map : dict
            Dicionário no formato {nome_tabela: colunas_sensiveis}.
        source_value : str
            Nome da fonte de dados (ex.: "VRM", "DataDrive").
        azure_configs_dict : dict
            Configurações globais do Azure (paths para Bronze, Silver, Gold).
        valid_domains_dict : dict
            Dicionário de domínios válidos (ambientes, schemas, prefixos de tabela).
        dbutils : Optional[DBUtils]
            Objeto DBUtils para interação com o Databricks.
        logger : Logger
            Objeto de logging para rastreabilidade.

    Retorno:
    -------
        bool: True se o processamento for bem-sucedido, False caso contrário.
        
    Use esta função se:
    -------------------
        - Você deseja realizar uma ingestão **FULL** de tabelas de uma fonte relacional (SQL Server)
        para a camada Bronze no Data Lake (Delta Lake + Metastore).
        - A ingestão deve ser feita em modo **FULL**, ou seja, todos os registos da tabela devem ser 
        processados e gravados no Data Lake.

    Exemplo de uso:
    --------------
        proc_pattern_ingest_data_to_bronze_layer(
            env=env,
            spark_session=spark_session,
            jdbc_url=vrm_jdbc_url,
            sql_server_driver = sql_server_driver,
            tables_map=VRM_tables_vehicles_map,
            source_value="VRM",
            azure_configs_dict=azure_configs_dict,
            valid_domains_dict=valid_domains_dict,
            dbutils = None,
            logger=logger
        )
    """
    # Valida se tables_map é um dicionário válido
    if not chk.check_if_is_valid_dict(tables_map):
        msg = "tables_map inválido; precisa ser um dicionário."
        if logger:
            logger.error(msg)
        raise ValueError(msg)

    # Valida se valid_domains_dict é um dicionário válido
    if not chk.check_if_is_valid_dict(valid_domains_dict):
        msg = "valid_domains_dict inválido; precisa ser um dicionário."
        if logger:
            logger.error(msg)
        raise ValueError(msg)

    # Valida se azure_configs_dict é um dicionário válido
    if not chk.check_if_is_valid_dict(azure_configs_dict):
        msg = "azure_configs_dict inválido; precisa ser um dicionário."
        if logger:
            logger.error(msg)
        raise ValueError(msg)

    valid_environments      = valid_domains_dict.get("environment")
    valid_schemas           = valid_domains_dict.get("schema")
    valid_write_modes       = valid_domains_dict.get("write_mode")
    valid_prefix_tables     = valid_domains_dict.get("prefix_tables")
    valid_ingestion_types   = valid_domains_dict.get("ingestion_type")

    # Extrai os caminhos do dicionário de configurações
    azure_paths = {
        "bronze": azure_configs_dict.get("bronze_path"),
        "audit": azure_configs_dict.get("audit_path"),
        "governance": azure_configs_dict.get("governance_path"),
    }

    # Validar o ambiente de execução e garantir uma SparkSession válida.
    spark_session = chk.chk_validate_environment(env = env, spark_session = spark_session, valid_environments = valid_environments, logger = logger)

    # Define o modo de escrita a partir do dicionário de configurações para ingestão
    if ingestion_mode.lower() == "compare":             
        write_mode = "overwrite"     

    elif ingestion_mode.lower() == "merge":
        write_mode = "append"

    elif ingestion_mode.lower() == "full":         
        write_mode = "overwrite"

    elif ingestion_mode.lower() == "incremental":
        write_mode = "append"

    # Validar URL/driver JDBC SQL Server, opcionalmente write_mode, E SEMPRE testa a conexão.
    flg_ok_jdbc = infra.chk_validate_and_test_jdbc_sql_server( env = env, spark_session = spark_session, jdbc_url = jdbc_url, valid_environments = valid_environments,
            valid_write_modes = valid_write_modes, write_mode = write_mode, sql_server_driver = sql_server_driver, logger = logger
    )
    if not flg_ok_jdbc:
        msg = f"Conexão JDBC SQL Server falhou: {jdbc_url}"
        if logger:
            logger.error(msg)
        raise ValueError(msg)

    # Valida se source_value é uma string válida e não vazia
    if not chk.check_if_is_valid_string(source_value, True):
        msg = "source_value inválido; precisa ser string não vazia e válida."
        if logger:
            logger.error(msg)
        raise ValueError(msg)

    schema_name_source = "dbo"  # fixo para origem
    schema_name_destination = "bronze"  # fixo para destino

    try:
        for table_name, config in tables_map.items():
            # Recupera lista de colunas sensíveis e modo e ingestão
            sensitive_cols = config.get("sensitive_cols", [])
            ingestion_mode = config.get("ingestion_mode", "full")
            partition_cols = config.get("partition_cols", [])
            
            msg = f"Processando a tabela: {table_name} | Fonte: {source_value} | Modo: {ingestion_mode}"
            if logger:
                logger.info(msg)
            
            # Se a carga for da tabela transactions2 aplicar filtro extra
            if table_name.lower() == "transactions2":
                custom_where_clause = (
                    "AND YEAR(CreationDateTime) = 2026 "
                    "AND MONTH(CreationDateTime) >= 1 "
                )
            else:
                custom_where_clause = None

            # Padronização de nome da tabela
            standard_table_name = utl.util_standardize_table_name(table_name)
            final_table_name = ops.operation_add_prefix_on_table_name(standard_table_name, source_value, valid_prefix_tables, logger)

            if logger:
                msg = f"Nome da tabela final: {final_table_name}"
                logger.info(msg)

            if not source_value or source_value.lower() not in ("vrm", "datadrive"):
                # Obtém o source_name da tabela a partir do nome dela
                source_name = utl.util_get_source_value_from_table_name(final_table_name, valid_prefix_tables, logger)

            else:
                source_name = source_value

            # Leitura da tabela no SQL Server
            df_raw = proc_read_table_from_sql_server( env = env,
                                                      spark_session = spark_session,
                                                      jdbc_url = jdbc_url,
                                                      sql_server_driver = sql_server_driver,
                                                      table_name = table_name,
                                                      schema_name = schema_name_source,
                                                      valid_environments = valid_environments,
                                                      valid_schemas = valid_schemas,
                                                      custom_where_clause = custom_where_clause,
                                                      logger = logger
            )
                     
            # Persistir na memória o dataframe e materialização
            df_raw.persist(StorageLevel.MEMORY_AND_DISK)
            count_records = df_raw.count()

            msg = f"Registros lidos da tabela {table_name}: {count_records}"
            if logger:
                logger.info(msg)

            # Recupera lista de colunas da tabela original
            list_columns = utl.util_get_table_columns_fast_on_sql_server(env = env,
                                                                         spark_session = spark_session,
                                                                         jdbc_url = jdbc_url,
                                                                         schema_name = schema_name_source,
                                                                         table_name = table_name,
                                                                         sql_server_driver = sql_server_driver,
                                                                         valid_environments = valid_environments,
                                                                         valid_schemas = valid_schemas,
                                                                         logger = logger
            )
            # Tratamento de colunas sensíveis
            if sensitive_cols:
                df_cleaned = ops.operation_drop_sensitive_columns_on_df(df = df_raw, columns_to_drop = sensitive_cols, logger = logger)

            else:
                df_cleaned = df_raw

            # Reparticionamento inteligente baseado no tamanho
            if count_records > 1000000:  # Tabelas grandes: mais partições
                optimal_partitions = min(200, spark_session.sparkContext.defaultParallelism * 2)

            elif count_records > 100000:  # Tabelas médias
                optimal_partitions = max(8, spark_session.sparkContext.defaultParallelism)

            else:  # Tabelas pequenas: poucas partições
                optimal_partitions = 4
                
            df_cleaned = df_cleaned.repartition(optimal_partitions)
            df_cleaned.persist(StorageLevel.MEMORY_AND_DISK_2)  # Persistência nível 2 para reutilização múltipla

            # Faz unpersist da tabela original
            try:
                df_raw.unpersist(blocking=True)
            
            except Exception:
                pass

            # Se processo de ingestão for do tipo comparação de registos, verifica contagem atual no metastore
            if ingestion_mode == "compare":                 
                count_records_current = utl.util_count_records_in_metastore_table(env = env, 
                                                                                  spark_session = spark_session, 
                                                                                  valid_environments = valid_environments,
                                                                                  valid_schemas = valid_schemas,
                                                                                  schema_name = schema_name_destination, 
                                                                                  table_name = final_table_name, 
                                                                                  prefix_filter = None,
                                                                                  logger = logger
                )
                count_rows_int = int(list(count_records_current.values())[0])
                
                # Compara contagem atual com contagem lida no SQL Server
                if count_rows_int != count_records:
                    msg = f"Contagem de registros atual diferente da contagem lida no SQL Server: {count_rows_int} != {count_records}"
                    if logger:
                        logger.info(msg)

                else:
                    msg = "Contagem de registros atual igual à contagem lida no SQL Server. Processo de ingestão não necessário"
                    if logger:
                        logger.info(msg)    

            elif ingestion_mode == "incremental":
                flg_incremental_ok = proc_pattern_ingest_incremental_data_to_bronze_layer(  env = env,
                                                                                            spark_session = spark_session,
                                                                                            df = df_cleaned,
                                                                                            jdbc_url = jdbc_url,
                                                                                            sql_server_driver = sql_server_driver,
                                                                                            source_value = source_name,
                                                                                            table_name_sql = table_name,
                                                                                            partitionColumn = partition_cols,
                                                                                            standard_table_name = final_table_name,
                                                                                            azure_configs_dict = azure_configs_dict,
                                                                                            valid_domains_dict = valid_domains_dict,
                                                                                            dbutils = dbutils,
                                                                                            logger = logger
                )            
                if not flg_incremental_ok:
                    msg = f"Processo de ingestão incremental falhou para a tabela {table_name}."
                    if logger:
                        logger.error(msg)
                    raise Exception(msg)
                
                else:
                    msg = f"Processo de ingestão incremental foi realizado com sucesso para a tabela {table_name}."
                    if logger:
                        logger.info(msg)
                    return True

            elif ingestion_mode == "merge":
                write_mode = "append"

                flg_ok_merge = proc_pattern_ingest_merge_to_bronze_layer(   env = env,
                                                                            spark_session = spark_session,
                                                                            df = df_cleaned,
                                                                            schema_name = "dbo",
                                                                            table_name = table_name,
                                                                            source_value = source_name,
                                                                            delta_path = f"{azure_paths.get("bronze")}" + "tables/",
                                                                            jdbc_url = jdbc_url,
                                                                            sql_server_driver = sql_server_driver,
                                                                            valid_environments = valid_environments,
                                                                            valid_schemas = valid_schemas,
                                                                            valid_prefix_tables = valid_prefix_tables,
                                                                            logger = logger
                )
                if flg_ok_merge:
                    msg = f"Processo de ingestão por MERGE foi realizado com sucesso para a tabela {table_name}."
                    if logger:
                        logger.info(msg)

                else:
                    msg = f"Processo de ingestão por MERGE falhou para a tabela {table_name}."
                    if logger:
                        logger.error(msg)
                    raise Exception(msg)

            else:
                msg = f"Modo de ingestão inválido: {ingestion_mode}"
                if logger:
                    logger.error(msg)
                raise ValueError(msg)

            # Persistência em Delta Lake
            full_schema_delta_path = proc_write_df_to_delta_table(  env = env,
                                                                    spark_session = spark_session,
                                                                    df = df_cleaned,
                                                                    schema_name = schema_name_destination,
                                                                    schema_delta_path = f"{azure_paths.get("bronze")}" + "tables/",
                                                                    table_name = final_table_name,
                                                                    source_value = source_name,
                                                                    jdbc_url = jdbc_url,
                                                                    valid_environments = valid_environments,
                                                                    valid_schemas = valid_schemas,
                                                                    valid_write_modes = valid_write_modes,
                                                                    valid_prefix_tables = valid_prefix_tables,
                                                                    write_mode = write_mode,
                                                                    partitionColumn = partition_cols,
                                                                    dbutils = dbutils,
                                                                    logger = logger     
            )          
            msg = f"Tabela {final_table_name} gravada em {full_schema_delta_path}"
            if logger:
                logger.info(msg)

            # Registro no Metastore
            flg_result = proc_register_table_in_metastore(env = env,
                                                          spark_session = spark_session,
                                                          schema_name = schema_name_destination,
                                                          table_name = final_table_name,
                                                          full_schema_delta_path = full_schema_delta_path,
                                                          mode = write_mode,
                                                          df = df_cleaned,
                                                          valid_environments = valid_environments,
                                                          valid_schemas = valid_schemas,
                                                          valid_write_modes = valid_write_modes,
                                                          logger = logger
            )
            if flg_result:
                msg = f"Processamento da tabela {table_name} finalizado com sucesso"
                if logger:
                    logger.info(msg)
            
            else:
                msg = f"Processamento da tabela {table_name} finalizado com falha"
                if logger:
                    logger.error(msg)
                raise ValueError(msg)

            # Conta registros inseridos
            records_inserted = df_cleaned.count()

            # Atualiza tabela de controle de ingestão
            flg_ingest_control = proc_upsert_on_etl_control_table(  env = env,
                                                                    spark_session = spark_session,
                                                                    schema_name_destination = schema_name_destination,
                                                                    table_name_destination = final_table_name,    
                                                                    step_name = "Ingestion",
                                                                    last_update_date = datetime.now(),
                                                                    delta_path = f"{azure_paths.get("audit")}",
                                                                    valid_environments = valid_environments,
                                                                    valid_schemas = valid_schemas,
                                                                    valid_ingestion_types = valid_ingestion_types,
                                                                    source_name = source_name,
                                                                    schema_name_origin = schema_name_source,
                                                                    table_name_origin = table_name,
                                                                    last_pk = None,
                                                                    records_inserted = records_inserted,
                                                                    status = "SUCCESS",
                                                                    notes = "Ingestão pela função proc_pattern_ingest_data_to_bronze_layer",
                                                                    ingestion_mode = ingestion_mode,
                                                                    logger = logger
            )       
            msg = f"Controle de ingestão atualizado: {flg_ingest_control}"
            if logger:
                logger.info(msg)

            # Atualiza tabela de metadados de governança
            flg_governance_ok = proc_create_metadata_for_table_on_governance_layer( env = env, 
                                                                                    spark_session = spark_session, 
                                                                                    df = df_cleaned,
                                                                                    table_name = final_table_name, 
                                                                                    schema_name = schema_name_destination, 
                                                                                    base_metadata_path = f"{azure_paths.get("governance")}", 
                                                                                    valid_environments = valid_environments, 
                                                                                    valid_schemas = valid_schemas, 
                                                                                    valid_write_modes = valid_write_modes,
                                                                                    user = None,
                                                                                    dbutils = dbutils,
                                                                                    logger = logger
            )
            msg = f"Criação de dados na tabela de governança: {flg_governance_ok}"
            if logger:
                logger.info(msg)
        return True

    except Exception as e:
        if logger:
            logger.exception(f"Processamento da tabela {table_name} finalizado com falha")
        raise e
    
    finally:
        try:
            # Limpa dataframe da memória imediatamente após uso
            flg_ok_clear = utl.util_delete_dataframe_variable_from_scope(
                ['df_cleaned', 'df_raw'],
                "both",
                logger
            )
        except Exception:
            pass
