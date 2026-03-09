# Criar uma tabela Delta Lake física a partir de uma tabela existente no Metastore, somente se ainda não existir uma tabela Delta no destino
def proc_create_delta_table_from_metastore( env : str,
                                            spark_session: SparkSession,
                                            source_schema: str,
                                            source_table: str,
                                            target_schema: str,
                                            target_table: str,
                                            target_path: str,
                                            valid_environments,
                                            valid_schemas,
                                            valid_write_modes,
                                            logger = None
) -> bool:
    """
    Última revisão: 23/02/2026
    Testado em: 

    Cria uma tabela Delta Lake física a partir de uma tabela existente no Metastore,
    somente se ainda não existir uma tabela Delta no destino.

    Parâmetros:
    -----------
        env : str
            Ambiente atual.
        spark_session : SparkSession
            Sessão Spark ativa.
        source_schema : str
            Nome do schema de origem (ex.: 'bronze').
        source_table : str
            Nomeda tabela existente no Metastore ('matriculas_raw').
        target_schema : str
            Nome do schema de destino (ex.: 'silver').
        target_table : str
            Nome da nova tabela Delta.
        target_path : str
            Caminho físico base no storage (ex.: caminho do container Silver).
        valid_environments : Interável
            Lista de ambientes válidos.
        valid_schemas : Interável
            Lista de schemas válidos.
        valid_write_modes : Iterável
            Lista de modos de escrita válidos.
        logger : opcional
            Logger para registrar eventos e erros.

    Retorno:
    --------
        bool: True se a tabela for criada com sucesso, False se já existir em Delta Lake.

    Use esta função se:
    -------------------
        - A tabela Delta não existe no storage e deseja criar uma tabela Delta Lake a partir da tabela existente no Metastore.

    Exemplo de uso:
    ---------------
        proc_create_delta_table_from_metastore(
            env = "DEV",
            spark_session = spark_session,
            source_schema = "bronze",
            source_table = "matriculas_raw",
            target_schema = "silver",
            target_table = "matriculas",
            target_path = "/mnt/dados",
            valid_environments = ["DEV", "TEST", "PROD"],
            valid_schemas,
            valid_write_modes,
            logger = logger
        )
    """
    # Valida interáveis
    for name, value in [("valid_environments", valid_environments),
                        ("valid_schemas", valid_schemas),
                        {"valid_write_modes", valid_write_modes}]:
        if not hasattr(value, "__iter__"):
            raise TypeError(f"Esperava um iterável para {name}, mas recebeu {type(value)}")

    # Validar o ambiente de execução e garantir uma SparkSession válida.
    spark_session = chk.chk_validate_environment(env = env, spark_session = spark_session, valid_environments = valid_environments, logger = logger)
    
    # Validar opcionalmente o nome do schema e/ou da tabela para operações em Metastore, Delta Lake ou fontes externas.
    flg_ok_schema_table = chk.chk_validate_schema_and_table(schema_name = source_schema, table_name = source_table, valid_schemas = valid_schemas, logger = logger)
    if not flg_ok_schema_table:
        msg = f"Schema ou tabela inválidos: {source_schema}, {source_table}"
        if logger:
            logger.error(msg)
        raise ValueError(msg)

    # Validar opcionalmente o nome do schema e/ou da tabela para operações em Metastore, Delta Lake ou fontes externas.
    flg_ok_schema_table = chk.chk_validate_schema_and_table(schema_name = target_schema, table_name = target_table, valid_schemas = valid_schemas, logger = logger)
    if not flg_ok_schema_table:
        msg = f"Schema ou tabela inválidos: {target_schema}, {target_table}"
        if logger:
            logger.error(msg)
        raise ValueError(msg)

    # Valida se target_path é um caminho físico válido
    if not infra.check_if_is_physical_path(target_path):
        msg = f"O caminho '{target_path}' não é um caminho físico válido."
        if logger: 
            logger.error(msg)
        raise ValueError(msg)

    try:
        # Montar caminho completo do Delta Lake a partir do path base da camada.
        delta_path = hlp.hlp_get_full_delta_path(layer_path = target_path, table_name = target_table, flg_is_view = False)

        # Verifica se já existe tabela Delta no destino (pasta física)
        flg_delta_exists = infra.check_if_table_exists_in_delta_table(  env = env,
                                                                        spark_session = spark_session,
                                                                        full_delta_path = delta_path,
                                                                        valid_environments = valid_environments,
                                                                        valid_schemas = valid_schemas,
                                                                        logger = logger
        )

        # Define o nome completo da tabela no metastore
        full_table_name = hlp.hlp_create_full_table_name(target_schema, target_table)   

        if flg_delta_exists:
            # Verifica metastore
            flg_exists_in_metastore = infra.check_if_table_exists_in_metastore(env = env, spark_session = spark_session, schema_name = target_schema, table_name = target_table,
                                                                                    valid_environments = valid_environments, valid_schemas = valid_schemas, logger = logger
            )
            if flg_exists_in_metastore:
                # Obtém location atual
                current_location =  utl.util_get_table_location_from_metastore( env = env,
                                                                                spark_session = spark_session,
                                                                                full_table_name = full_table_name,
                                                                                valid_environments = valid_environments,
                                                                                valid_schemas = valid_schemas,
                                                                                logger = logger
                )           
                # Se location no metastore diferente do path físico, atualiza
                if current_location and current_location.rstrip('/') != delta_path.rstrip('/'):
                    msg = f"Location no metastore difere de {delta_path}, atualizando para o caminho correto."
                    if logger:
                        logger.warning(msg)

                    spark_session.sql(f"ALTER TABLE {full_table_name} SET LOCATION '{delta_path}'")

                else:
                    msg = f"Location no metastore já está correto: {current_location}"
                    if logger:
                        logger.info(msg)
            else:
                msg = f"Tabela {full_table_name} não existe no metastore."
                if logger:
                    logger.warning(msg)
                return False

        # Criar nome completo da tabela no formato `schema`.`table`
        full_source_table = hlp.hlp_create_full_table_name(schema_name = source_schema, table_name = source_table)

        # SEMPRE: Lê partições do metastore
        # MÉTODO 1: SHOW PARTITIONS (mais confiável)
        partition_columns_lst = []
        try:
            partitions = spark_session.sql(f"SHOW PARTITIONS {full_source_table}").limit(5).collect()
            if partitions:
                # Extrai nomes das colunas da primeira partição
                first_partition = partitions[0].partition
                partition_columns_lst = [p.split('=')[0] for p in first_partition.split('/')]
                partition_columns_lst = list(dict.fromkeys(partition_columns_lst))  # Remove duplicatas
                
                if logger: 
                    logger.info(f"Partições encontradas: {partition_columns_lst}")

        except Exception as e:
            if logger: 
                logger.warning(f"SHOW PARTITIONS falhou: {e}")
            
        # MÉTODO 2: DESCRIBE (fallback mais simples)
        if not partition_columns_lst:
            try:
                describe_rows = spark_session.sql(f"DESCRIBE EXTENDED {full_source_table}").collect()
                for row in describe_rows:
                    if "Partition Columns" in str(row.col_name) and row.data_type and row.data_type != "None":

                        # Extrai colunas da string "col1 STRING, col2 INT"
                        import re

                        cols_match = re.findall(r'([^\s,]+)', row.data_type)
                        partition_columns_lst = [col.split(':')[0] for col in cols_match if col != "STRING" and col != "INT"]
                        break

            except:
                pass
        
        # Lê tabela RESPEITANDO partições
        df = spark_session.table(full_source_table).persist(StorageLevel.MEMORY_AND_DISK)
        
        if logger:
            if partition_columns_lst:
                logger.info(f"{len(partition_columns_lst)} partições detectadas: {partition_columns_lst}")
            else:
                logger.info("Tabela sem partições")
        
        if logger:
            logger.info(f"Criando nova tabela Delta em: {delta_path}")

        userMetadata = f"Salvo por proc_create_delta_table_from_metastore para {target_table} no ambiente {env}"

        # Escrita Delta
        flg_success = proc_save_df_to_table_on_metastore(
                                    env = env,
                                    spark_session = spark_session,
                                    df = df,
                                    schema_name = target_schema, 
                                    table_name = target_table,
                                    valid_environments = valid_environments, 
                                    valid_schemas = valid_schemas,
                                    valid_write_modes = valid_write_modes,
                                    target_type = "path",
                                    full_delta_path = delta_path,  # ← Caminho físico
                                    mode = "overwrite",
                                    merge_schema = False,
                                    overwrite_schema = True,
                                    user_metadata = userMetadata,
                                    partition_columns_lst = partition_columns_lst,
                                    logger = logger
        )
        if not flg_success:
            msg = f"Não foi possível salvar dataframe da tabela {target_table} no caminho {delta_path}."
            if logger:
                logger.error(msg)
            raise ValueError(msg)
        
        else:
            msg = f"Tabela Delta {target_schema}.{target_table} criada com sucesso em {delta_path}."
            if logger:
                logger.info(msg)
            return True

    except Exception as e:
        msg = f"Erro ao criar tabela Delta a partir do metastore ({source_schema}.{source_table} → {target_schema}.{target_table}): {e}"
        if logger:
            logger.exception(msg)
        raise Exception(msg)
