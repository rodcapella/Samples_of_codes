# Gravar um DataFrame Spark em uma tabela via JDBC (Azure SQL Server), com suporte a particionamento para otimizar a escrita de grandes volumes de dados.    
def proc_write_df_to_jdbc(env: str,
                          df: 'DataFrame',
                          spark_session: SparkSession,
                          jdbc_url: str,
                          sql_server_driver : str,
                          table_name: str,
                          valid_environments,
                          valid_schemas,
                          valid_write_modes,
                          partition_columns: str | list[str] = None,
                          mode: str = "append",
                          schema_name: str = "dbo",
                          target_partitions: int = 8,
                          logger = None
) -> bool:
    """
    Última revisão: 18/01/2026
    Testado em: 

    Grava um DataFrame Spark em uma tabela via JDBC (Azure SQL Server), com suporte a particionamento
    para otimizar a escrita de grandes volumes de dados.

    Parâmetros:
    -----------
        env : str
            Ambiente de execução.
        df : DataFrame
            DataFrame Spark a ser gravado via JDBC.
        spark_session : SparkSession
            Sessão Spark ativa.
        jdbc_url : str
            URL de conexão JDBC (ex: 'jdbc:sqlserver://server:1433;database=mydb').
        sql_server_driver : str
            Driver JDBC (ex: 'com.microsoft.sqlserver.jdbc.SQLServerDriver').
        table_name : str
            Nome da tabela de destino (sem schema).
        valid_environments : Iterable
            Iterável com os ambientes válidos.
        valid_schemas : Iterable
            Iterável com os schemas válidos.
        valid_write_modes : Iterable
            Iterável com os modos de escrita válidos ('append' ou 'overwrite').
        partition_columns : str ou list[str], opcional
            Coluna(s) de partição. Para JDBC, apenas a primeira coluna será usada.
        mode : str, opcional
            Modo de escrita ('append' ou 'overwrite'). Padrão: 'append'.
        schema_name : str, opcional
            Nome do schema no banco de dados. Padrão: 'dbo'.
        target_partitions : int, opcional
            Número de partições para otimizar a escrita. Padrão: 8.
        logger : logging.Logger, opcional
            Logger para registrar logs da operação.

    Retorno:
    --------
        bool
            True se a gravação for bem-sucedida, False caso contrário.
    
    Use esta função se:
    -------------------
        - Deseja gravar um DataFrame Spark em uma tabela SQL Server/Azure SQL via JDBC.
        - Deseja otimizar a escrita com particionamento quando houver grandes volumes de dados.
        - Deseja registrar logs da operação de gravação.

    Exemplo de uso:
    ---------------
        df = spark.createDataFrame([(1, "A"), (2, "B")], ["id", "name"])
        ok = proc_write_df_to_jdbc(
            env="dev",
            df=df,
            spark_session=spark,
            jdbc_url="jdbc:sqlserver://myserver:1433;database=mydb",
            sql_server_driver = sql_server_driver,
            table_name="customers",
            valid_environments=["dev", "prod"],
            valid_schemas = valid_schemas,
            valid_write_modes = valid_write_modes,
            partition_columns="id",
            mode="append",
            target_partitions=8,
            logger=logger
        )
    """
    # Validar o ambiente de execução e garantir uma SparkSession válida.
    spark_session = chk.chk_validate_environment(env = env, spark_session = spark_session, valid_environments = valid_environments, logger = logger)

    try:     
        # CONFIGS JDBC OBRIGATÓRIAS para otimização
        spark_session.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
        spark_session.conf.set("spark.sql.shuffle.partitions", "200")  # Para writes

        # Valida se o DataFrame é válido e não vazio
        if not chk.check_if_is_valid_dataframe(df):
            msg = "df deve ser um DataFrame Spark válido e não vazio."
            if logger:
                logger.error(msg)
            raise ValueError(msg)
        
        # Valida se o target_partitions é válido
        if not chk.check_if_is_valid_integer(target_partitions, True):
            msg = "target_partitions deve ser um inteiro não vazio e positivo."
            if logger: 
                logger.error(msg)
            raise ValueError(msg)

        if df.rdd.getNumPartitions() > target_partitions:
            df = df.coalesce(target_partitions)

        # Validar opcionalmente o nome do schema e/ou da tabela para operações em Metastore, Delta Lake ou fontes externas.
        flg_ok_schema_table = chk.chk_validate_schema_and_table(schema_name = schema_name, table_name = table_name, valid_schemas = valid_schemas, logger = logger)
        if not flg_ok_schema_table:
            msg = f"Schema ou tabela inválidos: {schema_name}, {table_name}"
            if logger:
                logger.error(msg)
            raise ValueError(msg)
            
        # Validar URL/driver JDBC SQL Server, opcionalmente write_mode, E SEMPRE testa a conexão.
        flg_ok_jdbc = infra.chk_validate_and_test_jdbc_sql_server( env = env, spark_session = spark_session, jdbc_url = jdbc_url, valid_environments = valid_environments,
            valid_write_modes = valid_write_modes, write_mode = mode, sql_server_driver = sql_server_driver, logger = logger
        )
        if not flg_ok_jdbc:
            msg = f"Conexão JDBC SQL Server falhou: {jdbc_url}"
            if logger:
                logger.error(msg)
            raise ValueError(msg)

        # Padroniza o nome da tabela
        standardize_table_name = utl.util_standardize_table_name(table_name)

        # Monta nome completo da tabela JDBC
        jdbc_table = standardize_table_name if schema_name.lower() == "dbo" else f"{schema_name}.{standardize_table_name}"
        
        # CONFIGS JDBC ULTRA-OTIMIZADAS
        options = {
            "url": jdbc_url,
            "dbtable": jdbc_table,
            "driver" : sql_server_driver,
            "batchsize": "10000",
            "numPartitions": str(target_partitions),
            "isolationLevel": "READ_UNCOMMITTED",
            "fetchsize": "1000",
            "queryTimeout": "3600"
        }
        
        # Valida e processa colunas de partição
        partition_columns_lst = None
        if partition_columns:
            partition_columns_lst = utl.util_validate_and_get_columns_list(partition_columns, logger)
            
            # Valida se colunas de partição existem no DataFrame
            if not chk.check_if_columns_exists_on_df(df, partition_columns_lst):
                msg = "Colunas de partição não existem no DataFrame."
                if logger: 
                    logger.error(msg)
                raise ValueError(msg)
        
            # Particionamento para JDBC aceita somente uma coluna
            partition_col = partition_columns_lst[0]
            
            if logger:
                logger.info(f"Gravando tabela '{jdbc_table}' via JDBC com particionamento na coluna '{partition_col}'.")

            # Obtém limites de partição da tabela
            lower_bound, upper_bound, num_partitions = utl.util_get_partition_bounds_on_df(df, partition_col)

            if lower_bound is not None and upper_bound is not None:
                # Gravação com opções de particionamento
                options.update({
                    "partitionColumn": partition_col,
                    "lowerBound": lower_bound,
                    "upperBound": upper_bound,
                    "numPartitions": str(num_partitions)
                })
                
                if logger:
                    logger.info(f"Tabela '{jdbc_table}' gravada via JDBC com particionamento (bounds: {lower_bound}-{upper_bound}, partitions: {num_partitions}).")

            else:
                # Sem particionamento se não for possível calcular bounds
                if logger:
                    logger.warning(f"Não foi possível calcular bounds para particionamento. Gravando sem particionamento.")
        
        # Normaliza colunas de data para JDBC
        df = utl.util_normalize_date_columns_on_df_to_jdbc_tables(df)

        # Gravação do datafram em JDBC no modo e opções informados
        df.write.format("jdbc") \
                .options(**options) \
                .mode(mode) \
                .save()
            
        if logger:
            logger.info(f"Tabela '{jdbc_table}' gravada via JDBC com sucesso.")
        return True

    except Exception as e:
        msg = f"Erro ao gravar tabela '{table_name}' via JDBC na função proc_write_df_to_jdbc: {e}"
        if logger:
            logger.exception(msg)
        raise Exception(msg)
