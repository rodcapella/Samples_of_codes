# Ler tabela SQL Server via JDBC e retorna DataFrame Spark.
def proc_read_table_from_sql_server(env: str,
                                    spark_session: SparkSession,
                                    jdbc_url: str,
                                    sql_server_driver : str,
                                    table_name: str,
                                    schema_name: str,
                                    valid_environments,
                                    valid_schemas,
                                    custom_where_clause: str | None = None,
                                    logger = None
    ) -> 'DataFrame':
    """
    Última revisão: 30/09/2015
    Testado em: 09/10/2025

    Lê tabela SQL Server via JDBC e retorna DataFrame Spark.

    Parâmetros:
    ----------
        env: str
            Ambiente de execução.
        spark_session: SparkSession
            Sessão Spark.
        jdbc_url: str
            URL JDBC.
        sql_server_driver : str
            Driver JDBC 
        table_name: str
            Nome da tabela.
        schema_name: str
            Nome do esquema.
        valid_environments: iterável
            Valores válidos para o parâmetro env.
        valid_schemas: iterável
            Valores válidos para o parâmetro schema_name.
        custom_where_clause: str
            Cláusula WHERE personalizada, opcional.
        logger: Logger
            Objeto Logger, opcional.

    Retorna:
    -------
        df: DataFrame Spark

    Use esta função se:
    -------------------
        - Deseja ler uma tabela SQL Server via JDBC.
    
    Exemplo de uso:
    ---------------
        df = proc_read_table_from_sql_server(env, spark_session, jdbc_url, sql_server_driver, table_name, schema_name, valid_environments, valid_schemas, custom_where_clause, logger)
    """
    # Valida interáveis
    for name, value in [("valid_environments", valid_environments),
                        ("valid_schemas", valid_schemas)]:
        if not hasattr(value, "__iter__"):
            raise TypeError(f"Esperava um iterável para {name}, mas recebeu {type(value)}")

    # Validar o ambiente de execução e garantir uma SparkSession válida.
    spark_session = chk.chk_validate_environment(env = env, spark_session = spark_session, valid_environments = valid_environments, logger = logger)

    # Validar opcionalmente o nome do schema e/ou da tabela para operações em Metastore, Delta Lake ou fontes externas.
    flg_ok_schema_table = chk.chk_validate_schema_and_table(schema_name = schema_name, table_name = table_name, valid_schemas = valid_schemas, logger = logger)
    if not flg_ok_schema_table:
        msg = f"Schema ou tabela inválidos: {schema_name}, {table_name}"
        if logger:
            logger.error(msg)
        raise ValueError(msg)

    # Validar URL/driver JDBC SQL Server, opcionalmente write_mode, E SEMPRE testa a conexão.
    flg_ok_jdbc = infra.chk_validate_and_test_jdbc_sql_server( env = env, spark_session = spark_session, jdbc_url = jdbc_url, valid_environments = valid_environments,
            valid_write_modes = None, write_mode = None, sql_server_driver = sql_server_driver, logger = logger
    )
    if not flg_ok_jdbc:
        msg = f"Conexão JDBC SQL Server falhou: {jdbc_url}"
        if logger:
            logger.error(msg)
        raise ValueError(msg)
    
    if custom_where_clause:
        # Valida se custom_where_clause é uma string não vazia
        if not chk.check_if_is_valid_string(custom_where_clause, False):
            msg = f"O where clause '{custom_where_clause} não é uma string não vazia."
            if logger:
                logger.error(msg)
            raise ValueError(msg)

    # Padronização do nome da tabela
    full_table_name_sql = f"dbo.{table_name}"

    full_table_name_sql = f"{schema_name}.{table_name}"

    # Monta o SELECT para quando table_name for Transaction2
    if table_name.lower() == "transactions2":
        select_inner = f"""
            SELECT
                *,
                YEAR(CreationDateTime) AS Year,
                MONTH(CreationDateTime) AS Month
            FROM {schema_name}.{table_name}
            WHERE TransactionStatusID = 1
             AND ModuleID = 1
             AND PackID is not null
        """
        
        if logger:
            logger.info("Adicionando colunas 'Year' e 'Month' derivadas de 'CreationDateTime' (Transactions2).")

        # Adiciona filtro direto na base_query
        if custom_where_clause:
            select_inner += f" {custom_where_clause}"          
            full_query = select_inner

    else:
        full_query = f"SELECT * FROM {schema_name}.{table_name}"

    try:
        # Leitura direta
        if logger:
            logger.info(f"Iniciando leitura JDBC: {schema_name}.{table_name}")

        df = (spark_session.read.format("jdbc")
              .option("url", jdbc_url)
              .option("query", full_query)
              .option("driver", sql_server_driver)
              .option("fetchsize", "10000")  # Otimização extra
              .load())
        
        df.persist()  # MEMORY_AND_DISK_SER por default no Delta
        df.count()    # força materialização

        if logger:
            logger.info(f"JDBC concluído: Tabela {table_name} persistidos em memória/disco")  
        return df
    
    except Exception as e:
        msg = f"Erro ao ler '{full_table_name_sql}' via JDBC: {e}"
        if logger:
            logger.exception(msg)
        raise Exception(msg)

    finally:
        try:
            df.unpersist(blocking=False)  # Libera versão original
        except:
            pass
