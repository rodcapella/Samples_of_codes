# Lê uma tabela Delta registrada no metastore com base em schema e nome da tabela, garantindo leitura atualizada com REFRESH TABLE.
def proc_read_table_from_metastore(env: str, 
                                   spark_session: SparkSession, 
                                   schema_name: str, 
                                   table_name: str, 
                                   valid_environments, 
                                   valid_schemas, 
                                   partition_filters: dict = None,
                                   where_clause: str = None,
                                   logger = None
    ) -> 'DataFrame':
    """
    Última revisão: 03/02/2026
    Testado em: 

    Lê uma tabela Delta registrada no metastore com base em schema e nome da tabela,
    garantindo leitura atualizada com REFRESH TABLE.

    Parâmetros:
    -----------
        env : str
            Ambiente de execução.
        spark_session : SparkSession
            Sessão Spark ativa.
        schema_name : str
            Nome do schema (ex: 'silver').
        table_name : str
            Nome da tabela (ex: 'clientes').
        valid_environments : Interable
            Iterável com os ambientes válidos.
        valid_schemas : Interable
            Iterável com os schemas válidos.
        partition_filters : dict, opcional
            Dicionário com os filtros de partição (ex: {'ano': 2023, 'mes': 1}).
        where_clause : str, opcional
            Cláusula WHERE personalizada
        logger : logging.Logger, opcional
            Logger para registrar logs da operação.

    Retorno:
    --------
        df : pyspark.sql.DataFrame
            DataFrame com os dados atualizados da tabela.
    
    Use esta função se:
    -------------------
        - Deseja ler uma tabela Delta registrada no metastore com base em schema e nome da tabela.
        - Deseja garantir que a leitura seja atualizada com REFRESH TABLE.
        - Deseja evitar erros de leituras antigos.

    Exemplo de uso:
    ---------------
        df = proc_read_table_from_metastore(env, spark, "silver", "clientes", valid_environments, valid_schemas, partition_filters, where_clause, logger)
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

    # Define o nome completo da tabela no metastore
    full_table_name = hlp.hlp_create_full_table_name(schema_name, table_name)   

    if partition_filters:
        if not chk.check_if_is_valid_dict(partition_filters):
            msg = "partition_filters deve ser um dicionário."
            if logger:
                logger.error(msg)
            raise ValueError(msg)

    # Valida where_clause
    if where_clause:
        if not chk.check_if_is_valid_string(where_clause, False):
            msg = "where_clause deve ser uma string não vazia."
            if logger: 
                logger.error(msg)
            raise ValueError(msg)

    # Obtém colunas de partição usando função criada antes
    partition_columns = utl.util_get_partition_columns_from_metastore(env, spark_session, schema_name, table_name, valid_environments, valid_schemas, logger)

    try:
        if logger:
            logger.info(f"[READ TABLE] Atualizando cache da tabela: {full_table_name}")

        # Atualiza metastore
        spark_session.sql(f"REFRESH TABLE {full_table_name}")

        # Retorna DataFrame
        df = spark_session.table(full_table_name)

        # Aplica filtros sobre colunas de partição, se informado e válido
        if partition_filters:
            for key, value in partition_filters.items():
                if key not in partition_columns:
                    msg = f"Coluna '{key}' não é uma coluna de partição da tabela {full_table_name}."
                    if logger:
                        logger.warning(msg)

                else:
                    df = df.filter(f"{key} = {repr(value)}")

        # Aplica where_clause personalizada
        if where_clause:
            df = df.filter(where_clause)
            if logger:
                logger.info(f"[WHERE FILTER] Aplicado: {where_clause}")

        if logger:
            logger.info(f"[READ TABLE] Sucesso: {full_table_name}.")       
        return df
        
    except Exception as e:
        msg = f"[READ TABLE] Erro ao ler tabela '{full_table_name}': {e}"
        if logger:
            logger.exception(msg)
        raise e
