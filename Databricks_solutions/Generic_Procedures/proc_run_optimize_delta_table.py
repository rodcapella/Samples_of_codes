# Executar o comando OPTIMIZE em uma tabela Delta para compactar pequenos arquivos, melhorando performance de leitura. Permite uso opcional do ZORDER BY para otimizar
# ordenação de colunas (ex: queries com filtros frequentes) e pode focar apenas numa partição.
def proc_run_optimize_delta_table(env: str, 
                                  spark_session: SparkSession, 
                                  schema_name: str, 
                                  table_name: str, 
                                  azure_configs_dict : dict,
                                  valid_environments, 
                                  valid_schemas,
                                  partition_columns: str | list [str] = None, 
                                  zorder_columns : str | list[str] = None, 
                                  stats_columns : str | list[str] = None, 
                                  logger = None
    ) -> dict:
    """
    Última revisão: 29/08/2025
    Testado em: 06/10/2025

    Executa o comando OPTIMIZE em uma tabela Delta para compactar pequenos arquivos, melhorando performance de leitura. Permite uso opcional do ZORDER BY para otimizar ordenação de colunas (ex: queries com filtros frequentes) e pode focar apenas numa partição. Retorna estatísticas da execução em formato dict.

    Parâmetros:
    ----------
        env : str
            Ambiente de execução (ex: 'dev', 'prod').
        spark_session : pyspark.sql.SparkSession
            Sessão Spark ativa para execução dos comandos.
        schema_name : str
            Nome do schema da tabela Delta (ex: 'bronze').
        table_name : str
            Nome completo da tabela Delta (ex: 'bronze.minha_tabela').
        azure_configs_dict : dict
            Dicionário com configurações do Azure.
        valid_environments : Iterable
            Interável com lista de ambientes válidos.   
        valid_schemas : Iterable
            Interável com lista de schemas válidos.
        partition_columns : str ou list[str] ou str, opcional
            Coluna(s) de partição para restringir a otimização.
            Se None, otimiza a tabela inteira.
        zorder_columns : str ou list[str], opcional
            Lista de colunas para aplicar ZORDER BY, otimizando a leitura.
            Se None, não aplica ZORDER BY.
        stats_columns : : str ou list[str], opcional
            Colunas para calcular estatísticas durante a otimização.
            Se None, não calcula estatísticas.
        logger : logging.Logger, opcional
            Logger para registrar eventos e erros.

    Retorno:
    --------
        dict com métricas do OPTIMIZE
        {
            "numFilesAdded": int,
            "numFilesRemoved": int,
            "bytesAdded": int,
            "bytesRemoved": int,
            "durationMs": int,
            ...
        }

    Use esta função se:
    -------------------
        - Deseja compactar arquivos pequenos em uma tabela Delta para melhorar performance.
        - Precisa otimizar a leitura por colunas específicas usando ZORDER BY.
        - Quer restringir a otimização a partições específicas.
        - Deseja calcular estatísticas durante a otimização.

    Exemplo de uso:
    ---------------
        success = proc_run_optimize_delta_table(
                        env,
                        spark_session,
                        "bronze"
                        "clientes",
                        azure_configs_dict,
                        valid_environments,
                        valid_schemas,
                        ["dt_partition"],
                        zorder_columns=["customer_id", "region"],
                        stats_columns = "customer_id, region"],
                        logger=logger
        )
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
    
    # Valida se azure_configs_dict é um dicionário válido e não vazio
    if not chk.check_if_is_valid_dict(azure_configs_dict):
        msg = "Parâmetro azure_configs_dict deve ser um dicionário não vazio."
        if logger:
            logger.error(msg)
        raise ValueError(msg)

    # Extrai os caminhos do dicionário de configurações
    azure_paths = {
        "bronze": azure_configs_dict.get("bronze_path"),
        "silver": azure_configs_dict.get("silver_path"),
        "gold": azure_configs_dict.get("gold_path"),
    }

    # Define o nome completo da tabela no metastore
    full_table_name = hlp.hlp_create_full_table_name(schema_name, table_name)   

    # Monta caminho do delta lake
    if schema_name.lower() == "bronze":
        full_delta_path = f"{azure_paths.get("bronze")}" + "tables/" + table_name

    elif schema_name.lower() == "silver":
        full_delta_path = f"{azure_paths.get("silver")}" + "tables/" + table_name

    elif schema_name.lower() == "gold":
        full_delta_path = f"{azure_paths.get("gold")}" + "tables/" + table_name

        # Melhora o desempenho em queries analíticas pesadas
        spark_session.sql(f"ALTER TABLE {full_table_name} SET TBLPROPERTIES ('delta.parquet.vorder.enabled' = 'true')")

    else:   
        msg = f"Schema '{schema_name}' inválido."
        if logger:
            logger.error(msg)
        raise ValueError(msg)

    # Validação se table_name é uma string não vazia
    if not chk.check_if_is_valid_string(table_name, True):
        msg = "Parâmetro table_name deve ser string não vazia."
        if logger:
            logger.error(msg)
        raise ValueError(msg)

    else:
        # Confirma se tabela existe no metastore
        if not infra.check_if_table_exists_in_delta_table(env, spark_session, full_delta_path, valid_environments, valid_schemas, logger):
            msg = f"Tabela Delta '{table_name}' não existe no metastore."
            if logger:
                logger.error(msg)
            raise ValueError(msg)

    partition_columns_lst = None
    if partition_columns:
        # Validar e padroniza o parâmetro partition_columns. 
        partition_columns_lst = utl.util_validate_and_get_columns_list(partition_columns, logger)

        # Valida se todas as colunas indicadas existem na tabela Delta
        if not infra.check_if_columns_exists_in_delta_table(env, spark_session, valid_environments, valid_schemas, full_delta_path, partition_columns_lst, logger):
            msg = f"Colunas {partition_columns_lst} não existem na tabela do caminho {full_delta_path}."
            if logger:
                logger.error(msg)
            raise ValueError(msg)

    zorder_columns_lst = None
    # Validação de zorder_columns            
    if zorder_columns:
        # zorder_columns também deve ser uma lista de strings mesmo que contenha somente um elemento
        zorder_columns_lst = utl.util_validate_and_get_columns_list(zorder_columns, logger)
       
        # Valida se todas as colunas indicadas existem na tabela Delta
        if not infra.check_if_columns_exists_in_delta_table(env, spark_session, valid_environments, valid_schemas, full_delta_path, zorder_columns_lst, logger):
            msg = f"Colunas {zorder_columns_lst} não existem na tabela do caminho {full_delta_path}."
            if logger:
                logger.error(msg)
            raise ValueError(msg)

    stats_columns_lst = None
    # Validação de stats_columns            
    if stats_columns:
        # stats_columns também deve ser uma lista de strings mesmo que contenha somente um elemento
        stats_columns_lst = utl.util_validate_and_get_columns_list(stats_columns, logger)
       
        # Valida se todas as colunas indicadas existem na tabela Delta
        if not infra.check_if_columns_exists_in_delta_table(env, spark_session, valid_environments, valid_schemas, full_delta_path, stats_columns_lst, logger):
            msg = f"Colunas {stats_columns_lst} não existem na tabela do caminho {full_delta_path}."
            if logger:
                logger.error(msg)
            raise ValueError(msg)

    try:
        spark_session.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
        spark_session.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
        if logger:
            logger.info("Auto-Optimize habilitado para writes futuros")

        # Monta comando SQL para OPTIMIZE
        optimize_query = f"OPTIMIZE {full_table_name}"

        # PARTITION handling
        partition_filter = None
        if partition_columns_lst:
            if not all(chk.check_if_is_valid_string(col) for col in partition_columns_lst):
                msg = "Lista de partições inválida."
                if logger:
                    logger.error(msg)
                raise ValueError(msg)

            partition_filter = " AND ".join(
                f"{col} >= (SELECT MIN({col}) FROM {full_table_name})"
                for col in columns
            )

        if partition_filter:
            optimize_query += f" WHERE {partition_filter}"

        if zorder_columns_lst:
            optimize_query += f" ZORDER BY ({', '.join(zorder_columns_lst)})"

    except Exception as e:
        if logger:
            logger.exception(f"Erro da construção da query no processo de OPTIMIZE: {e}")
        raise e

    # EXECUÇÃO
    try:
        if logger:
            logger.info(f"Executando comando: {optimize_query}")

        result_df = spark_session.sql(optimize_query)
        row = result_df.first() 
        result_dict = row.asDict(recursive=True)
        
        # Valida se as stats_columns foram computadas corretamente e executa a computação, caso necessário
        if stats_columns_lst:
            spark_session.sql(f"ANALYZE TABLE {full_table_name} COMPUTE STATISTICS FOR COLUMNS {stats_columns_lst}")
            if logger:
                logger.info(f"Estatísticas computadas para colunas {stats_columns_lst}.")

        if logger:
            logger.info(f"OPTIMIZE concluído. Estatísticas: {result_dict}")
        return result_dict
    
    except Exception as e:
        if logger:
            logger.exception(f"Erro durante OPTIMIZE: {e}")
        raise e

    finally:
        try:
            # Limpa dataframe da memória imediatamente após uso
            flg_ok_clear = utl.util_delete_dataframe_variable_from_scope(
                ['result_df'],
                "both",
                logger
            )
        except Exception:
            pass 
