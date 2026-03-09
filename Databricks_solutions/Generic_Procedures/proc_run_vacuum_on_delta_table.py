# Executar o comando VACUUM em uma tabela Delta Lake e retorna métricas dos arquivos afetados.
def proc_run_vacuum_on_delta_table(env: str, 
                                   spark_session: SparkSession, 
                                   table_path_or_name: str, 
                                   valid_environments, 
                                   valid_schemas, 
                                   retention_hours: int = 168, 
                                   dry_run: bool = False, 
                                   logger = None
    ) -> dict:
    """
    Última revisão: 29/08/2025


    Executa VACUUM em uma tabela Delta Lake e retorna métricas dos arquivos afetados, com trava de segurança para retention_hours < 168, exceto em ambientes considerados seguros.

    Parâmetros:
    ----------
        env : str
            Ambiente de execução (ex: 'dev', 'prod').
        spark_session : pyspark.sql.SparkSession
            Sessão Spark ativa.
        table_path_or_name : str
            Caminho físico (ex: 'dbfs:/mnt/delta/tabela') ou nome da tabela Delta registrada (ex: 'silver.vendas').
        valid_environments : Iterable
            Interável de ambientes válidos.
        valid_schemas : Iterable
            Interável de esquemas válidos.
        retention_hours : int, opcional
            Quantidade de horas para retenção dos arquivos antigos. Default é 168 (7 dias).
        dry_run : bool, opcional
            Se True, executa VACUUM em modo simulação (não remove arquivos, apenas lista).
        logger : logging.Logger, opcional
            Logger para registrar informações e erros.

    Retorno:
    -------
        dict
            Dicionário contendo métricas da operação:
            - "tabela": caminho ou nome da tabela.
            - "total_arquivos": total de arquivos afetados (listados ou removidos).
            - "tamanho_total_MB": tamanho total em megabytes.
            - "tamanho_legivel": tamanho em formato legível (ex: MB, GB).
            - "dry_run": valor do parâmetro dry_run usado.

    Use esta função se:
    ------------------
        - Deseja limpar arquivos antigos de tabelas Delta para economizar espaço.
        - Quer monitorar o impacto do VACUUM antes da execução (dry_run).
        - Precisa das métricas de limpeza para auditoria ou logs.

    Exemplo de uso:
    ---------------
        metrics = proc_run_vacuum_on_delta_table(
            env,
            spark_session,
            table_path_or_name="dbfs:/mnt/delta/tabela",
            valid_environments,
            valid_schemas,
            retention_hours=168,
            dry_run=True,
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

    # Valida se table_path_or_name é uma string válida e não vazia
    if not chk.check_if_is_valid_string(table_path_or_name, False):
        msg = "Parâmetro table_path_or_name deve ser uma string válida e não vazia."
        if logger:
            logger.error(msg)
        raise ValueError(msg)

    # Divide 'schema.table' → (schema_name, table_name).
    schema_name, table_name =  hlp.hlp_parse_table_name(full_table_name = table_path_or_name) 

    # Validar opcionalmente o nome do schema e/ou da tabela para operações em Metastore, Delta Lake ou fontes externas.
    flg_ok_schema_table = chk.chk_validate_schema_and_table(schema_name = schema_name, table_name = table_name, valid_schemas = valid_schemas, logger = logger)
    if not flg_ok_schema_table:
        msg = f"Schema ou tabela inválidos: {schema_name}, {table_name}"
        if logger:
            logger.error(msg)
        raise ValueError(msg)

    # Verifica se a tabela Delta existe
    flg_exists = infra.check_if_table_exists_in_delta_table(env, spark_session, table_path_or_name, valid_environments, valid_schemas, logger)
    if not flg_exists:
        msg = f"Tabela Delta '{table_path_or_name}' não existe."
        if logger:
            logger.error(msg)
        raise ValueError(msg)

    # Ambientes em que retention abaixo de 168h é permitido
    safe_for_low_retention = {"dev"}

    # Validação de retenção segura
    if retention_hours:
        # Valida se retention_hours é um número inteiro positivo
        if not chk.check_if_is_valid_integer(retention_hours):
            msg = f"Parâmetro retention_hours deve ser um número inteiro positivo. Valor fornecido: {retention_hours}" 
            if logger:
                logger.error(msg)
            raise ValueError(msg)
        
        # Configura desabilitação da checagem de retenção para valores abaixo de 168 horas
        if retention_hours < 168:
            if env not in safe_for_low_retention:
                msg = (
                    f"ATENÇÃO: Retenção menor que 168h (valor fornecido: {retention_hours}) "
                    f"não é permitida no ambiente '{env}'. "
                    "Esta operação pode causar perda de dados não versionados para rollback. "
                    "Mantenha retention >= 168 em produção!"
                )
                if logger: 
                    logger.error(msg)
                raise ValueError(msg)

            else:
                if logger: 
                    logger.warning(
                        f"Executando VACUUM com retenção inferior a 168 horas em ambiente seguro ('{env}'). "
                        "Desativando checagem de segurança."
                    )            
                spark_session.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

    # Construção do comando VACUUM
    dry_run_clause = "DRY RUN" if dry_run else ""
    vacuum_query = f"VACUUM {table_identifier} RETAIN {retention_hours} HOURS {dry_run_clause}"
    
    if logger:
        logger.info(f"Executando: {vacuum_query}")
    
    try:
        # Executa comando VACUUM e obtém resultados como DataFrame
        result_df = spark_session.sql(vacuum_query)

        metrics = result_df.agg(
            F.sum("size").alias("total_bytes"),
            F.count("*").alias("total_files")
        ).collect()[0]
        
        size_mb = round((metrics["total_bytes"] or 0) / (1024**2), 2)
        
        if logger:
            logger.info(f"VACUUM: {metrics['total_files']:,} arquivos | {size_mb} MB {'estimado' if dry_run else 'removido'}")
        
        return {
            "table": table_path_or_name,
            "files_removed": metrics["total_files"] or 0,
            "size_mb": size_mb,
            "dry_run": dry_run,
            "retention_hours": retention_hours
        }

    except Exception as e:
        if logger:
            logger.exception(f"Erro ao executar VACUUM em {table_path_or_name}: {str(e)}")
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
