# Aplicar Slowly Changing Dimension (SCD) Tipo 2 em dataframes usada no processo de criação e atualização de dimensões no processo de ETL
def proc_generic_scd2_control_on_dimensions(df_new: 'DataFrame',
                                            df_current: 'DataFrame',
                                            key_columns: str | List[str],
                                            attribute_columns: str | List[str],
                                            surrogate_key_name: str,
                                            id_key_name : str,
                                            current_flag_column: str,
                                            logger = None
) -> 'DataFrame':
    """
    Última revisão: 16/01/2026
    Testado em: 

    Função genérica para aplicar Slowly Changing Dimension (SCD) Tipo 2 em dataframes usada no processo de criação e atualização de dimensões no processo de ETL.

    Parâmetros:
    -----------
        df_new: Dataframe
            DataFrame com os dados mais recentes.
        df_current: Dataframe
            DataFrame da dimensão atual com histórico.
        key_columns: str ou List[str]
            Lista das colunas de chave natural para identificar registros (ex: ['IDModule']).
        attribute_columns: str ou List[str] 
            Lista das colunas de atributos a serem comparadas para detectar mudanças.
        surrogate_key_name: str
            Nome da coluna que será a chave substituta (default 'SurrogateKey').
        id_key_name : str   
            Nome da coluna chave natural usada para identificar registros atuais no cross join (ex: "ID_Module")
        current_flag_column: str
            Nome da coluna de chave natural usada para identificar registros atuais no cross join (ex: "Flg_Is_Current")
        logger: Logger
            Objeto de logger para registrar logs.

    Retorno:
    --------
        df_result : DataFrame
            DataFrame atualizado com histórico SCD Tipo 2.

    Use esta função se:
    -------------------
        - Quando você deseja aplicar SCD Tipo 2 em uma dimensão em um processo de ETL.
        - Quando você deseja manter um histórico completo de alterações em uma dimensão.
        - Quando você deseja manter uma versão atualizada da dimensão com os histórico completo.

    Exemplo de uso:
    --------------
        df_new = spark.createDataFrame([
            (1, 'Brazil', 'America', 'America'),
            (2, 'USA', 'America', 'America'),
            (3, 'Canada', 'America', 'America')
        ], ['IDModule', 'Country', 'Continent', 'Region'])
        df_current = spark.createDataFrame([
            (1, 'Brazil', 'America', 'America', datetime(2023, 1, 1), datetime(2025, 1, 1), True),
            (2, 'USA', 'America', 'America', 'America', datetime(2023, 1, 1), datetime(2025, 1, 1), True),
            (3, 'Canada', 'America', 'America', datetime(2023, 1, 1), datetime(2025, 1, 1), True)
        ], ['SurrogateKey', 'Country', 'Continent', 'Region', 'Start_Date', 'End_Date', 'Flg_Is_Current'])
        df_result = proc_generic_scd2_control_on_dimensions(df_new, df_current, ['Country'], ['Continent', 'Region'], 'SurrogateKey', "ID_Module")
        display(df_result)  
    """
    # Colunas padrão controle SCD
    scd_fields = ['Start_Date', 'End_Date', 'Flg_Is_Current']

    now = F.current_timestamp()

    # Valida se o df_current é um dataframe válido e não vazio
    if not chk.check_if_is_valid_dataframe(df_current):
        msg = f"df_current vazio ou inválido."
        if logger:
            logger.error(msg)
        raise ValueError(msg)

    # Valida se o df_new é um dataframe válido e não vazio
    if not chk.check_if_is_valid_dataframe(df_new):
        if logger: 
            logger.warning("df_new vazio. Retornando df_current inalterado.")
        return df_current 

    # Validar e padronizar o parâmetros do tipo lista de strings ou strings para lista.
    key_columns_lst = utl.util_validate_and_get_columns_list(key_columns, logger)

    # Validar e padronizar o parâmetros do tipo lista de strings ou strings para lista.
    attribute_columns_lst = utl.util_validate_and_get_columns_list(attribute_columns, logger)
    
    # FILTRA COLUNAS QUE REALMENTE EXISTEM
    valid_attributes = [c for c in attribute_columns_lst 
                    if c in df_new.columns and c in df_current.columns]
    attribute_columns_lst = valid_attributes

    if not attribute_columns_lst:
        msg = "Nenhuma coluna de atributo válida para SCD2! Verifique attribute_columns."
        if logger:
            logger.error(msg)
        raise ValueError(msg)

    try:
        # Valida parâmetros obrigatórios
        for param, name in [(surrogate_key_name, "surrogate_key_name"), 
                        (id_key_name, "id_key_name"), (current_flag_column, "current_flag_column")]:
            if not chk.check_if_is_valid_string(param, True):
                msg = f"O parâmetro {name} é obrigatório e deve ser uma string não vazia."
                if logger:
                    logger.error(msg)
                raise ValueError(msg)

            # Preparação para Comparação
            df_current.persist(StorageLevel.MEMORY_AND_DISK)
            
            # Filtramos apenas os ativos para o Join de comparação
            df_curr_active = df_current.filter(F.col(current_flag_column) == True).alias("curr")
            df_new_distinct = df_new.dropDuplicates(key_columns_lst).alias("new")

            join_cond = [F.col(f"new.{k}") == F.col(f"curr.{k}") for k in key_columns_lst]
            df_joined = df_new_distinct.join(df_curr_active, on=join_cond, how="left")

            # Detecção de Mudanças (Logic Improvement)
            change_cond = None
            for col_name in attribute_columns_lst:
                # null-safe equality check
                cond = ~F.col(f"new.{col_name}").eqNullSafe(F.col(f"curr.{col_name}"))
                change_cond = cond if change_cond is None else change_cond | cond

            df_marked = df_joined.select(
                "new.*",
                F.col(f"curr.{surrogate_key_name}").alias("curr_sk"),
                when(F.col(f"curr.{surrogate_key_name}").isNull(), "NEW")
                .when(change_cond, "CHANGED")
                .otherwise("UNCHANGED").alias("change_type")
            ).persist(StorageLevel.MEMORY_AND_DISK)

            # Processamento de IDs
            max_id = df_current.agg({surrogate_key_name: "max"}).collect()[0][0] or 0
            
            # Criar Novos Registros (NEW + CHANGED)
            df_to_insert = df_marked.filter(F.col("change_type").isin("NEW", "CHANGED"))
            
            # Expiração de Registros Antigos (Somente os CHANGED)
            changed_keys = df_marked.filter(F.col("change_type") == "CHANGED").select("curr_sk")

            df_new_versions = (df_to_insert
                .withColumn(surrogate_key_name, F.row_number().over(Window.orderBy(key_columns_lst)) + F.lit(max_id))
                .withColumn("Start_Date", now)
                .withColumn("End_Date", F.lit(None).cast("timestamp"))
                .withColumn("Flg_Is_Current", F.lit(True))
                .drop("curr_sk", "change_type")
            )
            
            if changed_keys is None or changed_keys.count() == 0:
                if logger:
                    logger.info("SEM MUDANÇAS - mantendo df_current inalterado")

                df_result_final = df_current  # ← RETORNA ORIGINAL

            else:
                changed_count = changed_keys.count()
                if logger:
                    logger.info(f"Processando {changed_count} chaves alteradas")
            
                if changed_count == 0:
                    df_result_final = df_current

                else:
                    df_expired = (df_curr_active
                        .join(changed_keys, F.col(surrogate_key_name) == F.col("curr_sk"), "inner")
                        .withColumn("End_Date", now)
                        .withColumn("Flg_Is_Current", F.lit(False))
                        .select(df_current.columns)
                    )
                    
                    df_survivors_raw = df_current.join(changed_keys, 
                        df_current[surrogate_key_name] == changed_keys["curr_sk"], "left_anti")
                    
                    if df_survivors_raw.count() == 0:
                        if logger:
                            logger.warning("df_survivors vazio - usando df_current completo")
                        df_survivors = df_current

                    else:
                        df_survivors = df_survivors_raw
                        
                    # AGORA aplica ops com segurança
                    df_survivors = ops.operation_remove_duplicate_columns_on_df(df_survivors, logger)
                    df_expired = ops.operation_remove_duplicate_columns_on_df(df_expired, logger)
                    df_new_versions = ops.operation_remove_duplicate_columns_on_df(df_new_versions, logger)
                    
                    df_result_final = (df_survivors
                        .unionByName(df_expired, allowMissingColumns=True)
                        .unionByName(df_new_versions, allowMissingColumns=True))
                        
                    df_result_final = ops.operation_remove_duplicate_columns_on_df(df = df_result_final, logger = logger)

            return df_result_final
    
    except Exception as e:
        msg = f"Ocorreu um erro durante o processamento do SCD2: {str(e)}"
        if logger:
            logger.exception(msg)
        raise Exception(msg)

    finally:
        try:
            # Limpa dataframe da memória imediatamente após uso
            flg_ok_clear = utl.util_delete_dataframe_variable_from_scope(
                ['df_survivors', 'df_expired', 'df_new_versions', 'df_current', 'df_to_insert', 'df_marked'],
                "both",
                logger
            )
        except Exception:
            pass 
