# Adicionar colunas UUID e hash aos dados.   
# - UUID: identificador único para cada linha (útil para rastreabilidade).
# - Hash: resumo criptográfico SHA-256 das colunas especificadas, para detectar mudanças/fraudes.
def operation_add_uuid_and_hash_columns_on_df(df: 'DataFrame', 
                                              hash_columns: list[str], 
                                              name_uuid_column: str = "uuid", 
                                              name_hash_column: str = "hash_data", 
                                              logger = None
    ) -> 'DataFrame':
    """
    Última revisão: 18/01/2026
    Testado em: 

    Adicionar colunas UUID e hash aos dados.   
        - UUID: identificador único para cada linha (útil para rastreabilidade).
        - Hash: resumo criptográfico SHA-256 das colunas especificadas, para detectar mudanças/fraudes.

    Parâmetros:
    ----------
        df : pyspark.sql.DataFrame
            DataFrame de entrada.
        hash_columns : list[str]
            Lista de nomes das colunas usadas para gerar o hash.
        name_uuid_column : str, opcional
            Nome da coluna que receberá o UUID (default "uuid").
        name_hash_column : str, opcional
            Nome da coluna que receberá o hash SHA-256 (default "hash_data").
        logger : logging.Logger, opcional
            Logger para registrar informações de log (default=None).

    Retorno:
    -------
        new_df : pyspark.sql.DataFrame
            Novo DataFrame com as colunas UUID e hash adicionadas.

    Use esta função se:
    -------------------
        - Precisa adicionar colunas para rastreabilidade e verificação de integridade/detecção de alterações
        em linhas, usando UUIDs únicos e hashes criptográficos das colunas especificadas.

    Exemplo de uso:
    ---------------
        df_with_hash = operation_add_uuid_and_hash_columns_on_df(
                                df, 
                                hash_columns=["col1", "col2", "col3"],
                                name_uuid_column="uuid_col",
                                name_hash_column="hash_col",
                                logger=logger
        )
    """
    try:
        # DETECTA o que FALTA criar
        flg_needs_uuid = not chk.chk_validate_dataframe(df = df, required_columns = name_uuid_column, logger = logger)
        flg_needs_hash = not chk.chk_validate_dataframe(df = df, required_columns = name_hash_column, logger = logger)
        
        if logger:
            logger.info(f"UUID necessário: {flg_needs_uuid}, Hash necessário: {flg_needs_hash}")

        # Se TUDO já existe → retorna original
        if not flg_needs_uuid and not flg_needs_hash:
            if logger:
                logger.info("UUID e Hash já existem - retornando DataFrame original") 
            return df
        
        # Prepara expressões
        new_columns = []
        
        if flg_needs_uuid:
            uuid_expr = F.sha1(F.concat(
                F.lit(str(df.rdd.getNumPartitions())), 
                F.string(F.monotonically_increasing_id())
            ))

            new_columns.append(uuid_expr.alias(name_uuid_column))

            if logger:
                logger.info(f"Criando coluna UUID: {name_uuid_column}")
        
        if flg_needs_hash:
            hash_cols_concat = F.concat_ws("||", *[F.col(c) for c in hash_columns])
            hash_expr = F.sha2(hash_cols_concat, 256)
            new_columns.append(hash_expr.alias(name_hash_column))

            if logger:
                logger.info(f"Criando coluna Hash: {name_hash_column}")
        
        # SINGLE PASS seletivo
        if new_columns:
            df_new = df.select("*", *new_columns)

        else:
            df_new = df
            
        if logger:
            logger.info(f"Final: Foram adicionados {len(new_columns)} colunas: {', '.join([c.alias() for c in new_columns])}")
        return df_new
    
    except Exception as e:
        msg = f"Erro ao adicionar UUID e hash: {str(e)}"
        if logger: 
            logger.exception(msg)
        raise Exception(msg)

    finally:
        try:
            # Limpa dataframe da memória imediatamente após uso
            flg_ok_clear = utl.util_delete_dataframe_variable_from_scope(
                ["df"],
                "both",
                logger
            )
        except Exception:
            pass   
