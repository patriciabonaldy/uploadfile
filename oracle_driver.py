import cx_Oracle

""" ---------------------------------------------------------------------------------------- """
""" Oracle """


def get_connection(user, pwd, ipdb):
    """ Obtiene la conexion a Oracle """
    try:
        #connection = cx_Oracle.connect(user + '/' + pwd + '@' + ipdb)
        connection =  cx_Oracle.connect(user, pwd, ipdb, encoding="UTF-8", nencoding="UTF-8")
        connection.autocommit = False
        log.root_logger.info('Connection Opened')
        return connection
    except Exception as e:
        log.root_logger.error('Exception: ' + str(e))


def get_first_pend_in_elastic(connection, string_sql):
    cursor_pe = connection.cursor()
    cursor_pe.execute(string_sql)
    reg = cursor_pe.fetchone()
    cursor_pe.close()
    return reg        


def create_lote_100(df):
    mapa = []
    for row in df:
        mapa = mapa.append((str(hipotesis), row['cuit'], monto if monto is not None else '0', str(corrida)))
        if len(mapa) == 100:
            yield mapa
    if len(mapa) > 0: 
        yield mapa   
        

def update_cola(connection, id_lote, estado):
    """ Actualiza el resultado del proceso """
    cursor_upd = connection.cursor()
    try:
        cursor_upd.execute("""
            UPDATE FISCAR.QUEUE_SPARK 
                SET ESTADO = '{estado}',
                FECHA_PROCESO = SYSDATE,
                REPROCESO = NVL(REPROCESO, 0) + 1 
            WHERE 
                ID_CORRIDA = {corrida} 
                AND ID_HIPOTESIS = {hipotesis} 
        """.format(estado=estado, corrida=corrida, hipotesis=hipotesis))
        connection.commit()
    except Exception as e:
        log.root_logger.error('Exception: ' + str(e))
    finally:
        if cursor_upd is not None:
            cursor_upd.close()


def save_dataset_to_oracle_bulk(df):
    """ Guarda el dataframe obtenido en la db cada 100 registros """
    log.root_logger.debug('saveDataFrameToOracle')
    try:
        connection = get_connection(user, pwd, ipdb)
        cursor_ins = connection.cursor()
        for mapa in create_lote_100(df):
            id_lote = insertar_lote_en_matriz_temporal(cursor_ins, mapa)
        cursor_ins.close()
    except Exception as e:
        #log.root_logger.error('Exception: ' + str(e))
        if connection is not None:
            connection.rollback()
        if cursor_ins is not None:
            cursor_ins.close()
        update_cola(connection, id_lote, 'ER')
        #log.root_logger.info('Procesado ER')


def close_connection(connection):
    """ Cierra la conexion a Oracle """
    try:
        if connection is not None:
            connection.close()
            log.root_logger.info('Connection Closed')
    except Exception as e:
        log.root_logger.error('Exception: ' + str(e))