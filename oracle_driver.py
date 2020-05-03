import cx_Oracle
from config import oracle as oracle_config

""" ---------------------------------------------------------------------------------------- """
""" Oracle """

user = oracle_config.USER
pwd  = oracle_config.PWD
ipdb = oracle_config.CONNECTION_STRING

def get_connection():
    """ Obtiene la conexion a Oracle """
    try:
        #connection = cx_Oracle.connect(user + '/' + pwd + '@' + ipdb)
        connection =  cx_Oracle.connect(user, pwd, ipdb, encoding="UTF-8", nencoding="UTF-8")
        connection.autocommit = False
        #log.root_logger.info('Connection Opened')
        return connection
    except Exception as e:
        #log.root_logger.error('Exception: ' + str(e))
        pass


def execute_query(connection, string_sql):
    try:
        cursor_pe = connection.cursor()
        cursor_pe.execute(string_sql)
        reg = cursor_pe.fetchone()
        cursor_pe.close()
        return reg
    except Exception as e:  
        #log.root_logger.error('Exception: ' + str(e))
        print(e)     


def create_lote_100(id_lote, df):
    mapa = []
    for row in df:
        if row is None:
            continue
        if row[0] is None:
            continue

        row[1] = int(row[1])
        row.append(id_lote)
        mapa.append(tuple(row))
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


def insertar_header_lote(df):
    try:
        connection = get_connection()
        try:
            #obtenemos la secuencia
            string_sql = "SELECT SEQUENCE1.nextval FROM dual"
            seq = execute_query(connection, string_sql)
            v_list = list(df)
            v_list[0] = seq[0]
            mapa = tuple(v_list)
            cursor_ins = connection.cursor()
            cursor_ins.execute("""
                INSERT INTO MELI1.LOTE_SITE
                    (ID_LOTE, 
                    NOMBRE_ARCHIVO, 
                    ENCODE, 
                    EXTENSION, 
                    SEPARADOR_COLUMNA, 
                    FECHA_INSERCION, 
                    ESTADO)
                VALUES
                    ( :1, :2, :3, :4, :5, sysdate, :6)
            """, mapa)
            connection.commit()
            return seq[0]
        except Exception as e:
            #log.root_logger.error('Exception: ' + str(e))
            if connection is not None:
                connection.rollback()
            if cursor_ins is not None:
                cursor_ins.close()
    finally:
        close_connection(connection)


def insertar_en_detalle_lote_site(cursor_ins, mapa):

    cursor_ins.executemany("""
        INSERT INTO meli1.DETALLE_LOTE_SITE
            (SITE, ID_ITEM, ID_LOTE)
        VALUES
            (:1, :2, :3)
    """,  mapa, batcherrors = True)


def save_dataset_to_oracle_bulk(id_lote, df):
    """ Guarda el dataframe obtenido en la db cada 100 registros """
    #log.root_logger.debug('saveDataFrameToOracle')
    try:
        connection = get_connection()        
        cursor_ins = connection.cursor()
        for mapa in create_lote_100(id_lote,df):
            try:
                insertar_en_detalle_lote_site(cursor_ins, mapa)
                #for errorObj in cursor_ins.getbatcherrors():
                print("Row in error: ", len(cursor_ins.getbatcherrors()))
                print(cursor_ins.rowcount)
                connection.commit()
            except cx_Oracle.DatabaseError as e:
                errorObj, = e.args
                print("Row", cursor_ins.rowcount, "has error", errorObj.message)        
        cursor_ins.close()
        connection.commit()
    except Exception as e:
        print(e)
        #log.root_logger.error('Exception: ' + str(e))        
        #update_cola(connection, id_lote, 'ER')
    finally:
        connection.commit()
        close_connection(connection)


def close_connection(connection):
    """ Cierra la conexion a Oracle """
    try:
        if connection is not None:
            connection.close()
            #log.root_logger.info('Connection Closed')
    except Exception as e:
        #log.root_logger.error('Exception: ' + str(e))
        pass