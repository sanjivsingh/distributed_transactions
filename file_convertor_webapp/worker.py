import time

from setup.kafka_setup import conversion_producer, config, constants
from commons import logger


def main():
    
    # from file_convertor_webapp import database
    #dbConn =  database.SqliteDatabaseConnection(reset=False)

    from .mysql_database import MysqlDatabaseConnection
    dbConn = MysqlDatabaseConnection(reset=True)
    dbConn.init_db()
    connection = dbConn.get_connection()

    from commons import logger
    log = logger.setup_logger(__name__)

    try:
        producer = conversion_producer.ConversionProducer(config.configurations)
        while True:
            #print_records()
            ## time.sleep(60) # Pause for 60 seconds
  
            with connection.cursor() as cursor:
                try:
                    #reqs = dbConn.get_pending_requests(limit = config.configurations[constants.DB_NULL_LIMIT])
                    cursor.execute(f"SELECT * FROM {dbConn.table} WHERE status = 'pending' LIMIT %s FOR UPDATE", (10))
                    reqs =  cursor.fetchall()

                    if reqs:
                        processed_ids = []
                        for req in reqs:
                            log.info(f"request found: {req}")
                            try:   
                                req_dict = { 
                                    "id" : req['id'] , 
                                    "filename" : req['filename'] , 
                                    "input_format" : req['input_format'], 
                                    "output_format" : req['output_format'], 
                                    "upload_path" : req['upload_path'], 
                                    "converted_path" : req['converted_path'], 
                                    "status" : req['status'], 
                                    "details" : req['details'], 
                                    "file_hash" : req['file_hash']
                                }
                                processed_ids.append( f"'{req['id']}'")
                                producer.push(req_dict)
                                log.info(f"pushed to broker: {req}")
                            except Exception as e:
                                log.error(f"error pushing message: {req}: {e}")
                        cursor.execute(f"""Update {dbConn.table} Set status = 'submitted' WHERE id in ( {", ".join(processed_ids)}) """)
                    else:
                        log.info("no requests found")
                    connection.commit()
                except Exception as e:
                    connection.rollback()
                    print(f"Error in get_pending_requests: {e}")
                finally:
                    log.info("waiting 60 seconds...")
                    time.sleep(10) # Pause for 60 seconds


            

    finally:
        pass 

if __name__ == "__main__":
    main()