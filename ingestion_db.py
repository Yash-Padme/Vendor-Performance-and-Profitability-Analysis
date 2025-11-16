import pandas as pd 
import os 
from sqlalchemy import create_engine

os.makedirs('logs', exist_ok=True)
import logging
import time

logging.basicConfig(
    filename='logs/data_ingestion.log',
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filemode="a" # a stands for append mode
)
engine = create_engine('sqlite:///vendor_performance.db', echo=False)
data_folder =  r"C:\Users\yashp\Downloads\Project - Vendor Performance Data Analytics End-To-End Project  SQL + Python + Power BI + Reporting\data"


def ingest_db(df , table_name , engine) :
    # this function will ingest the dataframe into db
    df.to_sql(table_name , con = engine , if_exists= 'append' , index = False)
for file in os.listdir(data_folder) :
    if '.csv' in file :
        df = pd.read_csv(os.path.join(data_folder , file))
        print(df.shape)


 


def load_raw_data():
    """
    This function loads CSVs as dataframes and ingests them into the db in chunks.
    """
    logging.info('Starting data ingestion process')
    total_start = time.time()

    for file in os.listdir(data_folder):
        if '.csv' in file:
            file_path = os.path.join(data_folder, file)
            table_name = file.split('.')[0] # Get table name from filename
            
            logging.info(f'Starting ingestion for {file} into table "{table_name}"')
            file_start = time.time()
            
            # Use an iterator to process the file in chunks
            chunk_iterator = pd.read_csv(file_path, chunksize=100000)
            
            # Ingest the first chunk with if_exists='replace' to create the table
            try:
                first_chunk = next(chunk_iterator)
                first_chunk.to_sql(table_name, con=engine, if_exists='replace', index=False)
                logging.info(f'Table "{table_name}" created and first chunk ingested.')
                
                # Ingest the rest of the chunks with if_exists='append'
                for chunk in chunk_iterator:
                    chunk.to_sql(table_name, con=engine, if_exists='append', index=False)

                file_end = time.time()
                logging.info(f'Successfully ingested {file} in {(file_end - file_start):.2f} seconds.')

            except StopIteration:
                logging.warning(f'File {file} is empty and was skipped.')
            except Exception as e:
                logging.error(f'An error occurred while processing {file}: {e}')

    total_end = time.time()
    logging.info(f'Entire data ingestion process completed in {(total_end - total_start)/60:.2f} minutes.')
    
if __name__ == '__main__':
    load_raw_data()

from sqlalchemy import inspect, text
ins = inspect(engine)
for t in ins.get_table_names():
    with engine.connect() as conn:
        cnt = conn.execute(text(f"SELECT COUNT(*) FROM \"{t}\"")).scalar()
    print(f"{t}: {cnt:,}")
