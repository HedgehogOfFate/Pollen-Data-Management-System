import os
import datetime
import pandas as pd
import json
import sqlalchemy as sa
from sqlalchemy.sql import text
from sqlalchemy.exc import SQLAlchemyError
from db import get_engine
import time

def load_file(file_path):
    ext = os.path.splitext(file_path)[-1].lower()

    if ext == '.csv':
        try:
            df = pd.read_csv(file_path, sep=';', decimal=',')
        except:
            df = pd.read_csv(file_path)
    elif ext in ['.xls', '.xlsx']:
        df = pd.read_excel(file_path)
    elif ext == '.json':
        with open(file_path, 'r') as f:
            data = json.load(f)
        df = pd.json_normalize(data) if isinstance(data, list) else pd.json_normalize([data])
    else:
        raise ValueError("Unsupported file format")

    for col in df.columns:
        if isinstance(col, str) and col.startswith('Unnamed:'):
            if col == 'Unnamed: 1':
                df = df.rename(columns={col: 'Particle'})

    return df

def store_to_db(df, table_name='hirst_ltklai_bi_hourly_data'):
    engine = get_engine()
    temp_table_name = None

    try:
        if table_name == 'hirst_daily_particle_totals':
            renamed_columns = {}
            for col in df.columns:
                if isinstance(col, datetime.datetime):
                    date_str = col.strftime('%Y-%m-%d')
                    renamed_columns[col] = date_str

            if renamed_columns:
                df = df.rename(columns=renamed_columns)

            if 'Station' in df.columns:
                df = df.rename(columns={'Station': 'station'})
            if 'Particle' in df.columns:
                df = df.rename(columns={'Particle': 'particle'})

            with engine.connect() as connection:
                cols_query = f"""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = '{table_name}'
                """
                existing_columns = [row[0] for row in connection.execute(text(cols_query))]

                date_pattern = r'\d{4}-\d{2}-\d{2}'
                date_columns = [col for col in df.columns
                                if isinstance(col, str) and
                                pd.to_datetime(col, errors='coerce') is not pd.NaT and
                                col not in existing_columns]

                for date_col in date_columns:
                    try:
                        alter_query = f"""
                        ALTER TABLE {table_name}
                        ADD COLUMN IF NOT EXISTS "{date_col}" real
                        """
                        connection.execute(text(alter_query))
                        connection.commit()
                        print(f"Added new date column: {date_col}")
                    except Exception as e:
                        print(f"Error adding column {date_col}: {e}")
                        raise Exception(f"Failed to add column {date_col}: {e}")

        if table_name == 'hirst_ltklai_bi_hourly_data':
            if 'LTKLAI' in df.columns and not pd.api.types.is_datetime64_any_dtype(df['LTKLAI']):
                df['LTKLAI'] = pd.to_datetime(df['LTKLAI'])

            numeric_columns = ['00-02', '02-04', '04-06', '06-08', '08-10', '10-12',
                               '12-14', '14-16', '16-18', '18-20', '20-22', '22-24',
                               'Daily Total', 'Pollen Factor', 'Spores Factor']

            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')

        if table_name == 'hirst_ltsiau_bi_hourly_data':
            if 'LTSIAU' in df.columns and not pd.api.types.is_datetime64_any_dtype(df['LTSIAU']):
                df['LTSIAU'] = pd.to_datetime(df['LTSIAU'])

            numeric_columns = ['00-02', '02-04', '04-06', '06-08', '08-10', '10-12',
                               '12-14', '14-16', '16-18', '18-20', '20-22', '22-24',
                               'Daily Total', 'Pollen Factor', 'Spores Factor']

            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')

        if table_name == 'hirst_ltviln_bi_hourly_data':
            if 'LTVILN' in df.columns and not pd.api.types.is_datetime64_any_dtype(df['LTVILN']):
                df['LTVILN'] = pd.to_datetime(df['LTVILN'])

            numeric_columns = ['00-02', '02-04', '04-06', '06-08', '08-10', '10-12',
                               '12-14', '14-16', '16-18', '18-20', '20-22', '22-24',
                               'Daily Total', 'Pollen Factor', 'Spores Factor']

            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')

        if table_name == 'polen_sence_data':
            if 'time' in df.columns and not pd.api.types.is_datetime64_any_dtype(df['time']):
                df['time'] = pd.to_datetime(df['time'])

            numeric_columns = ['pollen', 'mold', 'plastic_particles']
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')

        with engine.connect() as connection:
            if table_name == 'hirst_ltklai_bi_hourly_data':
                try:
                    connection.execute(
                        text('CREATE INDEX IF NOT EXISTS idx_ltklai ON hirst_ltklai_bi_hourly_data("LTKLAI");'))
                except Exception as e:
                    print(f"Index creation skipped: {e}")
            elif table_name == 'hirst_ltsiau_bi_hourly_data':
                try:
                    connection.execute(
                        text('CREATE INDEX IF NOT EXISTS idx_ltsiau ON hirst_ltsiau_bi_hourly_data("LTSIAU");'))
                except Exception as e:
                    print(f"Index creation skipped: {e}")
            elif table_name == 'hirst_ltviln_bi_hourly_data':
                try:
                    connection.execute(
                        text('CREATE INDEX IF NOT EXISTS idx_ltviln ON hirst_ltviln_bi_hourly_data("LTVILN");'))
                except Exception as e:
                    print(f"Index creation skipped: {e}")

            try:
                connection.execute(text(f'CREATE INDEX IF NOT EXISTS idx_id_{table_name} ON {table_name}(id);'))
                print(f"Created index on ID column for {table_name}")
            except Exception as e:
                print(f"ID index creation skipped: {e}")

            cols_query = f"""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = '{table_name}'
            """
            db_col_info = {row[0]: row[1] for row in connection.execute(text(cols_query))}

            if 'id' in db_col_info:
                del db_col_info['id']

            if table_name == 'hirst_daily_particle_totals':
                valid_cols = [col for col in df.columns if col in db_col_info or
                              (isinstance(col, str) and
                               pd.to_datetime(col, errors='coerce') is not pd.NaT)]
            else:
                valid_cols = [col for col in df.columns if col in db_col_info]

            df = df[valid_cols]

            for col in valid_cols:
                if col not in db_col_info:
                    continue

                data_type = db_col_info[col]
                if data_type in ('real', 'double precision', 'numeric'):
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                elif data_type == 'date' and col in ['LTKLAI', 'LTSIAU', 'LTVILN']:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                elif data_type == 'timestamp without time zone' and col == 'time':
                    df[col] = pd.to_datetime(df[col], errors='coerce')

            print("Column types after conversion:")
            for col in df.columns:
                print(f"  - {col}: {df[col].dtype}")

            temp_table_name = f"temp_{table_name}_{int(time.time())}"
            df.to_sql(temp_table_name, engine, if_exists='replace', index=False)

            if table_name == 'hirst_daily_particle_totals':
                key_columns = ['station', 'particle']
            elif table_name == 'hirst_ltklai_bi_hourly_data':
                key_columns = ['LTKLAI', 'Particle']
            elif table_name == 'hirst_ltsiau_bi_hourly_data':
                key_columns = ['LTSIAU', 'Particle']
            elif table_name == 'hirst_ltviln_bi_hourly_data':
                key_columns = ['LTVILN', 'Particle']
            elif table_name == 'polen_sence_data':
                key_columns = ['time']
            else:
                raise ValueError(f"Unknown table: {table_name}")

            for key in key_columns:
                if key not in valid_cols:
                    raise ValueError(f"Key column '{key}' not found in the data. Available columns: {list(df.columns)}")

            row_count_before = connection.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()

            existing_cols_query = f"""
            SELECT column_name FROM information_schema.columns 
            WHERE table_name = '{table_name}'
            """
            existing_cols = [row[0] for row in connection.execute(text(existing_cols_query))]
            temp_cols_query = f"""
            SELECT column_name FROM information_schema.columns 
            WHERE table_name = '{temp_table_name}'
            """
            temp_cols = [row[0] for row in connection.execute(text(temp_cols_query))]

            common_cols = [col for col in temp_cols if col in existing_cols]
            columns_str = ", ".join([f'"{col}"' for col in common_cols])

            key_conditions = " AND ".join([f'a."{col}" = b."{col}"' for col in key_columns])
            update_columns = [col for col in common_cols if col not in key_columns]
            update_str = ", ".join([f'"{col}" = b."{col}"' for col in update_columns])

            if table_name == 'hirst_ltklai_bi_hourly_data' and 'LTKLAI' in valid_cols:
                key_conditions = key_conditions.replace(
                    'a."LTKLAI" = b."LTKLAI"',
                    'a."LTKLAI" = b."LTKLAI"::date'
                )
            elif table_name == 'hirst_ltsiau_bi_hourly_data' and 'LTSIAU' in valid_cols:
                key_conditions = key_conditions.replace(
                    'a."LTSIAU" = b."LTSIAU"',
                    'a."LTSIAU" = b."LTSIAU"::date'
                )
            elif table_name == 'hirst_ltviln_bi_hourly_data' and 'LTVILN' in valid_cols:
                key_conditions = key_conditions.replace(
                    'a."LTVILN" = b."LTVILN"',
                    'a."LTVILN" = b."LTVILN"::date'
                )
            elif table_name == 'polen_sence_data' and 'time' in valid_cols:
                key_conditions = key_conditions.replace(
                    'a."time" = b."time"',
                    'a."time" = b."time"::timestamp'
                )

            update_count = 0
            if update_columns:
                update_query = f"""
                UPDATE {table_name} a
                SET {update_str}
                FROM {temp_table_name} b
                WHERE {key_conditions}
                """
                update_result = connection.execute(text(update_query))
                connection.commit()
                update_count = update_result.rowcount
                print(f"  - {update_count} existing rows updated")

            insert_query = f"""
            INSERT INTO {table_name} ({columns_str})
            SELECT {columns_str} FROM {temp_table_name} b
            WHERE NOT EXISTS (
                SELECT 1 FROM {table_name} a
                WHERE {key_conditions}
            )
            """

            insert_result = connection.execute(text(insert_query))
            connection.commit()
            insert_count = insert_result.rowcount

            connection.execute(text(f"DROP TABLE IF EXISTS {temp_table_name}"))
            connection.commit()
            temp_table_name = None

            try:
                connection.execute(text(f"CLUSTER {table_name} USING idx_id_{table_name};"))
                connection.execute(text(f"ALTER TABLE {table_name} CLUSTER ON idx_id_{table_name};"))
                print(f"Table {table_name} clustered by ID - data is now physically ordered by ID")
            except Exception as e:
                print(f"Clustering operation failed: {e}")

            row_count_after = connection.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()

            print(f"Data processed for '{table_name}' table:")
            print(f"  - {len(df)} rows in file")
            print(f"  - {insert_count} new rows inserted")
            print(f"  - {update_count} existing rows updated")
            print(f"  - {row_count_after - row_count_before} net change in row count")

    except SQLAlchemyError as e:
        error_msg = f"Database error while processing {table_name}: {str(e)}"
        print(error_msg)
        raise Exception(error_msg)
    except Exception as e:
        error_msg = f"Error processing data for {table_name}: {str(e)}"
        print(error_msg)
        raise Exception(error_msg)
    finally:
        if temp_table_name is not None:
            try:
                with engine.connect() as connection:
                    connection.execute(text(f"DROP TABLE IF EXISTS {temp_table_name}"))
                    connection.commit()
                    print(f"Cleaned up temporary table: {temp_table_name}")
            except Exception as cleanup_error:
                print(f"Warning: Failed to clean up temporary table {temp_table_name}: {cleanup_error}")

def ingest_file(file_path, table_name=None):
    try:
        df = load_file(file_path)
        print(f"Loaded {len(df)} rows from {file_path}")
        print(f"Columns: {df.columns.tolist()}")

        if table_name is None:
            polen_sence_columns = ['time', 'pollen', 'mold', 'plastic_particles']
            if all(col in df.columns for col in ['time', 'pollen']):
                table_name = 'polen_sence_data'
            elif any(isinstance(col, datetime.datetime) for col in df.columns) or any(
                    isinstance(col, str) and pd.to_datetime(col, errors='coerce') is not pd.NaT
                    for col in df.columns):
                table_name = 'hirst_daily_particle_totals'
            else:
                time_period_columns = ['00-02', '02-04', '04-06', '06-08', '08-10', '10-12',
                                       '12-14', '14-16', '16-18', '18-20', '20-22', '22-24']

                if any(col in df.columns for col in time_period_columns):
                    if 'LTKLAI' in df.columns:
                        table_name = 'hirst_ltklai_bi_hourly_data'
                    elif 'LTSIAU' in df.columns:
                        table_name = 'hirst_ltsiau_bi_hourly_data'
                    elif 'LTVILN' in df.columns:
                        table_name = 'hirst_ltviln_bi_hourly_data'
                    else:
                        table_name = 'hirst_ltklai_bi_hourly_data'
                else:
                    raise ValueError(
                        "Could not determine appropriate table for this data format. Please check that your file contains the expected columns.")

        print(f"Determined table type: {table_name}")

        if table_name == 'hirst_daily_particle_totals':
            required_cols = ['station', 'particle']
            missing_cols = [col for col in required_cols if col not in df.columns and col.title() not in df.columns]
            if missing_cols:
                raise ValueError(f"File structure doesn't match {table_name}. Missing required columns: {missing_cols}")

        elif table_name in ['hirst_ltklai_bi_hourly_data', 'hirst_ltsiau_bi_hourly_data',
                            'hirst_ltviln_bi_hourly_data']:
            station_col = table_name.split('_')[1].upper()
            if station_col not in df.columns:
                raise ValueError(f"File structure doesn't match {table_name}. Missing required column: {station_col}")

        elif table_name == 'polen_sence_data':
            required_cols = ['time', 'pollen']
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                raise ValueError(f"File structure doesn't match {table_name}. Missing required columns: {missing_cols}")

        store_to_db(df, table_name)

    except Exception as e:
        error_msg = f"Failed to process {file_path}: {str(e)}"
        print(error_msg)
        raise Exception(error_msg)
