import pandas as pd
import numpy as np
from sqlalchemy import text
from db import get_engine
import datetime


def identify_date_layout(df):
    if 'time' in df.columns:
        return 'time'
    elif 'LTKLAI' in df.columns and pd.to_datetime(df['LTKLAI'], errors='coerce').notna().all():
        return 'LTKLAI'
    elif 'LTSIAU' in df.columns and pd.to_datetime(df['LTSIAU'], errors='coerce').notna().all():
        return 'LTSIAU'
    elif 'LTVILN' in df.columns and pd.to_datetime(df['LTVILN'], errors='coerce').notna().all():
        return 'LTVILN'
    else:
        return 'columns'


def compute_statistics(df, include_totals=False):
    if 'particle' in df.columns:
        if not include_totals:
            total_mask = df['particle'].astype(str).str.strip().str.upper() == 'TOTAL'
            df = df[~total_mask]
        else:
            total_mask = df['particle'].astype(str).str.strip().str.upper() == 'TOTAL'
            df = df[total_mask]

    return pd.DataFrame({
        "Average": df.mean(numeric_only=True),
        "Min": df.min(numeric_only=True),
        "Max": df.max(numeric_only=True),
        "Standard Deviation": df.std(numeric_only=True),
        "Median": df.median(numeric_only=True)
    })


def fetch_data(table_name, selected_date=None, selected_station=None, start_date=None, end_date=None):

    engine = get_engine()

    with engine.connect() as conn:
        if table_name == "hirst_daily_particle_totals" and selected_station:
            query = text(f'SELECT * FROM "{table_name}" WHERE station = :station')
            df = pd.read_sql(query, conn, params={"station": selected_station})
        else:
            df = pd.read_sql(text(f'SELECT * FROM "{table_name}"'), conn)

    if df.empty:
        return pd.DataFrame(), pd.DataFrame()

    layout = identify_date_layout(df)
    has_totals = table_name == "hirst_daily_particle_totals" and 'particle' in df.columns

    if layout in ['time', 'LTKLAI', 'LTSIAU', 'LTVILN']:
        df[layout] = pd.to_datetime(df[layout], errors='coerce')

        if selected_date:
            selected_date = pd.to_datetime(selected_date).date()
            df = df[df[layout].dt.date == selected_date]

        elif start_date and end_date:
            start_date_obj = pd.to_datetime(start_date).date()
            end_date_obj = pd.to_datetime(end_date).date()

            df = df[(df[layout].dt.date >= start_date_obj) &
                    (df[layout].dt.date <= end_date_obj)]

        if not df.empty:
            date_col = df[layout].copy()
            df = df.drop(columns=[layout])
            df.insert(0, 'date', date_col)

    elif layout == 'columns':
        date_columns = [col for col in df.columns if col.startswith('2024-')]
        non_date_columns = [col for col in df.columns if col not in date_columns]

        if selected_date:
            matching_columns = [col for col in date_columns if selected_date in col]
            if not matching_columns:
                df = pd.DataFrame()
            else:
                selected_cols = non_date_columns + matching_columns
                df = df[selected_cols]

        elif start_date and end_date:
            start_date_obj = pd.to_datetime(start_date)
            end_date_obj = pd.to_datetime(end_date)

            matching_columns = []
            date_mapping = {}

            for col in date_columns:
                try:
                    date_part = col.split(' ')[0]
                    col_date = pd.to_datetime(date_part)
                    if start_date_obj <= col_date <= end_date_obj:
                        matching_columns.append(col)
                        date_mapping[col] = col_date.strftime('%Y-%m-%d')
                except:
                    continue

            if matching_columns:
                selected_cols = non_date_columns + matching_columns
                df = df[selected_cols]

                stats_by_date = pd.DataFrame()

                for index, row in df.iterrows():
                    row_data = {col: row[col] for col in non_date_columns}

                    for date_col in matching_columns:
                        new_row = row_data.copy()
                        new_row['date'] = date_mapping[date_col]
                        new_row['value'] = row[date_col]

                        stats_by_date = pd.concat([stats_by_date, pd.DataFrame([new_row])], ignore_index=True)

                display_df = df.copy()

                stats_df = stats_by_date

                if has_totals:
                    result = {'regular': None, 'total': None}

                    regular_mask = stats_df['particle'].astype(str).str.strip().str.upper() != 'TOTAL'
                    stats_regular = stats_df[regular_mask]

                    total_mask = stats_df['particle'].astype(str).str.strip().str.upper() == 'TOTAL'
                    stats_total = stats_df[total_mask]

                    expected_columns = ['date', 'Average', 'Min', 'Max', 'Standard Deviation', 'Median']

                    if not stats_regular.empty:
                        try:
                            pivot_regular = stats_regular.pivot_table(
                                values='value',
                                index='date',
                                aggfunc=['mean', 'min', 'max', 'std', 'median']
                            )

                            pivot_regular.columns = ['Average', 'Min', 'Max', 'Standard Deviation', 'Median']
                            pivot_regular = pivot_regular.reset_index()

                            overall_regular = pd.DataFrame([{
                                'date': f'Overall Regular ({start_date} to {end_date})',
                                'Average': stats_regular['value'].mean(),
                                'Min': stats_regular['value'].min(),
                                'Max': stats_regular['value'].max(),
                                'Standard Deviation': stats_regular['value'].std(),
                                'Median': stats_regular['value'].median()
                            }])

                            result['regular'] = pd.concat([pivot_regular, overall_regular], ignore_index=True)
                        except Exception as e:
                            result['regular'] = pd.DataFrame([{
                                'date': f'Overall Regular ({start_date} to {end_date})',
                                'Average': stats_regular['value'].mean(),
                                'Min': stats_regular['value'].min(),
                                'Max': stats_regular['value'].max(),
                                'Standard Deviation': stats_regular['value'].std(),
                                'Median': stats_regular['value'].median()
                            }])
                    else:
                        result['regular'] = pd.DataFrame(columns=expected_columns)

                    if not stats_total.empty:
                        try:
                            pivot_total = stats_total.pivot_table(
                                values='value',
                                index='date',
                                aggfunc=['mean', 'min', 'max', 'std', 'median']
                            )

                            pivot_total.columns = ['Average', 'Min', 'Max', 'Standard Deviation', 'Median']
                            pivot_total = pivot_total.reset_index()

                            overall_total = pd.DataFrame([{
                                'date': f'Overall Totals ({start_date} to {end_date})',
                                'Average': stats_total['value'].mean(),
                                'Min': stats_total['value'].min(),
                                'Max': stats_total['value'].max(),
                                'Standard Deviation': stats_total['value'].std(),
                                'Median': stats_total['value'].median()
                            }])

                            result['total'] = pd.concat([pivot_total, overall_total], ignore_index=True)
                        except Exception as e:
                            result['total'] = pd.DataFrame([{
                                'date': f'Overall Totals ({start_date} to {end_date})',
                                'Average': stats_total['value'].mean(),
                                'Min': stats_total['value'].min(),
                                'Max': stats_total['value'].max(),
                                'Standard Deviation': stats_total['value'].std(),
                                'Median': stats_total['value'].median()
                            }])
                    else:
                        result['total'] = pd.DataFrame(columns=expected_columns)

                    return display_df, result

                try:
                    stats_df = stats_df.pivot_table(
                        values='value',
                        index='date',
                        aggfunc=['mean', 'min', 'max', 'std', 'median']
                    )

                    stats_df.columns = ['Average', 'Min', 'Max', 'Standard Deviation', 'Median']

                    stats_df = stats_df.reset_index()

                    overall_stats = pd.DataFrame([{
                        'date': f'Overall ({start_date} to {end_date})',
                        'Average': stats_by_date['value'].mean(),
                        'Min': stats_by_date['value'].min(),
                        'Max': stats_by_date['value'].max(),
                        'Standard Deviation': stats_by_date['value'].std(),
                        'Median': stats_by_date['value'].median()
                    }])

                    stats_df = pd.concat([stats_df, overall_stats], ignore_index=True)
                except Exception as e:
                    stats_df = pd.DataFrame([{
                        'date': f'Overall ({start_date} to {end_date})',
                        'Average': stats_by_date['value'].mean() if not stats_by_date.empty else np.nan,
                        'Min': stats_by_date['value'].min() if not stats_by_date.empty else np.nan,
                        'Max': stats_by_date['value'].max() if not stats_by_date.empty else np.nan,
                        'Standard Deviation': stats_by_date['value'].std() if not stats_by_date.empty else np.nan,
                        'Median': stats_by_date['value'].median() if not stats_by_date.empty else np.nan
                    }])

                return display_df, stats_df
            else:
                df = pd.DataFrame()
        else:
            df = df[non_date_columns + date_columns]

    if df.empty:
        if has_totals:
            empty_stats = pd.DataFrame(columns=['Average', 'Min', 'Max', 'Standard Deviation', 'Median'])
            return df, {'regular': empty_stats, 'total': empty_stats}
        else:
            return df, pd.DataFrame(columns=['Average', 'Min', 'Max', 'Standard Deviation', 'Median'])

    display_df = df.copy()

    if has_totals:
        try:
            stats_regular = df.copy()

            exclude_cols = ['id', 'station', 'particle', 'date']

            exclude_cols_lower = [col.lower() for col in exclude_cols]
            columns_to_drop = [col for col in stats_regular.columns
                               if col.lower() in exclude_cols_lower or
                               (isinstance(col, str) and col.lower().replace(' ', '') in ['pollenfactor',
                                                                                          'sporesfactor'])]

            stats_filtered = stats_regular.drop(columns=[col for col in columns_to_drop if col != 'particle'],
                                                errors='ignore')

            has_total_particles = any(stats_filtered['particle'].astype(str).str.strip().str.upper() == 'TOTAL')
            has_regular_particles = any(stats_filtered['particle'].astype(str).str.strip().str.upper() != 'TOTAL')

            regular_stats = pd.DataFrame()
            total_stats = pd.DataFrame()

            if has_regular_particles:
                regular_stats = compute_statistics(stats_filtered, include_totals=False)

            if has_total_particles:
                total_stats = compute_statistics(stats_filtered, include_totals=True)

            if regular_stats.empty and not total_stats.empty:
                regular_stats = pd.DataFrame(index=total_stats.index, columns=total_stats.columns)
            elif total_stats.empty and not regular_stats.empty:
                total_stats = pd.DataFrame(index=regular_stats.index, columns=regular_stats.columns)

            return display_df, {'regular': regular_stats, 'total': total_stats}
        except Exception as e:
            empty_stats = pd.DataFrame(columns=['Average', 'Min', 'Max', 'Standard Deviation', 'Median'])
            return display_df, {'regular': empty_stats, 'total': empty_stats}
    else:
        try:
            stats_df = df.copy()

            exclude_cols = ['id', 'station', 'particle', 'date']

            if table_name in ['hirst_ltklai_bi_hourly_data', 'hirst_ltsiau_bi_hourly_data',
                              'hirst_ltviln_bi_hourly_data']:
                exclude_cols.extend(['pollen factor', 'spores factor'])

            exclude_cols_lower = [col.lower() for col in exclude_cols]
            columns_to_drop = [col for col in stats_df.columns
                               if col.lower() in exclude_cols_lower or
                               (isinstance(col, str) and col.lower().replace(' ', '') in ['pollenfactor',
                                                                                          'sporesfactor'
                                                                                          ])]

            stats_df = stats_df.drop(columns=columns_to_drop, errors='ignore')

            return display_df, stats_df
        except Exception as e:
            return display_df, pd.DataFrame(columns=['Average', 'Min', 'Max', 'Standard Deviation', 'Median'])


def get_available_stations(table_name):
    engine = get_engine()
    with engine.connect() as conn:
        try:
            query = text(
                f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}' AND column_name = 'station'")
            result = conn.execute(query).fetchone()

            if result:
                stations = pd.read_sql(text(f'SELECT DISTINCT station FROM "{table_name}"'), conn)
                return stations['station'].tolist()
            return []
        except Exception:
            return []


def validate_date_format(date_str):
    try:
        date_obj = pd.to_datetime(date_str).date()
        return date_obj.strftime('%Y-%m-%d')
    except:
        return None