from flask import Flask, render_template, send_file, request, redirect, flash
import os
import pandas as pd
from db import get_engine
from uploader import ingest_file
from statistics import fetch_data, compute_statistics, get_available_stations, validate_date_format
from werkzeug.utils import secure_filename
import io
import logging
from sqlalchemy import text
from datetime import datetime
import warnings

app = Flask(__name__)

log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

UPLOAD_FOLDER = 'uploads'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

TABLE_NAMES = {
    'hirst_ltklai_bi_hourly_data': 'LTKLAI HIRST Bi-hourly Data',
    'hirst_ltsiau_bi_hourly_data': 'LTSIAU HIRST Bi-hourly Data',
    'hirst_ltviln_bi_hourly_data': 'LTVILN HIRST Bi-hourly Data',
    'hirst_daily_particle_totals': 'HIRST Daily Particle Totals',
    'polen_sence_data': 'Pollen Sence Data'
}

DATE_COLUMN_MAPPING = {
    'hirst_ltklai_bi_hourly_data': 'LTKLAI',
    'hirst_ltsiau_bi_hourly_data': 'LTSIAU',
    'hirst_ltviln_bi_hourly_data': 'LTVILN',
    'hirst_daily_particle_totals': None,
    'polen_sence_data': 'time'
}


def get_date_column_for_table(table_name):
    return DATE_COLUMN_MAPPING.get(table_name.lower())


def filter_data_by_date(table_name, search_date, engine):
    date_column = get_date_column_for_table(table_name)

    if table_name.lower() == 'hirst_daily_particle_totals':
        with engine.connect() as conn:
            columns_query = text("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = :table_name 
                AND table_schema = 'public'
                ORDER BY ordinal_position
            """)
            columns_result = conn.execute(columns_query, {"table_name": table_name}).fetchall()
            all_columns = [row[0] for row in columns_result]

            date_columns = []
            for col in all_columns:
                try:
                    if len(col) >= 8 and ('-' in col or '/' in col):
                        for date_format in ['%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y']:
                            try:
                                parsed_date = datetime.strptime(col, date_format)
                                search_parsed = datetime.strptime(search_date, '%Y-%m-%d')
                                if parsed_date.date() == search_parsed.date():
                                    date_columns.append(col)
                                break
                            except ValueError:
                                continue
                except:
                    continue

            if date_columns:
                essential_columns = []
                for col in ['id', 'station', 'particle']:
                    if col in all_columns:
                        essential_columns.append(col)

                select_columns = essential_columns + date_columns
                columns_str = ', '.join([f'"{col}"' for col in select_columns])
                query = text(f'SELECT {columns_str} FROM {table_name} ORDER BY station, particle')
            else:
                essential_columns = []
                for col in ['id', 'station', 'particle']:
                    if col in all_columns:
                        essential_columns.append(col)

                if essential_columns:
                    columns_str = ', '.join([f'"{col}"' for col in essential_columns])
                    query = text(f'SELECT {columns_str} FROM {table_name} WHERE 1=0')
                else:
                    query = text(f'SELECT * FROM {table_name} WHERE 1=0')

            df = pd.read_sql(query, engine)

    elif date_column:
        if table_name.lower() == 'polen_sence_data':
            query = text(f"""
                SELECT * FROM {table_name} 
                WHERE DATE("{date_column}") = :search_date
                ORDER BY "{date_column}"
            """)
        else:
            query = text(f"""
                SELECT * FROM {table_name} 
                WHERE "{date_column}" = :search_date
                ORDER BY "{date_column}"
            """)

        df = pd.read_sql(query, engine, params={"search_date": search_date})
    else:
        df = pd.read_sql(f'SELECT * FROM {table_name}', engine)

    return df


@app.route('/')
def index():
    engine = get_engine()
    with engine.connect() as conn:
        tables = conn.execute(
            text("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
        ).fetchall()
        table_names = [row[0] for row in tables]

    return render_template('index.html', tables=table_names)


@app.route('/view/<table_name>')
def view_table(table_name):
    try:
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 50, type=int)
        search_date = request.args.get('search_date', '')
        sort_by = request.args.get('sort_by', 'id')
        sort_dir = request.args.get('sort_dir', 'asc')

        if sort_dir not in ['asc', 'desc']:
            sort_dir = 'asc'

        engine = get_engine()

        if search_date:
            try:
                datetime.strptime(search_date, '%Y-%m-%d')
                df = filter_data_by_date(table_name, search_date, engine)

                total_rows = len(df)

                if sort_by in df.columns:
                    ascending = (sort_dir == 'asc')
                    df = df.sort_values(by=sort_by, ascending=ascending)

                start_idx = (page - 1) * per_page
                end_idx = start_idx + per_page
                df = df.iloc[start_idx:end_idx]

            except ValueError:
                flash('Invalid date format. Please use YYYY-MM-DD format.', 'danger')
                search_date = ''
                df, total_rows = get_paginated_data(table_name, page, per_page, sort_by, sort_dir, engine)
        else:
            df, total_rows = get_paginated_data(table_name, page, per_page, sort_by, sort_dir, engine)

        total_pages = (total_rows + per_page - 1) // per_page

        table_html = df.to_html(classes='table table-striped table-hover', index=False)

        friendly_name = TABLE_NAMES.get(table_name, table_name)

        return render_template(
            'table_view.html',
            table_name=table_name,
            friendly_name=friendly_name,
            data=table_html,
            page=page,
            total_pages=total_pages,
            per_page=per_page,
            total_rows=total_rows,
            sort_by=sort_by,
            sort_dir=sort_dir,
            search_date=search_date
        )
    except Exception as e:
        flash(f'Error viewing table: {str(e)}', 'danger')
        return redirect('/')


def get_paginated_data(table_name, page, per_page, sort_by, sort_dir, engine):
    offset = (page - 1) * per_page

    with engine.connect() as conn:
        total_rows = conn.execute(
            text(f"SELECT COUNT(*) FROM {table_name}")
        ).scalar()

    with engine.connect() as conn:
        columns_query = text("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = :table_name 
            AND table_schema = 'public'
            ORDER BY ordinal_position
        """)
        columns_result = conn.execute(columns_query, {"table_name": table_name}).fetchall()
        available_columns = [row[0] for row in columns_result]

        if sort_by not in available_columns:
            if 'id' in available_columns:
                sort_by = 'id'
            elif available_columns:
                sort_by = available_columns[0]
            else:
                sort_by = '*'

    query = text(f'SELECT * FROM {table_name} ORDER BY "{sort_by}" {sort_dir} LIMIT :limit OFFSET :offset')
    df = pd.read_sql(query, engine, params={"limit": per_page, "offset": offset})

    return df, total_rows


@app.route('/stats/<table_name>', methods=['GET', 'POST'])
def view_stats(table_name):
    try:
        stations = get_available_stations(table_name)

        selected_station = request.args.get('station', None)
        selected_date = request.args.get('date', None)
        start_date = request.args.get('start_date', None)
        end_date = request.args.get('end_date', None)
        date_range_mode = request.args.get('date_range_mode', 'single')

        display_stats = False
        sample_html = ""
        stats_html = ""
        total_stats_html = ""
        has_total_stats = False

        date_range_display = None

        if date_range_mode == 'range' and start_date and end_date:
            formatted_start_date = validate_date_format(start_date)
            formatted_end_date = validate_date_format(end_date)

            if not formatted_start_date or not formatted_end_date:
                flash("Invalid date format. Please use YYYY-MM-DD format.", 'danger')
                return redirect(f'/stats/{table_name}')

            if formatted_start_date > formatted_end_date:
                flash("Start date must be before end date.", 'danger')
                return redirect(f'/stats/{table_name}')

            df, stats_df = fetch_data(
                table_name,
                selected_date=None,
                selected_station=selected_station,
                start_date=formatted_start_date,
                end_date=formatted_end_date
            )

            date_range_display = f"{formatted_start_date} to {formatted_end_date}"

            if not df.empty:
                if isinstance(stats_df, dict) and 'regular' in stats_df and 'total' in stats_df:
                    display_stats = True
                    has_total_stats = True

                    regular_stats_df = stats_df['regular']
                    if 'date' in regular_stats_df.columns:
                        regular_stats_df = regular_stats_df[
                            regular_stats_df['date'].str.contains('Overall', na=False)]
                    stats_html = regular_stats_df.round(2).to_html(
                        classes='table table-striped table-hover',
                        index=False if 'date' in regular_stats_df.columns else True
                    )

                    total_stats_df = stats_df['total']
                    if 'date' in total_stats_df.columns:
                        total_stats_df = total_stats_df[total_stats_df['date'].str.contains('Overall', na=False)]
                    total_stats_html = total_stats_df.round(2).to_html(
                        classes='table table-striped table-hover',
                        index=False if 'date' in total_stats_df.columns else True
                    )

                    for html_var in [stats_html, total_stats_html]:
                        if 'Overall' in str(html_var):
                            rows = html_var.split('</tr>')
                            if len(rows) > 2:
                                for i, row in enumerate(rows):
                                    if 'Overall' in row:
                                        styled_row = row.replace('<tr>', '<tr class="table-primary font-weight-bold">')
                                        rows[i] = styled_row
                                if html_var == stats_html:
                                    stats_html = '</tr>'.join(rows)
                                else:
                                    total_stats_html = '</tr>'.join(rows)

                elif isinstance(stats_df, pd.DataFrame) and 'date' in stats_df.columns:
                    display_stats = True

                    stats_df = stats_df[stats_df['date'].str.contains('Overall', na=False)]

                    stats_html = stats_df.to_html(classes='table table-striped table-hover', index=False)

                    if 'Overall' in str(stats_html):
                        rows = stats_html.split('</tr>')
                        for i, row in enumerate(rows):
                            if 'Overall' in row:
                                styled_row = row.replace('<tr>', '<tr class="table-primary font-weight-bold">')
                                rows[i] = styled_row
                        stats_html = '</tr>'.join(rows)
                else:
                    stats = compute_statistics(stats_df)
                    display_stats = True
                    stats_html = stats.round(2).to_html(classes='table table-striped table-hover')

                sample_html = df.head(10).to_html(classes='table table-striped table-hover', index=False)
            else:
                sample_html = "<p>No data available for the selected date range.</p>"
                stats_html = "<p>No statistics available.</p>"

        elif date_range_mode == 'single' and selected_date:
            formatted_date = validate_date_format(selected_date)
            if not formatted_date:
                flash(f"Invalid date format. Please use YYYY-MM-DD format.", 'danger')
                return redirect(f'/stats/{table_name}')

            df, stats_df = fetch_data(
                table_name,
                selected_date=formatted_date,
                selected_station=selected_station
            )

            date_range_display = formatted_date

            if not df.empty:
                if isinstance(stats_df, dict) and 'regular' in stats_df and 'total' in stats_df:
                    display_stats = True
                    has_total_stats = True

                    regular_stats_df = stats_df['regular']
                    if isinstance(regular_stats_df.index, pd.MultiIndex):
                        if 'Overall' in regular_stats_df.index.get_level_values(0):
                            regular_stats_df = regular_stats_df.loc[['Overall']]
                    elif 'date' in regular_stats_df.columns:
                        regular_stats_df = regular_stats_df[
                            regular_stats_df['date'].str.contains('Overall', na=False)]

                    stats_html = regular_stats_df.round(2).to_html(
                        classes='table table-striped table-hover'
                    )

                    total_stats_df = stats_df['total']
                    if isinstance(total_stats_df.index, pd.MultiIndex):
                        if 'Overall' in total_stats_df.index.get_level_values(0):
                            total_stats_df = total_stats_df.loc[['Overall']]
                    elif 'date' in total_stats_df.columns:
                        total_stats_df = total_stats_df[total_stats_df['date'].str.contains('Overall', na=False)]

                    total_stats_html = total_stats_df.round(2).to_html(
                        classes='table table-striped table-hover'
                    )

                    for html_var in [stats_html, total_stats_html]:
                        if 'Overall' in str(html_var):
                            rows = html_var.split('</tr>')
                            for i, row in enumerate(rows):
                                if 'Overall' in row:
                                    styled_row = row.replace('<tr>', '<tr class="table-primary font-weight-bold">')
                                    rows[i] = styled_row
                            if html_var == stats_html:
                                stats_html = '</tr>'.join(rows)
                            else:
                                total_stats_html = '</tr>'.join(rows)
                else:
                    stats = compute_statistics(stats_df)
                    display_stats = True

                    stats_html = stats.round(2).to_html(classes='table table-striped table-hover')

                sample_html = df.head(10).to_html(classes='table table-striped table-hover', index=False)
            else:
                sample_html = "<p>No data available for the selected date.</p>"
                stats_html = "<p>No statistics available.</p>"

        friendly_name = TABLE_NAMES.get(table_name, table_name)

        return render_template(
            'statistics_view.html',
            table_name=table_name,
            friendly_name=friendly_name,
            sample_data=sample_html,
            stats_data=stats_html,
            total_stats_data=total_stats_html,
            has_total_stats=has_total_stats,
            stations=stations,
            selected_station=selected_station,
            selected_date=selected_date,
            start_date=start_date,
            end_date=end_date,
            date_range_mode=date_range_mode,
            date_range_display=date_range_display,
            display_stats=display_stats
        )
    except Exception as e:
        flash(f'Error calculating statistics: {str(e)}', 'danger')
        return redirect('/')


@app.route('/download/<table_name>')
def download_table(table_name):
    engine = get_engine()

    search_date = request.args.get('search_date', '')
    sort_by = request.args.get('sort_by', 'id')
    sort_dir = request.args.get('sort_dir', 'asc')

    if sort_dir not in ['asc', 'desc']:
        sort_dir = 'asc'

    with engine.connect() as conn:
        columns_query = text("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = :table_name 
            AND table_schema = 'public'
            ORDER BY ordinal_position
        """)
        columns_result = conn.execute(columns_query, {"table_name": table_name}).fetchall()
        available_columns = [row[0] for row in columns_result]

        if sort_by not in available_columns:
            if 'id' in available_columns:
                sort_by = 'id'
            elif available_columns:
                sort_by = available_columns[0]

    if search_date:
        try:
            datetime.strptime(search_date, '%Y-%m-%d')
            df = filter_data_by_date(table_name, search_date, engine)

            if not df.empty and sort_by in df.columns:
                ascending = (sort_dir == 'asc')
                df = df.sort_values(by=sort_by, ascending=ascending)

        except ValueError:
            query = text(f'SELECT * FROM {table_name} ORDER BY "{sort_by}" {sort_dir}')
            df = pd.read_sql(query, engine)
    else:
        query = text(f'SELECT * FROM {table_name} ORDER BY "{sort_by}" {sort_dir}')
        df = pd.read_sql(query, engine)

    if 'id' in df.columns:
        df = df.drop('id', axis=1)

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, sheet_name=table_name, index=False)

        workbook = writer.book
        worksheet = writer.sheets[table_name]

        for i, col in enumerate(df.columns):
            max_len = max(
                df[col].astype(str).apply(len).max(),
                len(str(col))
            )
            worksheet.set_column(i, i, min(max(max_len + 2, 10), 50))

    output.seek(0)

    friendly_name = TABLE_NAMES.get(table_name, table_name).replace(' ', '_')

    if search_date:
        filename = f"{friendly_name}_{search_date}.xlsx"
    else:
        filename = f"{friendly_name}.xlsx"

    return send_file(
        output,
        download_name=filename,
        as_attachment=True,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )


@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        flash('No file part', 'danger')
        return redirect('/')

    file = request.files['file']
    table = request.form.get('table') or None

    if file.filename == '':
        flash('No selected file', 'danger')
        return redirect('/')

    filename = secure_filename(file.filename)
    filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)

    try:
        file.save(filepath)

        ingest_file(filepath, table)
        flash(f'Successfully uploaded and processed: {filename}', 'success')

    except Exception as e:
        if os.path.exists(filepath):
            try:
                os.remove(filepath)
            except:
                pass

        flash(f'Error processing file "{filename}": {str(e)}', 'danger')

    return redirect('/')

if __name__ == '__main__':
    app.run(debug=True)