## Pollen Data Management System

This project is a web-based application for storing, managing, and analyzing environmental monitoring data. It allows uploading data files in various formats, saving them into a PostgreSQL database, calculating basic statistics, and exporting results in Excel format.

## Features

- Upload and process data from .csv, .xls/.xlsx, .json files.
- Store structured data into a PostgreSQL database.
- Automatically recognize file format and route to the correct table.
- Perform basic statistical analysis:
Average,
Minimum,
Maximum,
Standard deviation,
Median.
- Filter statistics by:
Specific date,
Date range,
Station (if applicable).
- View database tables and preview data online.
- Export full tables or statistics as Excel files.
- Flash message system for user feedback.

## Technologies Used:

Python, Flask, PostgreSQL, SQLAlchemy, Pandas, NumPy, XlsxWriter, HTML, CSS, JS and Bootstrap.	
