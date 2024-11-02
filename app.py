from flask import Flask, request, render_template, send_file, flash, redirect
import pandas as pd
from io import BytesIO
import webbrowser
from threading import Timer

app = Flask(__name__)
app.secret_key = 'supersecretkey'  # Required for flashing messages

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        try:
            # Get uploaded files
            weekly_report = request.files['weekly_report']
            storage_files = request.files.getlist('storage_reports')
            prev_report_1 = request.files.get('prev_report_1')
            prev_report_2 = request.files.get('prev_report_2')

            # Load data into DataFrames
            df_weekly = pd.read_excel(weekly_report)
            df_storages = [pd.read_excel(file) for file in storage_files]

            # Load previous reports if provided
            df_prev_1 = pd.read_excel(prev_report_1) if prev_report_1 else pd.DataFrame()
            df_prev_2 = pd.read_excel(prev_report_2) if prev_report_2 else pd.DataFrame()

            # Create the 'Key' column in the weekly report by combining 'SO#' and 'LI' columns as integers
            df_weekly['Key'] = df_weekly['SO#'].apply(lambda x: str(int(x)) if pd.notnull(x) else '') + \
                               df_weekly['LI'].apply(lambda x: str(int(x)) if pd.notnull(x) else '')

            # Create the 'Key' column in each storage report by combining 'So' and 'Li' columns as integers
            for i in range(len(df_storages)):
                df_storages[i]['Key'] = df_storages[i]['So'].apply(lambda x: str(int(x)) if pd.notnull(x) else '') + \
                                        df_storages[i]['Li'].apply(lambda x: str(int(x)) if pd.notnull(x) else '')

            # Create 'Key' column in previous reports if provided
            if not df_prev_1.empty:
                df_prev_1['Key'] = df_prev_1['SO#'].apply(lambda x: str(int(x)) if pd.notnull(x) else '') + \
                                   df_prev_1['LI'].apply(lambda x: str(int(x)) if pd.notnull(x) else '')

            if not df_prev_2.empty:
                df_prev_2['Key'] = df_prev_2['SO#'].apply(lambda x: str(int(x)) if pd.notnull(x) else '') + \
                                   df_prev_2['LI'].apply(lambda x: str(int(x)) if pd.notnull(x) else '')

            # Filter out rows with missing or empty 'Key' values in the weekly report
            df_weekly = df_weekly[df_weekly['Key'].notna() & (df_weekly['Key'] != '')]

            # Identify columns that represent dates
            date_columns = [col for col in df_weekly.columns if isinstance(col, str) and '/' in col]

            # Convert date column headers to datetime format for filtering
            date_columns_datetime = pd.to_datetime(date_columns, errors='coerce').dropna()
            date_columns = [col for col, date in zip(date_columns, date_columns_datetime) if pd.notnull(date)]

            # Get start and end date from the form
            start_date = pd.to_datetime(request.form['start_date'])
            end_date = pd.to_datetime(request.form['end_date'])

            # Format the sheet name as "StartDate-EndDate"
            sheet_name = f"{start_date.strftime('%b-%d')}_to_{end_date.strftime('%b-%d')}"

            # Filter date columns based on the selected date range
            selected_columns = [col for col in date_columns if start_date <= pd.to_datetime(col) <= end_date]

            # Ensure selected columns are numeric, coercing errors to NaN
            df_weekly[selected_columns] = df_weekly[selected_columns].apply(pd.to_numeric, errors='coerce')

            # Sum up planned quantities within the selected date range, ignoring NaNs
            df_weekly['Total Planned Qty'] = df_weekly[selected_columns].sum(axis=1)

            # Filter rows in the weekly report that have planned quantities greater than 0 for the selected date range
            df_weekly_filtered = df_weekly[df_weekly['Total Planned Qty'] > 0]

            # Group by 'Key' and aggregate 'Total Planned Qty', 'Module', 'Cell PSD', 'PED', and 'Delivery Date'
            df_weekly_grouped = df_weekly_filtered.groupby('Key').agg({
                'Total Planned Qty': 'sum',
                'Module': lambda x: ','.join(sorted(x.unique())),
                'Cell PSD': lambda x: ','.join(sorted(pd.to_datetime(x, errors='coerce').dropna().dt.strftime('%b-%d').unique())),
                'PED': lambda x: ','.join(sorted(pd.to_datetime(x, errors='coerce').dropna().dt.strftime('%b-%d').unique())),
                'Delivery Date': lambda x: ','.join(sorted(pd.to_datetime(x, errors='coerce').dropna().dt.strftime('%b-%d').unique()))
            }).reset_index()

            df_weekly_grouped.rename(columns={'Module': 'All Modules'}, inplace=True)

            # Combine all storage data into one DataFrame
            df_storage_combined = pd.concat(df_storages)

            # Calculate total stock for storage 118
            df_storage_118 = df_storage_combined[df_storage_combined['St Location'] == 118].groupby('Key')['Total Stock'].sum().reset_index()
            df_storage_118.rename(columns={'Total Stock': 'Total Stock (118)'}, inplace=True)

            # Calculate stock for storage 75 and 139 separately
            df_storage_75 = df_storage_combined[df_storage_combined['St Location'] == 75].groupby('Key')['Total Stock'].sum().reset_index()
            df_storage_75.rename(columns={'Total Stock': 'Stock (75)'}, inplace=True)

            df_storage_139 = df_storage_combined[df_storage_combined['St Location'] == 139].groupby('Key')['Total Stock'].sum().reset_index()
            df_storage_139.rename(columns={'Total Stock': 'Stock (139)'}, inplace=True)

            # Merge the grouped weekly report with storage data
            df_merged = pd.merge(df_weekly_grouped, df_storage_118, on='Key', how='left')
            df_merged = pd.merge(df_merged, df_storage_75, on='Key', how='left')
            df_merged = pd.merge(df_merged, df_storage_139, on='Key', how='left')

            # Fill NaN values in stock columns with 0
            df_merged['Total Stock (118)'] = df_merged['Total Stock (118)'].fillna(0)
            df_merged['Stock (75)'] = df_merged['Stock (75)'].fillna(0)
            df_merged['Stock (139)'] = df_merged['Stock (139)'].fillna(0)

            # Filter out rows with missing or empty 'Key' values in the final merged DataFrame
            df_merged = df_merged[df_merged['Key'].notna() & (df_merged['Key'] != '')]

            # Calculate shortage based on stock in storage 118
            df_merged['Shortage'] = df_merged['Total Planned Qty'] - df_merged['Total Stock (118)']
            df_shortages = df_merged[df_merged['Shortage'] > 0]

            # Merge with previous reports for comments if provided
            if not df_prev_1.empty:
                df_prev_1_comments = df_prev_1[['Key', 'Comment']].drop_duplicates()
                df_shortages = pd.merge(df_shortages, df_prev_1_comments, on='Key', how='left')

            if not df_prev_2.empty:
                df_prev_2_comments = df_prev_2[['Key', 'Comment']].drop_duplicates()
                df_shortages = pd.merge(df_shortages, df_prev_2_comments, on='Key', how='left', suffixes=('', '_from_prev_2'))

                # Combine comments from both reports
                df_shortages['Comment'] = df_shortages['Comment'].fillna('') + df_shortages['Comment_from_prev_2'].fillna('')
                df_shortages.drop(columns=['Comment_from_prev_2'], inplace=True)

            # Create an Excel report with the shortages
            output = BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                # Use the formatted date range as the sheet name
                df_shortages.to_excel(writer, index=False, sheet_name=sheet_name)

            output.seek(0)
            return send_file(output, download_name='shortage_report.xlsx', as_attachment=True)

        except Exception as e:
            flash(f"An error occurred: {e}")
            return redirect('/')

    return render_template('index.html')

# Automatically open the web app in the default browser when the server starts
def open_browser():
    webbrowser.open_new("http://127.0.0.1:5000/")

if __name__ == '__main__':
    Timer(1, open_browser).start()  # Opens the browser after 1 second
    app.run(debug=True)
