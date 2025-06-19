import pandas as pd
import traceback
import io
from flask import Flask, jsonify, render_template, request, send_file

app = Flask(__name__)

def sanitize_data_for_json(list_of_dicts):
    sanitized_list = []
    for row in list_of_dicts:
        clean_row = {}
        for key, value in row.items():
            if pd.isna(value):
                clean_row[key] = ''
            else:
                clean_row[key] = value
        sanitized_list.append(clean_row)
    return sanitized_list

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/get_columns', methods=['POST'])
def get_columns():
    file1, file2 = request.files.get('file1'), request.files.get('file2')
    if not file1 or not file2: return jsonify({'error': 'Please upload both files'}), 400
    try:
        df1, df2 = pd.read_excel(file1), pd.read_excel(file2)
        return jsonify({'columns1': list(df1.columns), 'columns2': list(df2.columns)})
    except Exception as e: return jsonify({'error': f"Error reading files: {str(e)}"}), 500

def perform_match(df1, df2, col1, col2, match_type):
    """ একটি হেল্পার ফাংশন যা ম্যাচিং লজিক পরিচালনা করে এবং তিনটি ডেটাফ্রেম ফেরত দেয়। """
    df1[col1], df2[col2] = df1[col1].astype(str), df2[col2].astype(str)
    
    # ডিফল্ট খালি ডেটাফ্রেম
    matched_df = pd.DataFrame()
    unmatched_file1_df = pd.DataFrame()
    unmatched_file2_df = pd.DataFrame()

    if match_type == 'inner':
        # শুধুমাত্র Exact Match
        matched_df = pd.merge(df1, df2, left_on=col1, right_on=col2, how='inner')
    else:
        # বাকি সব টাইপের জন্য 'outer' join প্রয়োজন
        merged_df = pd.merge(df1, df2, left_on=col1, right_on=col2, how='outer', indicator=True)
        
        if match_type == 'full':
            matched_df = merged_df[merged_df['_merge'] == 'both']
            unmatched_file1_df = merged_df[merged_df['_merge'] == 'left_only']
            unmatched_file2_df = merged_df[merged_df['_merge'] == 'right_only']
        elif match_type == 'missing_in_1':
            unmatched_file2_df = merged_df[merged_df['_merge'] == 'right_only']
        elif match_type == 'missing_in_2':
            unmatched_file1_df = merged_df[merged_df['_merge'] == 'left_only']
        
        # indicator কলামটি বাদ দেওয়া
        if not matched_df.empty: matched_df = matched_df.drop(columns=['_merge'])
        if not unmatched_file1_df.empty: unmatched_file1_df = unmatched_file1_df.drop(columns=['_merge'])
        if not unmatched_file2_df.empty: unmatched_file2_df = unmatched_file2_df.drop(columns=['_merge'])

    return matched_df, unmatched_file1_df, unmatched_file2_df


@app.route('/match', methods=['POST'])
def match_data():
    file1, file2 = request.files.get('file1'), request.files.get('file2')
    col1, col2 = request.form.get('col1'), request.form.get('col2')
    match_type = request.form.get('match_type', 'full') # ডিফল্ট 'full'

    if not all([file1, file2, col1, col2]): return jsonify({'error': 'Missing required data.'}), 400
    try:
        df1, df2 = pd.read_excel(file1), pd.read_excel(file2)
        matched_df, unmatched1_df, unmatched2_df = perform_match(df1, df2, col1, col2, match_type)

        return jsonify({
            'matched': sanitize_data_for_json(matched_df.to_dict(orient='records')),
            'unmatched1': sanitize_data_for_json(unmatched1_df.to_dict(orient='records')),
            'unmatched2': sanitize_data_for_json(unmatched2_df.to_dict(orient='records'))
        })
    except Exception as e:
        traceback.print_exc()
        return jsonify({'error': f"An error occurred during matching: {str(e)}"}), 500

@app.route('/export', methods=['POST'])
def export_results():
    file1, file2 = request.files.get('file1'), request.files.get('file2')
    col1, col2 = request.form.get('col1'), request.form.get('col2')
    match_type = request.form.get('match_type', 'full')

    if not all([file1, file2, col1, col2]): return "Error: Missing data for export.", 400
    try:
        df1, df2 = pd.read_excel(file1), pd.read_excel(file2)
        matched_df, unmatched1_df, unmatched2_df = perform_match(df1, df2, col1, col2, match_type)

        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            if not matched_df.empty: matched_df.to_excel(writer, sheet_name='Matched Rows', index=False)
            if not unmatched1_df.empty: unmatched1_df.to_excel(writer, sheet_name='Unmatched in File 1', index=False)
            if not unmatched2_df.empty: unmatched2_df.to_excel(writer, sheet_name='Unmatched in File 2', index=False)
        output.seek(0)

        return send_file(output, as_attachment=True, download_name='comparison_results.xlsx',
                         mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    except Exception as e:
        traceback.print_exc()
        return f"An error occurred during export: {str(e)}", 500

if __name__ == '__main__':
    app.run(debug=True)