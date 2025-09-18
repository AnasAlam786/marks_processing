import os
from flask import Flask, request, jsonify
from pandas import pandas as pd
from collections import OrderedDict


app = Flask(__name__)

# 🔑 Set API Key (use env variable in production!)
API_KEY = os.getenv("MARKS_API_KEY")


def add_grand_total(group):

    group["student_id"] = group.name
    total_subject_marks = OrderedDict()

    for subj_dict in group["subject_marks_dict"]:
        for subj, mark in subj_dict.items():
            try:
                mark = float(mark)
                total_subject_marks[subj] = total_subject_marks.get(subj, 0) + mark
            except:
                pass

    grand_total_row = group.iloc[0].copy()
    
    grand_total_row["exam_name"] = "G. Total"
    grand_total_row["exam_display_order"] = len(group)+1
    grand_total_row["exam_total"] = sum(total_subject_marks.values())
    grand_total_row["weightage"] = sum(group.weightage.values)
    grand_total_row["subject_marks_dict"] = total_subject_marks

    max_marks = int(grand_total_row["weightage"]) * len(grand_total_row["subject_marks_dict"])

    grand_total_row["percentage"] = int(
        (grand_total_row["exam_total"] / max_marks) * 100 if max_marks > 0 else 0
    )

    # Concatenate both
    return pd.concat([group, pd.DataFrame([grand_total_row])], ignore_index=True)


def process_marks(student_marks_data):


    student_marks_df = pd.DataFrame(student_marks_data)

    student_marks_df = student_marks_df.groupby("student_id", group_keys=False).apply(add_grand_total, include_groups=False).reset_index(drop=True)    
    student_marks_df['percentage'] = student_marks_df['percentage'].astype(int)


    all_columns = student_marks_df.columns.tolist()
    non_common_colums = ['exam_name', 'subject_marks_dict', 'exam_total', 'percentage', 'exam_display_order', 'weightage', "exam_term"]
    common_columns = [col for col in all_columns if col not in non_common_colums]


    def exam_info_group(df):
        df_sorted = df.sort_values('exam_display_order', na_position='last')
        
        ordered_exams = OrderedDict()
        for _, row in df_sorted.iterrows():
            ordered_exams[row['exam_name']] = {
                'subject_marks_dict': row['subject_marks_dict'],
                'exam_total': row['exam_total'],
                'percentage': row['percentage'],
                'weightage': row['weightage'],
                'exam_term': row['exam_term'],
            }
        return ordered_exams

    student_marks_df = (
            student_marks_df.groupby(common_columns)
            .apply(exam_info_group, include_groups=False)
            .reset_index(name="marks")
        )
    student_marks = student_marks_df.to_dict(orient='records')

    return student_marks



# -------------------------------
#   SECURITY CHECK
# -------------------------------
@app.before_request
def check_api_key():
    if request.endpoint == "process_marks_api":
        api_key = request.headers.get("X-API-Key")
        if api_key != API_KEY:
            return jsonify({"error": "Unauthorized"}), 401



@app.route("/process-marks", methods=["POST"])
def process_marks_api():
    try:
        data = request.get_json()
        if not data or "student_marks_data" not in data:
            return jsonify({"error": "Missing 'student_marks_data' in request"}), 400

        student_marks_data = data["student_marks_data"]
        result = process_marks(student_marks_data)
        return jsonify(result)

    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(debug=True)
