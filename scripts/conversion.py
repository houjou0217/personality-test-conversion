import pandas as pd
import os

input_csv = "/Users/shinnosuke/Downloads/exported_results_1739928610.csv"
output_folder = "output"
quiz_category_csv = "output/Quiz-Category.csv"

#=======================================問題抽出、分類プロセス===================================

def process_question_category(input_csv, output_folder):
    df = pd.read_csv(input_csv)
    # Question Title が NaN の行を削除
    df_questions = df.dropna(subset=["Question Title"])
    df_quiz_category = df_questions[["Question Title", "Question Category"]].drop_duplicates().reset_index(drop=True)
    df_quiz_category.insert(0, "ID", range(1, len(df_quiz_category) + 1))
    
    os.makedirs(output_folder, exist_ok=True)
    quiz_category_file_path = os.path.join(output_folder, "Quiz-Category.csv")
    df_quiz_category.to_csv(quiz_category_file_path, index=False, encoding='utf-8-sig')
    
    print(f"✅ outputのファイルパス : {quiz_category_file_path}")
    
process_question_category(input_csv, output_folder)

#==========================================性格診断CSV 修正プロセス=====================================================

def process_and_format_csv(input_csv, quiz_category_csv, output_folder):
    df = pd.read_csv(input_csv)
    df_quiz_category = pd.read_csv(quiz_category_csv)
    df['Timestamp'] = pd.to_datetime(df['Timestamp'], errors='coerce')
    
    # 各行に unique_id を付与
    df.insert(0, "unique_id", range(1, len(df) + 1))
    
    # User ID カラムの欠損値を一つ上の行で埋める
    df['User ID'].fillna(method='ffill', inplace=True)
    
    # number カラムを追加（User ID ごとにセッションを識別）
    df['session_change'] = (df['User ID'] != df['User ID'].shift()) | (df['Timestamp'].diff().dt.seconds > 1800)
    df['number'] = df['session_change'].cumsum()
    df.drop(columns=['session_change'], inplace=True)
    
    # Question Title ごとに result_id を quiz_category.csv の ID と紐付け
    df = df.merge(df_quiz_category[['ID', 'Question Title']], on='Question Title', how='left')
    df.rename(columns={'ID': 'result_id'}, inplace=True)
    
    # 出力ファイル名を Quiz/Survey の値にする（削除されるため事前に取得）
    output_file_name = df['Quiz/Survey'].dropna().unique()[0] if 'Quiz/Survey' in df.columns else "formatted_output"
    
    # Quiz/Survey カラムを削除
    if 'Quiz/Survey' in df.columns:
        df.drop(columns=['Quiz/Survey'], inplace=True)
    
    # 不要なカラムの削除
    columns_to_remove = ["IP Address", "Page URL", "User Email","name", "email", "comp", "phone", 
                        "Total Correct", "Total Questions", "Score",
                        "Question Right or Wrong", "Question Points Earned",
                        "Question Category"]
    df.drop(columns=[col for col in columns_to_remove if col in df.columns], inplace=True)
    
    # 数値カラムを適切に処理（数値があれば整数、なければnull）
    for col in df.select_dtypes(include=['float64', 'int64']).columns:
        df[col] = df[col].apply(lambda x: int(x) if not pd.isna(x) else pd.NA)
    
    # 出力フォルダが存在しない場合は作成
    os.makedirs(output_folder, exist_ok=True)
    
    # 整形されたCSVを保存
    output_file_path = os.path.join(output_folder, f"{output_file_name}.csv")
    df.to_csv(output_file_path, index=False, encoding='utf-8-sig')
    
    print(f"✅ CSV整形後のファイルパス : {output_file_path}")

process_and_format_csv(input_csv, quiz_category_csv, output_folder)

#==========================================性格診断CSV 整形プロセス=====================================================

def process_and_format_csv(input_csv, quiz_category_csv, output_folder):
    # CSVファイルの読み込み
    df = pd.read_csv(input_csv)
    df_quiz_category = pd.read_csv(quiz_category_csv)
    
    # timestamp を datetime 型に変換
    df['Timestamp'] = pd.to_datetime(df['Timestamp'], errors='coerce')
    
    # User ID カラムの欠損値を一つ上の行で埋める
    df['User ID'].fillna(method='ffill', inplace=True)
    
    # number カラムを追加（User ID ごとにセッションを識別）
    df['session_change'] = (df['User ID'] != df['User ID'].shift()) | (df['Timestamp'].diff().dt.seconds > 1800)
    df['number'] = df['session_change'].cumsum()
    df.drop(columns=['session_change'], inplace=True)
    
    # Question Title の前後のスペースを削除
    df['Question Title'] = df['Question Title'].str.strip()
    df_quiz_category['Question Title'] = df_quiz_category['Question Title'].str.strip()
    
    # Question Title ごとに result_id を quiz_category.csv の ID と紐付け
    df = df.merge(df_quiz_category[['ID', 'Question Title']], on='Question Title', how='left')
    df.rename(columns={'ID': 'result_id'}, inplace=True)
    
    # `Question Answer Provided` のデータ型を確認・修正
    df['Question Answer Provided'] = df['Question Answer Provided'].astype(str)
    
    # ピボットテーブルを利用して、Question Title をカラムとして整形
    pivot_df = df.pivot_table(
        index=['Timestamp', 'User ID', 'User Name', 'Comments Provided', 'Timer', 'number'],
        columns='Question Title',
        values='Question Answer Provided',
        aggfunc='first'
    ).reset_index()
    
    # カラムの階層をリセット
    pivot_df.columns.name = None
    
    # 出力フォルダが存在しない場合は作成
    os.makedirs(output_folder, exist_ok=True)
    
    # 整形されたCSVを保存
    output_file_path = os.path.join(output_folder, "formatted_output.csv")
    pivot_df.to_csv(output_file_path, index=False, encoding='utf-8-sig')
    
    print(f"Processed file saved to {output_file_path}")

# 使用例
input_conversion_csv = "output/202405_JPNAVI性格診断テスト.csv"  # 読み取るCSVファイル名
quiz_category_csv = "output/Quiz-Category.csv"  # クイズカテゴリファイル
output_folder = "output"  # 出力フォルダ
process_and_format_csv(input_conversion_csv, quiz_category_csv, output_folder)
