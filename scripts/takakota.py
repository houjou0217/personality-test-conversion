import pandas as pd

def get_file_name_keep(input_csv):
    df = pd.read_csv(input_csv)
    output_file_name = df['Quiz/Survey'].dropna().unique()[0] if 'Quiz/Survey' in df.columns else "formatted_output"
    return output_file_name

def process_quiz_data(input_file, output_file):
    # データの読み込み
    df = pd.read_csv(input_file)
    
    # `User ID` を前の値で埋める
    df["User ID"] = df["User ID"].fillna(method="ffill").astype(int)
    
    # メタデータの抽出
    meta_columns = ["User ID", "Timestamp", "name" ,"Quiz/Survey", "Total Correct", "Total Questions", "Score", "Timer"]
    meta_data = df[meta_columns].drop_duplicates(subset=["User ID"])
    
    # `User ID` を基準にして `Question Title` を列に展開し、`Question Answer Provided` を格納
    df_wide = df.pivot_table(
        index="User ID",
        columns="Question Title",
        values="Question Answer Provided",
        aggfunc="first"
    ).reset_index()
    
    # MultiIndex のカラム名をリセット
    df_wide.columns.name = None
    
    # メタデータと回答データを統合
    df_final = pd.merge(meta_data, df_wide, on="User ID", how="right")
    
    # CSVとして出力
    df_final.to_csv(output_file, index=False, encoding="utf-8-sig")
    
    print(f"Processed data saved to {output_file}")

# 実行例
if __name__ == "__main__":
    input_csv = "data/exported_results_1740113796.csv"
    output_file_name = get_file_name_keep(input_csv)                                 # 入力ファイル名を指定
    output_csv = f"output/{output_file_name}.csv"  # 出力ファイル名を指定
    process_quiz_data(input_csv, output_csv)