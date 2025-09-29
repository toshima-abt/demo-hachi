import streamlit as st
import duckdb
import pandas as pd
import google.generativeai as genai
import os

# --- 初期設定 ---
st.set_page_config(page_title="八王子市 事業者数分析", layout="wide")
st.title("自然言語で八王子市の事業者データを分析")

# Google Gemini APIキーの設定
# Streamlit Community Cloudにデプロイする際は、Secretsに設定します。
try:
    genai.configure(api_key=st.secrets["GOOGLE_API_KEY"])
except Exception:
    st.error("APIキーが設定されていません。StreamlitのSecretsに 'GOOGLE_API_KEY' を設定してください。")
    st.stop()


# --- プロンプトのテンプレート作成 ---
# データベースのスキーマとカラムの日本語説明をLLMに提供する
# この部分をファイルから読み込むようにすると、より管理しやすくなります。
TABLE_SCHEMA = """
CREATE TABLE business_stats("year" INTEGER, town_name VARCHAR, industry_name VARCHAR, num_offices INTEGER, num_employees INTEGER);
"""

COLUMN_DEFINITIONS = """
year:年度
town_name:町名
industry_name:事業種別
num_offices:事業所数
num_employees:事業者数
"""

INDUSTRY_NAMES= """
農林漁業
鉱業_採石業_砂利採取業
建設業
製造業
電気･ガス･熱供給･水道業
情報通信業
運輸業_郵便業
卸売業_小売業
金融業_保険業
不動産業_物品賃貸業
学術研究_専門･技術サービス業
宿泊業_飲食サービス業
生活関連サービス業_娯楽業
教育_学習支援業
医療_福祉
複合サービス事業
サービス業（他に分類されないもの）
公務（他に分類されるものを除く）
"""

YEARS = """
2015
2016
2017
2018
2019
2020
2021
2022
2023
2024
"""

PROMPT_TEMPLATE = f"""
あなたは優秀なデータアナリストです。以下のテーブル定義とカラム情報を参考に、ユーザーからの質問をDuckDBで実行可能なSQLクエリに変換してください。
SQLクエリのみを生成し、他の説明文は含めないでください。

### テーブル定義
{TABLE_SCHEMA}

### カラム情報
{COLUMN_DEFINITIONS}

### 事業種別
{INDUSTRY_NAMES}

### 年度
{YEARS}

### ユーザーの質問
{{user_question}}

### SQLクエリ
"""

# --- 関数定義 ---
def generate_sql(question):
    """ユーザーの質問からSQLを生成する"""
    model = genai.GenerativeModel('gemini-2.5-flash')
    prompt = PROMPT_TEMPLATE.format(user_question=question)
    try:
        response = model.generate_content(prompt)
        sql_query = response.text.strip().replace("```sql", "").replace("```", "")
        return sql_query
    except Exception as e:
        st.error(f"SQLの生成に失敗しました: {e}")
        return None

def execute_query(sql_query):
    """DuckDBでSQLを実行し、結果をDataFrameで返す"""
    try:
        with duckdb.connect('hachi_office.duckdb', read_only=True) as con:
            df = con.execute(sql_query).fetchdf()
        return df
    except Exception as e:
        st.error(f"SQLの実行に失敗しました: {e}")
        return None

# --- UI部分 ---
user_question = st.text_input("分析したい内容を質問してください:", "2021年の町名別で、建設業の事業所数が多いトップ5を教えて")

if st.button("分析を実行"):
    if user_question:
        with st.spinner("SQLを生成中..."):
            generated_sql = generate_sql(user_question)

        if generated_sql:
            st.info("生成されたSQLクエリ:")
            st.code(generated_sql, language="sql")

            with st.spinner("データベースでクエリを実行中..."):
                result_df = execute_query(generated_sql)

            if result_df is not None:
                st.success("クエリ結果")
                st.dataframe(result_df)

                # --- 簡単な可視化 ---
                if not result_df.empty and len(result_df.columns) >= 2:
                    try:
                        # 最初のカテゴリ列と最初の数値列でグラフを作成する試み
                        category_col = None
                        value_col = None
                        for col in result_df.columns:
                            if result_df[col].dtype == 'object' and category_col is None:
                                category_col = col
                            elif pd.api.types.is_numeric_dtype(result_df[col]) and value_col is None:
                                value_col = col
                        
                        if category_col and value_col:
                            st.bar_chart(result_df.set_index(category_col)[value_col])
                        else:
                            st.write("適切なグラフを作成できるデータ形式ではありません。")
                    except Exception as e:
                        st.warning(f"グラフの描画に失敗しました: {e}")

    else:
        st.warning("質問を入力してください。")
