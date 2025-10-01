import streamlit as st
import duckdb
import pandas as pd
import google.generativeai as genai
import os

# --- 初期設定 ---
st.set_page_config(page_title="八王子市 事業者数分析", layout="wide")
st.title("自然言語で八王子市の事業者データを分析")

with st.expander("使い方とデータについて"):
    st.markdown("""
    このアプリでは、八王子市の事業者に関する統計データについて、自然言語で質問することができます。
    AIがあなたの質問を解釈してSQLクエリを生成し、データベースから結果を取得・表示します。

    **利用可能なデータ**
    - **事業者統計データ (`business_stats`)**:
        - **対応年度**: 2015年～2024年
        - **事業種別**: 農林漁業, 鉱業_採石業_砂利採取業, 建設業, 製造業, 電気･ガス･熱供給･水道業, 情報通信業, 運輸業_郵便業, 卸売業_小売業, 金融業_保険業, 不動産業_物品賃貸業, 学術研究_専門･技術サービス業, 宿泊業_飲食サービス業, 生活関連サービス業_娯楽業, 教育_学習支援業, 医療_福祉, 複合サービス事業, サービス業（他に分類されないもの）, 公務（他に分類されるものを除く）
    - **人口統計データ (`population`)**:
        - 年度、町名、世帯数、人口数、男性数、女性数

    **質問の例**
    - `2021年の町名別で、建設業の事業所数が多いトップ5を教えて`
    - `情報通信業の事業所数が最も多い年度は？`
    - `八王子市全体の従業員数の推移を年度別に教えて`
    - `2022年の人口に対する事業者数の割合が高い町はどこ？`

    **ご注意**
    - AIが生成するSQLは必ずしも正確ではありません。意図した通りの結果が得られない場合は、質問の仕方を変えてみてください。
    - 複雑すぎる質問には答えられない場合があります。
    """)


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
CREATE TABLE population("year" BIGINT, town_name VARCHAR, num_households BIGINT, num_population BIGINT, num_male BIGINT, num_female BIGINT);
"""

COLUMN_DEFINITIONS = """
year:年度
town_name:町名
industry_name:事業種別
num_offices:事業所数
num_employees:事業者数
num_households:世帯数
num_population:人口数
num_male:男性数
num_female:女性数
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
あなたは優秀なデータアナリストです。八王子市に関する以下のテーブル定義とカラム情報を参考に、ユーザーからの質問をDuckDBで実行可能なSQLクエリに変換してください。
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

def calculate_derived_metrics(business_df, population_df, town_name=None, year=None):
    """世帯数と事業所数から派生した指標を計算する"""
    try:
        # データの結合
        merged_df = pd.merge(
            business_df,
            population_df[['year', 'town_name', 'num_households', 'num_population']],
            on=['year', 'town_name'],
            how='inner'
        )

        if merged_df.empty:
            return None

        # フィルタリング（町名・年度指定時）
        if town_name:
            merged_df = merged_df[merged_df['town_name'] == town_name]
        if year:
            merged_df = merged_df[merged_df['year'] == year]

        if merged_df.empty:
            return None

        # 派生指標の計算
        metrics_df = merged_df.copy()

        # 1. 事業所密度（事業所数 ÷ 世帯数）
        metrics_df['office_density_per_household'] = metrics_df['num_offices'] / metrics_df['num_households']

        # 2. 従業者比率（従業者数 ÷ 人口数）
        metrics_df['employee_ratio_per_population'] = metrics_df['num_employees'] / metrics_df['num_population']

        # 3. 事業所規模（従業者数 ÷ 事業所数）
        metrics_df['office_size'] = metrics_df['num_employees'] / metrics_df['num_offices'].replace(0, 1)  # ゼロ除算回避

        # 4. 世帯あたり事業所数（事業所数 ÷ 世帯数）
        metrics_df['offices_per_household'] = metrics_df['num_offices'] / metrics_df['num_households']

        # 5. 人口あたり事業所数（事業所数 ÷ 人口数）
        metrics_df['offices_per_population'] = metrics_df['num_offices'] / metrics_df['num_population']

        return metrics_df

    except Exception as e:
        st.error(f"指標の計算に失敗しました: {e}")
        return None

def generate_metric_interpretation(metrics_df, question_type="general"):
    """指標の解釈コメントを生成する"""
    try:
        if metrics_df is None or metrics_df.empty:
            return "解釈できるデータがありません。"

        # 基本統計の計算
        avg_density = metrics_df['office_density_per_household'].mean()
        avg_employee_ratio = metrics_df['employee_ratio_per_population'].mean()
        avg_office_size = metrics_df['office_size'].mean()

        # 解釈コメントの生成
        interpretations = []

        # 事業所密度の解釈
        if avg_density > 0.1:
            interpretations.append(f"事業所密度（{avg_density:.3f}）が高い水準を示しており、各世帯に対して多くの事業所が存在することを表しています。")
        elif avg_density > 0.05:
            interpretations.append(f"事業所密度（{avg_density:.3f}）は平均的な水準です。各世帯に対して適度な事業所数が配置されています。")
        else:
            interpretations.append(f"事業所密度（{avg_density:.3f}）は低い水準です。各世帯に対して事業所数が少ない状況です。")

        # 従業者比率の解釈
        if avg_employee_ratio > 0.3:
            interpretations.append(f"従業者比率（{avg_employee_ratio:.3f}）が高く、人口に対して多くの人が事業に従事している活発な経済活動を示しています。")
        elif avg_employee_ratio > 0.2:
            interpretations.append(f"従業者比率（{avg_employee_ratio:.3f}）は標準的な水準です。バランスの取れた雇用状況です。")
        else:
            interpretations.append(f"従業者比率（{avg_employee_ratio:.3f}）は低い水準です。経済活動が比較的少ない状況です。")

        # 事業所規模の解釈
        if avg_office_size > 10:
            interpretations.append(f"事業所規模（{avg_office_size:.1f}人/事業所）が大きく、中規模以上の事業所が多い傾向が見られます。")
        elif avg_office_size > 5:
            interpretations.append(f"事業所規模（{avg_office_size:.1f}人/事業所）は標準的で、小規模事業所を中心に構成されています。")
        else:
            interpretations.append(f"事業所規模（{avg_office_size:.1f}人/事業所）が小さく、零細事業所が多い状況です。")

        return " ".join(interpretations)

    except Exception as e:
        return f"解釈の生成に失敗しました: {e}"

def get_population_data():
    """人口データを取得する"""
    try:
        with duckdb.connect('hachi_office.duckdb', read_only=True) as con:
            df = con.execute("SELECT * FROM population").fetchdf()
        return df
    except Exception as e:
        st.error(f"人口データの取得に失敗しました: {e}")
        return None

def detect_metric_question(question):
    """質問が世帯数・事業所数関連の指標を求めているかを判定する"""
    metric_keywords = [
        '世帯数', '事業所数', '従業者数', '人口', '密度', '比率', '割合',
        '事業所密度', '従業者比率', '事業所規模', '世帯あたり', '人口あたり',
        'num_households', 'num_offices', 'num_employees', 'num_population'
    ]

    question_lower = question.lower()
    return any(keyword in question_lower for keyword in metric_keywords)

def get_business_data_for_metrics(year=None, town_name=None, industry_name=None):
    """指標計算用の事業者データを取得する"""
    try:
        with duckdb.connect('hachi_office.duckdb', read_only=True) as con:
            query = "SELECT * FROM business_stats"
            conditions = []

            if year:
                conditions.append(f"year = {year}")
            if town_name:
                conditions.append(f"town_name = '{town_name}'")
            if industry_name:
                conditions.append(f"industry_name = '{industry_name}'")

            if conditions:
                query += " WHERE " + " AND ".join(conditions)

            df = con.execute(query).fetchdf()
        return df
    except Exception as e:
        st.error(f"事業者データの取得に失敗しました: {e}")
        return None

# --- UI部分 ---
user_question = st.text_input("分析したい内容を質問してください:", "2021年の事業所密度（事業所数÷世帯数）と従業者比率（従業者数÷人口数）を町名別に比較して")

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

                # --- 派生指標の計算と解釈コメントの表示 ---
                if detect_metric_question(user_question):
                    with st.spinner("派生指標を計算中..."):
                        # 人口データを取得
                        population_df = get_population_data()

                        if population_df is not None:
                            # 事業者データを取得（元のクエリ結果に基づいてフィルタリング）
                            business_df = get_business_data_for_metrics()

                            if business_df is not None:
                                # 派生指標を計算
                                metrics_df = calculate_derived_metrics(business_df, population_df)

                                if metrics_df is not None and not metrics_df.empty:
                                    st.subheader("📊 派生指標分析")

                                    # 指標の概要を表示
                                    col1, col2, col3 = st.columns(3)

                                    with col1:
                                        avg_density = metrics_df['office_density_per_household'].mean()
                                        st.metric(
                                            label="平均事業所密度",
                                            value=f"{avg_density:.4f}",
                                            help="事業所数 ÷ 世帯数"
                                        )

                                    with col2:
                                        avg_employee_ratio = metrics_df['employee_ratio_per_population'].mean()
                                        st.metric(
                                            label="平均従業者比率",
                                            value=f"{avg_employee_ratio:.4f}",
                                            help="従業者数 ÷ 人口数"
                                        )

                                    with col3:
                                        avg_office_size = metrics_df['office_size'].mean()
                                        st.metric(
                                            label="平均事業所規模",
                                            value=f"{avg_office_size:.1f}人",
                                            help="従業者数 ÷ 事業所数"
                                        )

                                    # 解釈コメントを表示
                                    interpretation = generate_metric_interpretation(metrics_df)
                                    st.info(f"💡 解釈コメント: {interpretation}")

                                    # 詳細な指標テーブルを表示
                                    if st.checkbox("詳細な指標データを表示"):
                                        st.dataframe(metrics_df[[
                                            'year', 'town_name', 'num_offices', 'num_households',
                                            'office_density_per_household', 'employee_ratio_per_population',
                                            'office_size'
                                        ]].round(4))

                                        # 指標の相関関係を可視化
                                        if len(metrics_df) > 1:
                                            st.subheader("相関関係の可視化")
                                            correlation_cols = ['office_density_per_household', 'employee_ratio_per_population', 'office_size']
                                            corr_matrix = metrics_df[correlation_cols].corr()

                                            st.dataframe(corr_matrix.style.format("{:.3f}"))

                                else:
                                    st.warning("派生指標を計算できる十分なデータがありません。")
                            else:
                                st.warning("事業者データの取得に失敗しました。")
                        else:
                            st.warning("人口データの取得に失敗しました。")

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
