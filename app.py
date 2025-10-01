import streamlit as st
import duckdb
import pandas as pd
import google.generativeai as genai
from typing import Optional, Tuple
import logging

# ロギング設定
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- 初期設定 ---
st.set_page_config(page_title="八王子市 事業者数分析", layout="wide")
st.title("🏢 自然言語で八王子市の事業者データを分析")

with st.expander("📘 使い方とデータについて"):
    st.markdown("""
    このアプリでは、八王子市の事業者に関する統計データについて、自然言語で質問することができます。
    AIがあなたの質問を解釈してSQLクエリを生成し、データベースから結果を取得・表示します。

    **利用可能なデータ**
    - **事業者統計データ (`business_stats`)**:
        - **対応年度**: 2015年～2024年
        - **事業種別**: 農林漁業, 建設業, 製造業, 情報通信業, 卸売業_小売業, 宿泊業_飲食サービス業, 医療_福祉など18業種
        - **カラム**: 年度、町名、事業種別、事業所数、従業者数
    - **人口統計データ (`population`)**:
        - **カラム**: 年度、町名、世帯数、人口数、男性数、女性数

    **質問の例**
    - `2021年の町名別で、建設業の事業所数が多いトップ5を教えて`
    - `情報通信業の事業所数が最も多い年度は？`
    - `八王子市全体の従業員数の推移を年度別に教えて`
    - `2022年の人口に対する事業者数の割合が高い町はどこ？`

    **ご注意**
    - AIが生成するSQLは必ずしも正確ではありません。意図した通りの結果が得られない場合は、質問の仕方を変えてみてください。
    """)

# --- 定数定義 ---
TABLE_SCHEMA = """
CREATE TABLE business_stats("year" INTEGER, town_name VARCHAR, industry_name VARCHAR, num_offices INTEGER, num_employees INTEGER);
CREATE TABLE population("year" BIGINT, town_name VARCHAR, num_households BIGINT, num_population BIGINT, num_male BIGINT, num_female BIGINT);
"""

COLUMN_DEFINITIONS = """
year: 年度
town_name: 町名
industry_name: 事業種別
num_offices: 事業所数
num_employees: 従業者数
num_households: 世帯数
num_population: 人口数
num_male: 男性数
num_female: 女性数
"""

INDUSTRY_NAMES = """
農林漁業, 鉱業_採石業_砂利採取業, 建設業, 製造業, 電気･ガス･熱供給･水道業, 
情報通信業, 運輸業_郵便業, 卸売業_小売業, 金融業_保険業, 不動産業_物品賃貸業, 
学術研究_専門･技術サービス業, 宿泊業_飲食サービス業, 生活関連サービス業_娯楽業, 
教育_学習支援業, 医療_福祉, 複合サービス事業, サービス業（他に分類されないもの）, 
公務（他に分類されるものを除く）
"""

PROMPT_TEMPLATE = f"""
あなたは優秀なデータアナリストです。八王子市に関する以下のテーブル定義とカラム情報を参考に、ユーザーからの質問をDuckDBで実行可能なSQLクエリに変換してください。
SQLクエリのみを生成し、他の説明文は含めないでください。

### テーブル定義
{TABLE_SCHEMA}

### カラム情報
{COLUMN_DEFINITIONS}

### 利用可能な事業種別
{INDUSTRY_NAMES}

### 対応年度
2015年～2024年

### 重要なルール
- SELECT文のみを生成してください（INSERT/UPDATE/DELETE禁止）
- LIMIT句を適切に使用してください（トップ5なら LIMIT 5）
- 日本語のカラム名は英語名に変換してください（例: 事業所数 → num_offices）

### ユーザーの質問
{{user_question}}

### SQLクエリ（SQLのみ出力、説明不要）
"""

# --- セッション状態での接続管理 ---
@st.cache_resource
def get_db_connection():
    """データベース接続をキャッシュして再利用"""
    try:
        return duckdb.connect('hachi_office.duckdb', read_only=True)
    except Exception as e:
        logger.error(f"データベース接続エラー: {e}")
        return None

# --- API設定 ---
try:
    genai.configure(api_key=st.secrets["GOOGLE_API_KEY"])
except Exception as e:
    st.error("⚠️ APIキーが設定されていません。StreamlitのSecretsに 'GOOGLE_API_KEY' を設定してください。")
    logger.error(f"API設定エラー: {e}")
    st.stop()

# --- 関数定義 ---
def generate_sql(question: str) -> Optional[str]:
    """ユーザーの質問からSQLを生成する"""
    try:
        model = genai.GenerativeModel('gemini-2.0-flash-exp')
        prompt = PROMPT_TEMPLATE.format(user_question=question)
        response = model.generate_content(prompt)
        sql_query = response.text.strip().replace("```sql", "").replace("```", "").strip()
        logger.info(f"生成されたSQL: {sql_query}")
        return sql_query
    except Exception as e:
        st.error(f"❌ SQLの生成に失敗しました: {e}")
        logger.error(f"SQL生成エラー: {e}")
        return None

def execute_query(sql_query: str) -> Optional[pd.DataFrame]:
    """DuckDBでSQLを実行し、結果をDataFrameで返す"""
    try:
        con = get_db_connection()
        if con is None:
            st.error("データベース接続に失敗しました")
            return None
        
        # 安全性チェック（基本的なSQLインジェクション対策）
        dangerous_keywords = ['DROP', 'DELETE', 'INSERT', 'UPDATE', 'ALTER', 'CREATE']
        if any(keyword in sql_query.upper() for keyword in dangerous_keywords):
            st.error("⚠️ 危険なSQL操作が検出されました")
            return None
        
        df = con.execute(sql_query).fetchdf()
        logger.info(f"クエリ実行成功: {len(df)}行取得")
        return df
    except Exception as e:
        st.error(f"❌ SQLの実行に失敗しました: {e}")
        logger.error(f"SQL実行エラー: {e}\nSQL: {sql_query}")
        return None

def calculate_derived_metrics(business_df: pd.DataFrame, population_df: pd.DataFrame) -> Optional[pd.DataFrame]:
    """世帯数と事業所数から派生した指標を計算する"""
    try:
        if business_df.empty or population_df.empty:
            return None
        
        # データの結合
        merged_df = pd.merge(
            business_df,
            population_df[['year', 'town_name', 'num_households', 'num_population']],
            on=['year', 'town_name'],
            how='inner'
        )

        if merged_df.empty:
            return None

        # 派生指標の計算（ゼロ除算対策）
        merged_df['office_density'] = merged_df['num_offices'] / merged_df['num_households'].replace(0, 1)
        merged_df['employee_ratio'] = merged_df['num_employees'] / merged_df['num_population'].replace(0, 1)
        merged_df['office_size'] = merged_df['num_employees'] / merged_df['num_offices'].replace(0, 1)
        merged_df['offices_per_1000_pop'] = (merged_df['num_offices'] / merged_df['num_population']) * 1000

        logger.info(f"派生指標計算完了: {len(merged_df)}行")
        return merged_df

    except Exception as e:
        st.error(f"❌ 指標の計算に失敗しました: {e}")
        logger.error(f"指標計算エラー: {e}")
        return None

def generate_interpretation(metrics_df: pd.DataFrame) -> str:
    """指標の解釈コメントを生成する"""
    try:
        if metrics_df is None or metrics_df.empty:
            return "解釈できるデータがありません。"

        avg_density = metrics_df['office_density'].mean()
        avg_ratio = metrics_df['employee_ratio'].mean()
        avg_size = metrics_df['office_size'].mean()

        comments = []

        # 事業所密度の評価
        if avg_density > 0.1:
            comments.append(f"🏢 事業所密度が高水準（{avg_density:.3f}）で、商業活動が活発です。")
        elif avg_density > 0.05:
            comments.append(f"📊 事業所密度は標準的（{avg_density:.3f}）です。")
        else:
            comments.append(f"🏘️ 事業所密度が低め（{avg_density:.3f}）で、住宅地中心のエリアです。")

        # 従業者比率の評価
        if avg_ratio > 0.3:
            comments.append(f"💼 従業者比率が高く（{avg_ratio:.3f}）、雇用が活発です。")
        elif avg_ratio > 0.2:
            comments.append(f"👔 従業者比率は標準的（{avg_ratio:.3f}）です。")
        else:
            comments.append(f"🏠 従業者比率が低め（{avg_ratio:.3f}）です。")

        # 事業所規模の評価
        if avg_size > 10:
            comments.append(f"🏭 平均事業所規模が大きく（{avg_size:.1f}人/所）、中規模以上の企業が多いです。")
        else:
            comments.append(f"🏪 平均事業所規模は小さめ（{avg_size:.1f}人/所）で、小規模事業所中心です。")

        return " ".join(comments)

    except Exception as e:
        logger.error(f"解釈生成エラー: {e}")
        return "解釈の生成に失敗しました。"

def detect_metric_question(question: str) -> bool:
    """指標計算が必要な質問かを判定"""
    keywords = ['密度', '比率', '割合', '世帯', '人口', '従業者', 'あたり', '指標']
    return any(kw in question for kw in keywords)

def get_all_data(table_name: str) -> Optional[pd.DataFrame]:
    """指定テーブルの全データを取得"""
    try:
        con = get_db_connection()
        if con is None:
            return None
        return con.execute(f"SELECT * FROM {table_name}").fetchdf()
    except Exception as e:
        logger.error(f"データ取得エラー ({table_name}): {e}")
        return None

# --- UI部分 ---
st.markdown("---")

# セッション状態の初期化
if 'user_question' not in st.session_state:
    st.session_state.user_question = "2021年の事業所密度（事業所数÷世帯数）と従業者比率（従業者数÷人口数）を町名別に比較して"

# サンプル質問ボタン
st.subheader("💡 質問例")
col1, col2, col3 = st.columns(3)
with col1:
    if st.button("🏗️ 建設業トップ5"):
        st.session_state.user_question = "2021年の建設業の事業所数が多い町名トップ5"
with col2:
    if st.button("📈 従業員数推移"):
        st.session_state.user_question = "年度別の従業員数の推移"
with col3:
    if st.button("🏘️ 事業所密度分析"):
        st.session_state.user_question = "2022年の町名別の事業所密度を教えて"

# 質問入力（セッション状態と直接バインド）
user_question = st.text_input("🔍 分析したい内容を質問してください:", key="user_question")

if st.button("🚀 分析を実行", type="primary"):
    if user_question:
        with st.spinner("🤖 AIがSQLを生成中..."):
            generated_sql = generate_sql(user_question)

        if generated_sql:
            with st.expander("📝 生成されたSQLクエリ", expanded=False):
                st.code(generated_sql, language="sql")

            with st.spinner("💾 データベースでクエリを実行中..."):
                result_df = execute_query(generated_sql)

            if result_df is not None and not result_df.empty:
                st.success(f"✅ クエリ結果 ({len(result_df)}行)")
                st.dataframe(result_df, use_container_width=True)

                # --- 派生指標の計算 ---
                if detect_metric_question(user_question):
                    with st.spinner("📊 派生指標を計算中..."):
                        population_df = get_all_data('population')
                        business_df = get_all_data('business_stats')

                        if population_df is not None and business_df is not None:
                            metrics_df = calculate_derived_metrics(business_df, population_df)

                            if metrics_df is not None and not metrics_df.empty:
                                st.markdown("---")
                                st.subheader("📊 派生指標分析")

                                col1, col2, col3, col4 = st.columns(4)
                                with col1:
                                    st.metric("平均事業所密度", f"{metrics_df['office_density'].mean():.4f}", 
                                             help="事業所数 ÷ 世帯数")
                                with col2:
                                    st.metric("平均従業者比率", f"{metrics_df['employee_ratio'].mean():.4f}",
                                             help="従業者数 ÷ 人口数")
                                with col3:
                                    st.metric("平均事業所規模", f"{metrics_df['office_size'].mean():.1f}人",
                                             help="従業者数 ÷ 事業所数")
                                with col4:
                                    st.metric("人口1000人あたり事業所数", f"{metrics_df['offices_per_1000_pop'].mean():.1f}",
                                             help="(事業所数 ÷ 人口) × 1000")

                                interpretation = generate_interpretation(metrics_df)
                                st.info(f"💡 **解釈:** {interpretation}")

                                if st.checkbox("📋 詳細な指標データを表示"):
                                    display_cols = ['year', 'town_name', 'num_offices', 'num_employees', 
                                                  'office_density', 'employee_ratio', 'office_size']
                                    available_cols = [col for col in display_cols if col in metrics_df.columns]
                                    st.dataframe(metrics_df[available_cols].round(4), use_container_width=True)

                # --- グラフ表示 ---
                if len(result_df.columns) >= 2:
                    try:
                        numeric_cols = result_df.select_dtypes(include=['number']).columns.tolist()
                        category_cols = result_df.select_dtypes(include=['object']).columns.tolist()

                        if category_cols and numeric_cols:
                            st.subheader("📈 データ可視化")
                            chart_df = result_df.set_index(category_cols[0])[numeric_cols[0]]
                            st.bar_chart(chart_df)
                    except Exception as e:
                        logger.warning(f"グラフ描画スキップ: {e}")
            elif result_df is not None:
                st.warning("⚠️ 結果が0件でした。質問を変えてみてください。")
    else:
        st.warning("⚠️ 質問を入力してください。")

# フッター
st.markdown("---")
st.caption("💡 Powered by Google Gemini & DuckDB | 八王子市オープンデータを活用")