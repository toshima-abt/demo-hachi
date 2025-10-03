import streamlit as st
import duckdb
import pandas as pd
import google.generativeai as genai
import geopandas as gpd
import pydeck as pdk
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
        - **事業種別**: 農林漁業, 建設業, 製造業, 情報通信業, 卸売業_小売業, 宿泊業_飲食サービス業, 医療_福祉, 金融業_保険業, 不動産業_物品賃貸業, 電気･ガス･熱供給･水道業, 運輸業_郵便業, 学術研究_専門･技術サービス業, 鉱業_採石業_砂利採取業, 生活関連サービス業_娯楽業, 教育_学習支援業, 複合サービス事業, サービス業（他に分類されないもの）, 公務（他に分類されるものを除く）
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

@st.cache_data
def load_geojson_data() -> Optional[gpd.GeoDataFrame]:
    """GeoJSONデータを読み込み、キャッシュする"""
    try:
        gdf = gpd.read_file('geojson/hachiouji_aza.geojson')
        # 必要なカラムに絞り、町名カラムの名前を統一
        gdf = gdf[['S_NAME', 'geometry']].rename(columns={'S_NAME': 'town_name'})
        
        # CRSを確認し、WGS84でない場合は変換
        if gdf.crs is None:
            gdf = gdf.set_crs(epsg=4326)
        elif gdf.crs.to_epsg() != 4326:
            gdf = gdf.to_crs(epsg=4326)
        
        # PyDeck用にgeometry列を座標配列に変換
        def geometry_to_coordinates(geom):
            """GeometryオブジェクトをPyDeck用の座標配列に変換"""
            if geom.geom_type == 'Polygon':
                return [list(geom.exterior.coords)]
            elif geom.geom_type == 'MultiPolygon':
                return [list(poly.exterior.coords) for poly in geom.geoms]
            return []
        
        gdf['coordinates'] = gdf['geometry'].apply(geometry_to_coordinates)
        
        logger.info(f"GeoJSON読み込み成功: {len(gdf)}件のデータ")
        return gdf
        
    except Exception as e:
        logger.error(f"GeoJSONの読み込みに失敗しました: {e}")
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

def calculate_derived_metrics(business_df: pd.DataFrame, population_df: pd.DataFrame, 
                              year: int = None, industry: str = None, town: str = None) -> Optional[pd.DataFrame]:
    """世帯数と事業所数から派生した指標を計算する"""
    try:
        if business_df.empty or population_df.empty:
            return None
        
        # フィルタリング（年度・業種・町名）
        filtered_business = business_df.copy()
        
        if year:
            filtered_business = filtered_business[filtered_business['year'] == year]
            logger.info(f"年度フィルタ適用: {year}")
        
        if industry:
            filtered_business = filtered_business[filtered_business['industry_name'] == industry]
            logger.info(f"業種フィルタ適用: {industry}")
        
        if town:
            filtered_business = filtered_business[filtered_business['town_name'] == town]
            logger.info(f"町名フィルタ適用: {town}")
        
        if filtered_business.empty:
            logger.warning("フィルタ後のデータが空です")
            return None
        
        logger.info(f"フィルタ後のデータ件数: {len(filtered_business)}行")
        
        # データの結合
        merged_df = pd.merge(
            filtered_business,
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

def generate_contextual_explanation(user_question: str, metrics_df: pd.DataFrame) -> str:
    """ユーザーの質問に応じた文脈説明を生成"""
    try:
        question_lower = user_question.lower()
        
        # 町名が含まれているか確認
        has_town = 'town_name' in metrics_df.columns and len(metrics_df['town_name'].unique()) > 1
        
        # 年度が含まれているか確認
        has_year = 'year' in metrics_df.columns and len(metrics_df['year'].unique()) > 1
        
        explanations = []
        
        # 質問内容に応じた説明
        if '密度' in question_lower or '世帯' in question_lower:
            explanations.append("ご質問の内容に関連して、**事業所密度**（世帯数に対する事業所数の比率）を分析しました。")
        
        if '比率' in question_lower or '割合' in question_lower or '人口' in question_lower:
            explanations.append("**従業者比率**（人口に対する従業者数の割合）も計算し、地域の経済活動の活発さを評価しました。")
        
        if '規模' in question_lower or '従業者' in question_lower:
            explanations.append("**事業所規模**（1事業所あたりの従業者数）から、事業者の規模感を把握できます。")
        
        # 比較対象の説明
        if has_town and has_year:
            explanations.append(f"\n📍 {len(metrics_df['town_name'].unique())}の町名、{len(metrics_df['year'].unique())}年度のデータを比較しています。")
        elif has_town:
            explanations.append(f"\n📍 {len(metrics_df['town_name'].unique())}の町名を比較しています。")
        elif has_year:
            explanations.append(f"\n📅 {len(metrics_df['year'].unique())}年度の推移を分析しています。")
        
        if not explanations:
            return "ご質問に関連する経済指標を自動的に計算しました。以下の指標で地域の特徴を把握できます。"
        
        return " ".join(explanations)
        
    except Exception as e:
        logger.error(f"文脈説明生成エラー: {e}")
        return "質問内容に関連する追加指標を計算しました。"

def get_top_bottom_insights(metrics_df: pd.DataFrame, metric_name: str, display_name: str, n: int = 3) -> str:
    """指標のトップ・ボトムを抽出して洞察を提供"""
    try:
        if metric_name not in metrics_df.columns or 'town_name' not in metrics_df.columns:
            return ""
        
        df_sorted = metrics_df.sort_values(metric_name, ascending=False)
        
        # 最新年度のデータを優先
        if 'year' in df_sorted.columns:
            latest_year = df_sorted['year'].max()
            df_sorted = df_sorted[df_sorted['year'] == latest_year]
        
        top_towns = df_sorted.head(n)
        bottom_towns = df_sorted.tail(n)
        
        insights = f"\n\n**{display_name}の地域差:**\n"
        insights += f"- 📈 **上位**: {', '.join([f'{row.town_name}（{row[metric_name]:.3f}）' for _, row in top_towns.iterrows()])}\n"
        insights += f"- 📉 **下位**: {', '.join([f'{row.town_name}（{row[metric_name]:.3f}）' for _, row in bottom_towns.iterrows()])}"
        
        return insights
        
    except Exception as e:
        logger.error(f"洞察生成エラー: {e}")
        return ""

def extract_query_parameters(sql_query: str, user_question: str) -> dict:
    """SQLクエリとユーザー質問から年度・業種・町名を抽出"""
    params = {'year': None, 'industry': None, 'town': None}
    
    try:
        sql_upper = sql_query.upper()
        
        # 年度の抽出
        import re
        year_matches = re.findall(r'\b(20\d{2})\b', sql_query)
        if year_matches:
            params['year'] = int(year_matches[0])
        
        # 業種の抽出（WHERE句から）
        industry_keywords = [
            '農林漁業', '鉱業_採石業_砂利採取業', '建設業', '製造業', 
            '電気･ガス･熱供給･水道業', '情報通信業', '運輸業_郵便業', 
            '卸売業_小売業', '金融業_保険業', '不動産業_物品賃貸業',
            '学術研究_専門･技術サービス業', '宿泊業_飲食サービス業',
            '生活関連サービス業_娯楽業', '教育_学習支援業', '医療_福祉',
            '複合サービス事業', 'サービス業（他に分類されないもの）',
            '公務（他に分類されるものを除く）'
        ]
        
        for industry in industry_keywords:
            if industry in sql_query or industry in user_question:
                params['industry'] = industry
                break
        
        # 町名の抽出（もしWHERE句にあれば）
        town_match = re.search(r"town_name\s*=\s*'([^']+)'", sql_query)
        if town_match:
            params['town'] = town_match.group(1)
        
        logger.info(f"抽出されたパラメータ: {params}")
        return params
        
    except Exception as e:
        logger.error(f"パラメータ抽出エラー: {e}")
        return params

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
if 'result_df' not in st.session_state:
    st.session_state.result_df = None
if 'generated_sql' not in st.session_state:
    st.session_state.generated_sql = None
if 'metrics_df' not in st.session_state:
    st.session_state.metrics_df = None
if 'is_metric_question' not in st.session_state:
    st.session_state.is_metric_question = False
if 'query_params' not in st.session_state:
    st.session_state.query_params = {}

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
            # セッション状態に保存
            st.session_state.generated_sql = generated_sql
            
            with st.spinner("💾 データベースでクエリを実行中..."):
                result_df = execute_query(generated_sql)
                st.session_state.result_df = result_df
                st.session_state.is_metric_question = detect_metric_question(user_question)

            # 派生指標の計算（指標関連の質問の場合）
            if st.session_state.is_metric_question and result_df is not None and not result_df.empty:
                with st.spinner("📊 派生指標を計算中..."):
                    # クエリからパラメータを抽出
                    query_params = extract_query_parameters(generated_sql, user_question)
                    
                    population_df = get_all_data('population')
                    business_df = get_all_data('business_stats')

                    if population_df is not None and business_df is not None:
                        # 抽出したパラメータでフィルタリング
                        metrics_df = calculate_derived_metrics(
                            business_df, 
                            population_df,
                            year=query_params['year'],
                            industry=query_params['industry'],
                            town=query_params['town']
                        )
                        st.session_state.metrics_df = metrics_df
                        st.session_state.query_params = query_params  # パラメータも保存
                    else:
                        st.session_state.metrics_df = None
                        st.session_state.query_params = {}
            else:
                st.session_state.metrics_df = None
                st.session_state.query_params = {}
    else:
        st.warning("⚠️ 質問を入力してください。")

# --- 結果表示（セッション状態から復元） ---
if st.session_state.generated_sql:
    with st.expander("📝 生成されたSQLクエリ", expanded=False):
        st.code(st.session_state.generated_sql, language="sql")

if st.session_state.result_df is not None and not st.session_state.result_df.empty:
    result_df = st.session_state.result_df
    
    st.success(f"✅ クエリ結果 ({len(result_df)}行)")
    st.dataframe(result_df, use_container_width=True)

    # --- 派生指標の表示 ---
    if st.session_state.is_metric_question and st.session_state.metrics_df is not None:
        metrics_df = st.session_state.metrics_df
        query_params = st.session_state.get('query_params', {})
        
        if not metrics_df.empty:
            st.markdown("---")
            
            # フィルタ情報を表示
            filter_info = []
            if query_params.get('year'):
                filter_info.append(f"📅 **{query_params['year']}年度**")
            if query_params.get('industry'):
                filter_info.append(f"🏢 **{query_params['industry']}**")
            if query_params.get('town'):
                filter_info.append(f"📍 **{query_params['town']}**")
            
            if filter_info:
                st.info(f"🔍 **分析対象**: {' / '.join(filter_info)} のデータに基づいて指標を計算しました")
            
            # 文脈説明を表示
            context_explanation = generate_contextual_explanation(user_question, metrics_df)
            st.info(f"📊 **分析の背景**\n\n{context_explanation}")
            
            st.subheader("📊 経済指標の詳細分析")

            # メトリクス表示
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                avg_density = metrics_df['office_density'].mean()
                st.metric(
                    "平均事業所密度", 
                    f"{avg_density:.4f}", 
                    help="事業所数 ÷ 世帯数\n\n値が高いほど、世帯数に対して事業所が多い（商業地域的）"
                )
            with col2:
                avg_ratio = metrics_df['employee_ratio'].mean()
                st.metric(
                    "平均従業者比率", 
                    f"{avg_ratio:.4f}",
                    help="従業者数 ÷ 人口数\n\n値が高いほど、人口に対して働く人が多い（雇用が活発）"
                )
            with col3:
                avg_size = metrics_df['office_size'].mean()
                st.metric(
                    "平均事業所規模", 
                    f"{avg_size:.1f}人",
                    help="従業者数 ÷ 事業所数\n\n値が大きいほど、1事業所あたりの従業員が多い（大規模事業所）"
                )
            with col4:
                avg_per_1000 = metrics_df['offices_per_1000_pop'].mean()
                st.metric(
                    "人口1000人あたり事業所数", 
                    f"{avg_per_1000:.1f}",
                    help="(事業所数 ÷ 人口) × 1000\n\n国際比較などで使われる標準指標"
                )

            # 解釈コメント
            interpretation = generate_interpretation(metrics_df)
            
            # 地域差の洞察（町名別データがある場合）
            insights = ""
            if 'town_name' in metrics_df.columns and len(metrics_df['town_name'].unique()) > 1:
                insights += get_top_bottom_insights(metrics_df, 'office_density', '事業所密度', n=3)
            
            full_interpretation = f"💡 **データから読み取れること**\n\n{interpretation}{insights}"
            st.success(full_interpretation)

            # 詳細データ表示
            with st.expander("📋 詳細な指標データを表示", expanded=False):
                display_cols = ['year', 'town_name', 'num_offices', 'num_employees', 'num_households', 'num_population',
                              'office_density', 'employee_ratio', 'office_size', 'offices_per_1000_pop']
                available_cols = [col for col in display_cols if col in metrics_df.columns]
                
                # カラム名を日本語化
                col_rename = {
                    'year': '年度',
                    'town_name': '町名',
                    'num_offices': '事業所数',
                    'num_employees': '従業者数',
                    'num_households': '世帯数',
                    'num_population': '人口数',
                    'office_density': '事業所密度',
                    'employee_ratio': '従業者比率',
                    'office_size': '事業所規模',
                    'offices_per_1000_pop': '人口千人あたり事業所数'
                }
                
                display_df = metrics_df[available_cols].copy()
                display_df = display_df.rename(columns=col_rename)
                
                st.dataframe(
                    display_df.round(4), 
                    use_container_width=True,
                    hide_index=True
                )
                
                # CSVダウンロード
                csv = display_df.to_csv(index=False).encode('utf-8-sig')
                st.download_button(
                    "📥 CSVでダウンロード",
                    csv,
                    "hachioji_metrics.csv",
                    "text/csv",
                    key='download-csv'
                )

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

    # --- 地図表示 ---
    if result_df is not None and not result_df.empty:

        # 町名と数値データがある場合のみ地図を表示
        numeric_cols = result_df.select_dtypes(include=['number']).columns.tolist()
        if 'town_name' in result_df.columns and len(numeric_cols) > 0:
            st.subheader("🗺️ 地図で結果を確認")
            
            # 使用する数値カラムを選択
            metric_to_map = st.selectbox(
                "地図に表示する指標を選択してください:",
                options=numeric_cols,
                index=0
            )

            with st.spinner("🗺️ 地図データを生成中..."):
                gdf = load_geojson_data()
                if gdf is not None:
                    # GeoDataFrameと結果をマージ
                    map_df = gdf.merge(result_df, on='town_name', how='inner')

                    if not map_df.empty:
                        # 値の正規化（0-1の範囲に）
                        max_val = map_df[metric_to_map].max()
                        min_val = map_df[metric_to_map].min()
                        
                        if max_val > min_val:
                            map_df['normalized'] = (map_df[metric_to_map] - min_val) / (max_val - min_val)
                        else:
                            map_df['normalized'] = 0.5
                        
                        # 色を計算（赤→黄色→緑のグラデーション）
                        def get_color(normalized_value):
                            """正規化された値(0-1)からRGBA色を生成"""
                            r = int(255 * (1 - normalized_value))
                            g = int(255 * normalized_value)
                            b = 0
                            return [r, g, b, 180]
                        
                        map_df['fill_color'] = map_df['normalized'].apply(get_color)
                        
                       # Pydeckで地図を描画
                        st.pydeck_chart(pdk.Deck(
                            map_style=None,
                            initial_view_state=pdk.ViewState(
                                latitude=35.655,
                                longitude=139.33,
                                zoom=11,
                                pitch=0,
                            ),
                            layers=[
                                pdk.Layer(
                                    'PolygonLayer',
                                    data=map_df,
                                    get_polygon='coordinates',
                                    filled=True,
                                    stroked=True,
                                    get_fill_color='fill_color',
                                    get_line_color=[80, 80, 80],
                                    line_width_min_pixels=1,
                                    pickable=True,
                                    auto_highlight=True,
                                )
                            ],
                            tooltip={
                                "html": f"<b>町名:</b> {{town_name}}<br/><b>{metric_to_map}:</b> {{{metric_to_map}}}",
                                "style": {
                                    "backgroundColor": "steelblue",
                                    "color": "white"
                                }
                            }
                        ))
                        
                        # 凡例を表示
                        st.caption(f"🎨 色の凡例: 赤（低い値: {min_val:.2f}）→ 黄色（中間）→ 緑（高い値: {max_val:.2f}）")
                    else:
                        st.warning("⚠️ 地図データと結合できる町名が見つかりませんでした。")
                else:
                    st.error("❌ 地図データの読み込みに失敗しました。")

elif st.session_state.result_df is not None:
    st.warning("⚠️ 結果が0件でした。質問を変えてみてください。")

# フッター
st.markdown("---")
st.caption("💡 Powered by Google Gemini & DuckDB | 八王子市オープンデータを活用")