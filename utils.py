import streamlit as st
import duckdb
import pandas as pd
import google.generativeai as genai
import geopandas as gpd
from typing import Optional
import logging
import re
import json
import os

logger = logging.getLogger(__name__)

# --- 定数定義 ---
TABLE_SCHEMA = """
CREATE TABLE business_stats("year" INTEGER, town_name VARCHAR, industry_name VARCHAR, num_offices INTEGER, num_employees INTEGER);
CREATE TABLE population("year" BIGINT, town_name VARCHAR, num_households BIGINT, num_population BIGINT, num_male BIGINT, num_female BIGINT);
CREATE TABLE crimes("year" BIGINT, town_name VARCHAR, major_crime VARCHAR, minor_crime VARCHAR, crime_count BIGINT);
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
major_crime: 犯罪大分類
minor_crime: 犯罪小分類
crime_count: 犯罪件数
"""

INDUSTRY_NAMES = """
農林漁業, 鉱業_採石業_砂利採取業, 建設業, 製造業, 電気･ガス･熱供給･水道業, 
情報通信業, 運輸業_郵便業, 卸売業_小売業, 金融業_保険業, 不動産業_物品賃貸業, 
学術研究_専門･技術サービス業, 宿泊業_飲食サービス業, 生活関連サービス業_娯楽業, 
教育_学習支援業, 医療_福祉, 複合サービス事業, サービス業（他に分類されないもの）, 
公務（他に分類されるものを除く）
"""

CRIMES_TYPES = """
凶悪犯:強盗,
凶悪犯:その他,
粗暴犯:傷害,
粗暴犯:恐喝,
粗暴犯:暴行,
粗暴犯:脅迫,
侵入窃盗:事務所荒し,
侵入窃盗:出店荒し,
侵入窃盗:学校荒し,
侵入窃盗:居空き,
侵入窃盗:忍込み,
侵入窃盗:空き巣,
侵入窃盗:金庫破り,
侵入窃盗:その他,
非侵入窃盗:すり,
非侵入窃盗:ひったくり,
非侵入窃盗:オートバイ盗,
非侵入窃盗:万引き,
非侵入窃盗:工事場ねらい,
非侵入窃盗:置引き,
非侵入窃盗:自動車盗,
非侵入窃盗:自販機ねらい,
非侵入窃盗:自転車盗,
非侵入窃盗:車上ねらい,
非侵入窃盗:その他,
その他:占有離脱物横領,
その他:詐欺,
その他:賭博,
その他:その他刑法犯,
その他:その他知能犯,
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

### 利用可能な犯罪分類(大分類:小分類)
{CRIMES_TYPES}

### 対応年度
2015年～2024年

### 重要なルール
- SELECT文のみを生成してください（INSERT/UPDATE/DELETE禁止）
- LIMIT句はデフォルトで120を指定（明示的に指定がある場合はそちらを優先する）
- 日本語のカラム名は英語名に変換してください（例: 事業所数 → num_offices）

### ユーザーの質問
{{user_question}}

### SQLクエリ（SQLのみ出力、説明不要）
"""

# --- データ関連 ---

@st.cache_resource
def get_db_connection():
    """データベース接続をキャッシュして再利用"""
    try:
        return duckdb.connect('hachi_office.duckdb', read_only=True)
    except Exception as e:
        logger.error(f"データベース接続エラー: {e}")
        st.error(f"データベース接続エラー: {e}")
        return None

@st.cache_data
def load_geojson_data() -> Optional[gpd.GeoDataFrame]:
    """GeoJSONデータを読み込み、キャッシュする"""
    try:
        gdf = gpd.read_file('geojson/hachiouji_aza_simplified.geojson')
        gdf = gdf[['S_NAME', 'geometry']].rename(columns={'S_NAME': 'town_name'})
        if gdf.crs is None:
            gdf = gdf.set_crs(epsg=4326)
        elif gdf.crs.to_epsg() != 4326:
            gdf = gdf.to_crs(epsg=4326)
        logger.info(f"GeoJSON読み込み成功: {len(gdf)}件のデータ")
        return gdf
    except Exception as e:
        logger.error(f"GeoJSONの読み込みに失敗しました: {e}")
        st.error(f"GeoJSONの読み込みに失敗しました: {e}")
        return None

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

def execute_query(sql_query: str) -> Optional[pd.DataFrame]:
    """DuckDBでSQLを実行し、結果をDataFrameで返す"""
    try:
        con = get_db_connection()
        if con is None:
            st.error("データベース接続に失敗しました")
            return None
        
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

# --- AI関連 ---

def generate_sql(question: str) -> Optional[str]:
    """ユーザーの質問からSQLを生成する"""
    try:
        model = genai.GenerativeModel('gemini-flash-latest')
        prompt = PROMPT_TEMPLATE.format(user_question=question)
        response = model.generate_content(prompt)
        sql_query = response.text.strip().replace("```sql", "").replace("```", "").strip()
        logger.info(f"生成されたSQL: {sql_query}")
        return sql_query
    except Exception as e:
        st.error(f"❌ SQLの生成に失敗しました: {e}")
        logger.error(f"SQL生成エラー: {e}")
        return None

def extract_query_parameters(sql_query: str, user_question: str) -> dict:
    """SQLクエリとユーザー質問から年度・業種・町名を抽出"""
    params = {'year': None, 'industry': None, 'town': None}
    try:
        year_matches = re.findall(r'\b(20\d{2})\b', sql_query)
        if year_matches:
            params['year'] = int(year_matches[0])
        
        industry_keywords = INDUSTRY_NAMES.replace('\n', '').replace(' ', '').split(',')
        for industry in industry_keywords:
            if industry and (industry in sql_query or industry in user_question):
                params['industry'] = industry
                break
        
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

# --- 分析ロジック ---

def calculate_derived_metrics(business_df: pd.DataFrame, population_df: pd.DataFrame, 
                              year: int = None, industry: str = None, town: str = None) -> Optional[pd.DataFrame]:
    """世帯数と事業所数から派生した指標を計算する"""
    try:
        if business_df.empty or population_df.empty:
            return None
        
        filtered_business = business_df.copy()
        if year:
            filtered_business = filtered_business[filtered_business['year'] == year]
        if industry:
            filtered_business = filtered_business[filtered_business['industry_name'] == industry]
        if town:
            filtered_business = filtered_business[filtered_business['town_name'] == town]
        
        if filtered_business.empty:
            return None
        
        merged_df = pd.merge(
            filtered_business,
            population_df[['year', 'town_name', 'num_households', 'num_population']],
            on=['year', 'town_name'],
            how='inner'
        )

        if merged_df.empty:
            return None

        merged_df['office_density'] = merged_df['num_offices'] / merged_df['num_households'].replace(0, 1)
        merged_df['employee_ratio'] = merged_df['num_employees'] / merged_df['num_population'].replace(0, 1)
        merged_df['office_size'] = merged_df['num_employees'] / merged_df['num_offices'].replace(0, 1)
        merged_df['offices_per_1000_pop'] = (merged_df['num_offices'] / merged_df['num_population']) * 1000

        return merged_df
    except Exception as e:
        st.error(f"❌ 指標の計算に失敗しました: {e}")
        logger.error(f"指標計算エラー: {e}")
        return None

def generate_interpretation(metrics_df: pd.DataFrame) -> str:
    """指標の解釈コメントを生成する"""
    if metrics_df is None or metrics_df.empty:
        return "解釈できるデータがありません。"
    try:
        avg_density = metrics_df['office_density'].mean()
        avg_ratio = metrics_df['employee_ratio'].mean()
        avg_size = metrics_df['office_size'].mean()
        comments = []
        if avg_density > 0.1:
            comments.append(f"🏢 事業所密度が高水準（{avg_density:.3f}）で、商業活動が活発です。")
        elif avg_density > 0.05:
            comments.append(f"📊 事業所密度は標準的（{avg_density:.3f}）です。")
        else:
            comments.append(f"🏘️ 事業所密度が低め（{avg_density:.3f}）で、住宅地中心のエリアです。")
        if avg_ratio > 0.3:
            comments.append(f"💼 従業者比率が高く（{avg_ratio:.3f}）、雇用が活発です。")
        elif avg_ratio > 0.2:
            comments.append(f"👔 従業者比率は標準的（{avg_ratio:.3f}）です。")
        else:
            comments.append(f"🏠 従業者比率が低め（{avg_ratio:.3f}）です。")
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
        has_town = 'town_name' in metrics_df.columns and len(metrics_df['town_name'].unique()) > 1
        has_year = 'year' in metrics_df.columns and len(metrics_df['year'].unique()) > 1
        explanations = []
        if '密度' in question_lower or '世帯' in question_lower:
            explanations.append("ご質問の内容に関連して、**事業所密度**（世帯数に対する事業所数の比率）を分析しました。")
        if '比率' in question_lower or '割合' in question_lower or '人口' in question_lower:
            explanations.append("**従業者比率**（人口に対する従業者数の割合）も計算し、地域の経済活動の活発さを評価しました。")
        if '規模' in question_lower or '従業者' in question_lower:
            explanations.append("**事業所規模**（1事業所あたりの従業者数）から、事業者の規模感を把握できます。")
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


def generate_ai_summary(df: pd.DataFrame, user_question: str) -> str:
    """分析結果をもとにGeminiが自然言語で傾向を説明する"""
    if df is None or df.empty:
        return "データが見つかりませんでした。"

    try:
        genai.configure(api_key=os.getenv("GOOGLE_API_KEY"))
        model = genai.GenerativeModel('gemini-flash-latest')

        # DataFrameをコンパクトに変換（最大30行）
        sample_data = df.head(30).to_dict(orient="records")
        data_str = json.dumps(sample_data, ensure_ascii=False)

        prompt = f"""
次の質問とデータに基づいて、八王子市に関する分析結果を日本語で説明してください。
質問: {user_question}
データサンプル: {data_str}

出力条件:
- 一般利用者にも分かりやすく、2〜4文で要約。
- 主な傾向・特徴・注目すべき点を述べる。
- 数値や町名が明確な場合はそれを挙げる。
"""

        response = model.generate_content(prompt)
        return response.text.strip()
    except Exception as e:
        logger.error(f"AI要約生成エラー: {e}")
        return "AIによる分析コメントの生成に失敗しました。"
