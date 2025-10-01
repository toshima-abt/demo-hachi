# 八王子市 事業者データ分析アプリ

自然言語で質問すると、AIがSQLを生成して八王子市の事業者データを分析・可視化するStreamlitアプリケーションです。

GoogleのGemini APIを利用して、ユーザーの質問をDuckDBで実行可能なSQLクエリに変換し、データベースから取得した結果を表示します。

![アプリのスクリーンショットをここに挿入](https://via.placeholder.com/800x400.png?text=Application+Screenshot)

## 主な機能

- **自然言語によるデータ分析**: 「2021年の建設業の事業所数が多い町トップ5は？」のような日常的な言葉で質問できます。
- **AIによるSQLクエリの自動生成**: 入力された質問を基に、Gemini APIがSQLクエリを生成します。
- **生成されたSQLの表示**: どのようなクエリが実行されたかを確認できます。
- **分析結果の表示**: クエリ結果をテーブル形式で分かりやすく表示します。
- **簡単な自動可視化**: 結果データから棒グラフを自動で生成し、データの傾向を視覚的に捉えやすくします。
- **派生指標の自動計算と解釈**: 世帯数と事業所数から事業所密度、従業者比率などの指標を自動計算し、AIが解釈コメントを生成します。

## 使用データ

このアプリケーションは、以下の2つのテーブルを含むDuckDBデータベース (`hachi_office.duckdb`) を使用します。

1.  **事業者統計データ (`business_stats`)**
    - `year`: 年度 (2015-2024)
    - `town_name`: 町名
    - `industry_name`: 事業種別
    - `num_offices`: 事業所数
    - `num_employees`: 従業者数

2.  **人口統計データ (`population`)**
    - `year`: 年度
    - `town_name`: 町名
    - `num_households`: 世帯数
    - `num_population`: 人口数
    - `num_male`: 男性数
    - `num_female`: 女性数

## 技術スタック

- Python
- Streamlit
- DuckDB
- Google Gemini API
- Pandas

## セットアップと実行方法

### 1. 前提条件
- Python 3.8以上
- Google Gemini APIキー

### 2. リポジトリのクローン

```bash
git clone https://github.com/your-username/your-repository-name.git
cd your-repository-name
```

### 3. 必要なライブラリのインストール

`requirements.txt` を使って、必要なPythonライブラリをインストールします。

```bash
pip install -r requirements.txt
```

*(もし `requirements.txt` がなければ、以下のコマンドでインストールしてください)*
```bash
pip install streamlit duckdb pandas google-generativeai
```

### 4. APIキーの設定

プロジェクトのルートディレクトリに `.streamlit` フォルダを作成し、その中に `secrets.toml` ファイルを配置します。ファイルに以下の内容を記述してください。

```toml
# .streamlit/secrets.toml
GOOGLE_API_KEY = "YOUR_GOOGLE_API_KEY"
```
`YOUR_GOOGLE_API_KEY` はご自身のAPIキーに置き換えてください。

### 5. アプリケーションの実行

```bash
streamlit run app.py
```

## 免責事項

- このアプリケーションが生成するSQLクエリはAIによって作られており、必ずしも正確または最適であるとは限りません。
- 生成された情報は、正確性を保証するものではありません。参考情報としてご利用ください。
