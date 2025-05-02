"""
Visualización: Dashboard interactivo para los datos de criptomonedas
"""
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash import Dash, html, dcc, Input, Output, dash_table
from pymongo import MongoClient
from sqlalchemy import create_engine
import logging
from datetime import datetime, timedelta

# Configuración del logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Importar configuraciones desde config.py
from config import (
    MONGO_URI,
    MONGO_DB,
    MYSQL_HOST,
    MYSQL_USER,
    MYSQL_PASSWORD,
    MYSQL_DATABASE
)

# Función para obtener datos desde MongoDB
def get_data_from_mongodb():
    try:
        client = MongoClient(MONGO_URI)
        db = client[MONGO_DB]
        collection = db['crypto_prices']
        # Obtener últimos 100 registros
        pipeline = [
            {"$sort": {"timestamp": -1}},
            {"$limit": 100},
            {"$project": {
                "_id": 0,
                "name": 1,
                "symbol": 1,
                "price_usd": 1,
                "market_cap_usd": 1,
                "timestamp": 1
            }}
        ]
        data = list(collection.aggregate(pipeline))
        df = pd.DataFrame(data) if data else pd.DataFrame()
        if not df.empty and 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
        return df
    except Exception as e:
        logger.error(f"Error al obtener datos de MongoDB: {e}")
        return pd.DataFrame()

# Función para obtener datos desde MySQL usando SQLAlchemy
def get_data_from_mysql():
    try:
        engine = create_engine(
            f"mysql+mysqlconnector://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/{MYSQL_DATABASE}"
        )
        query = """
        SELECT name, symbol, price_usd, market_cap_usd, timestamp
        FROM crypto_prices
        ORDER BY timestamp DESC
        LIMIT 100
        """
        df = pd.read_sql(query, engine)
        if not df.empty and 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
        return df
    except Exception as e:
        logger.error(f"Error al obtener datos de MySQL: {e}")
        return pd.DataFrame()

# Función para obtener noticias desde MySQL
def get_news_from_mysql():
    try:
        engine = create_engine(
            f"mysql+mysqlconnector://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/{MYSQL_DATABASE}"
        )
        query = """
        SELECT title, link AS url, timestamp
        FROM crypto_news
        ORDER BY timestamp DESC
        LIMIT 10
        """
        df = pd.read_sql(query, engine)
        if not df.empty:
            if 'timestamp' in df.columns:
                df['timestamp'] = pd.to_datetime(df['timestamp'])
            if 'url' not in df.columns:
                df['url'] = '#'
        return df
    except Exception as e:
        logger.error(f"Error al obtener noticias de MySQL: {e}")
        return pd.DataFrame()

# Inicializar aplicación Dash
app = Dash(__name__, title="Crypto Dashboard")

# Layout principal
app.layout = html.Div([
    # Encabezado
    html.H1("Dashboard de Criptomonedas", style={'textAlign': 'center', 'marginBottom': '20px'}),
    # Selector de fuente de datos y filtro de tiempo
    html.Div([
        html.Div([
            html.Label("Fuente de datos:"),
            dcc.RadioItems(
                id='database-selector',
                options=[
                    {'label': 'MongoDB', 'value': 'mongodb'},
                    {'label': 'MySQL', 'value': 'mysql'}
                ],
                value='mongodb',
                inline=True
            )
        ], style={'width': '45%', 'display': 'inline-block', 'verticalAlign': 'top'}),
        html.Div([
            html.Label("Rango de tiempo:"),
            dcc.Dropdown(
                id='time-range',
                options=[
                    {'label': 'Últimos 10 minutos', 'value': '10min'},
                    {'label': 'Últimas 24 horas', 'value': '24h'},
                    {'label': 'Todo el historial', 'value': 'all'}
                ],
                value='24h'
            )
        ], style={'width': '45%', 'display': 'inline-block', 'marginLeft': '10%'})
    ], style={'margin': '20px'}),
    # Selector de criptomonedas
    html.Div([
        html.Label("Seleccionar criptomonedas:"),
        dcc.Dropdown(
            id='crypto-selector',
            multi=True,
            value=[]
        )
    ], style={'margin': '20px'}),
    # Gráfico de precios
    html.Div([
        html.H2("Precios de Criptomonedas"),
        dcc.Graph(id='price-chart')
    ]),
    # Gráfico de capitalización de mercado
    html.Div([
        html.H2("Capitalización de Mercado"),
        dcc.Graph(id='market-cap-chart')
    ]),
    # Tabla de datos
    html.Div([
        html.H2("Datos de Criptomonedas"),
        html.Div(id='data-table-container')
    ], style={'margin': '20px'}),
    # Noticias recientes
    html.Div([
        html.H2("Noticias Recientes de Criptomonedas"),
        html.Ul(id='news-list')
    ], style={'margin': '20px'}),
    # Componente de intervalo para actualización automática
    dcc.Interval(
        id='interval-component',
        interval=60 * 1000,  # Actualiza cada minuto
        n_intervals=0
    ),
    # Contenedor para mensajes de error
    html.Div(id='error-message', style={'color': 'red', 'margin': '20px'})
])

# Callback para actualizar opciones del dropdown de criptomonedas
@app.callback(
    Output('crypto-selector', 'options'),
    [Input('database-selector', 'value'),
     Input('interval-component', 'n_intervals')]
)
def update_crypto_options(db_source, _):
    df = get_data_from_mongodb() if db_source == 'mongodb' else get_data_from_mysql()
    if df.empty:
        return []
    options = [{'label': name, 'value': name} for name in sorted(df['name'].unique())]
    return options

# Callback para filtrar y actualizar gráficos y tabla
@app.callback(
    [Output('price-chart', 'figure'),
     Output('market-cap-chart', 'figure'),
     Output('data-table-container', 'children'),
     Output('error-message', 'children')],
    [Input('database-selector', 'value'),
     Input('crypto-selector', 'value'),
     Input('time-range', 'value'),
     Input('interval-component', 'n_intervals')]
)
def update_dashboard(db_source, selected_cryptos, time_range, _):
    df = get_data_from_mongodb() if db_source == 'mongodb' else get_data_from_mysql()
    if df.empty:
        return {}, {}, None, "⚠️ No hay datos disponibles en la base seleccionada"
    
    # Validar columnas requeridas (sin price_change_24h)
    required_cols = ['name', 'symbol', 'price_usd', 'market_cap_usd', 'timestamp']
    if not all(col in df.columns for col in required_cols):
        error_msg = "Datos incompletos: faltan columnas esenciales"
        logger.error(error_msg)
        return {}, {}, None, error_msg
    
    now = datetime.now()
    if time_range == '10min':
        df = df[df['timestamp'] > now - timedelta(minutes=10)]
    elif time_range == '24h':
        df = df[df['timestamp'] > now - timedelta(hours=24)]
    
    if df.empty:
        return {}, {}, None, "No hay datos para el rango de tiempo seleccionado"
    
    if selected_cryptos:
        filtered_df = df[df['name'].isin(selected_cryptos)]
    else:
        latest_per_coin = df.sort_values('timestamp').drop_duplicates('name', keep='last')
        top_cryptos = latest_per_coin.sort_values('market_cap_usd', ascending=False)['name'][:5]
        filtered_df = df[df['name'].isin(top_cryptos)]
    
    if filtered_df.empty:
        return {}, {}, None, "No hay datos para las criptomonedas seleccionadas"
    
    # Gráfico de precios
    price_fig = px.line(
        filtered_df,
        x='timestamp',
        y='price_usd',
        color='name',
        title='Evolución de Precios (USD)',
        labels={'price_usd': 'Precio (USD)', 'timestamp': 'Fecha', 'name': 'Criptomoneda'}
    )
    price_fig.update_layout(template='plotly_white')
    
    # Gráfico de capitalización
    latest_df = filtered_df.sort_values('timestamp').drop_duplicates('name', keep='last')
    market_cap_fig = px.bar(
        latest_df,
        x='name',
        y='market_cap_usd',
        color='name',
        title='Capitalización de Mercado (USD)'
    )
    market_cap_fig.update_layout(template='plotly_white')
    
    # Tabla formateada (sin price_change_24h)
    try:
        table_df = latest_df[['name', 'symbol', 'price_usd', 'market_cap_usd']]
        table_df = table_df.rename(columns={
            'price_usd': 'Precio (USD)',
            'market_cap_usd': 'Cap. de Mercado'
        })
        # Formatear columnas
        table_df['Precio (USD)'] = table_df['Precio (USD)'].apply(lambda x: f"${x:,.2f}")
        table_df['Cap. de Mercado'] = table_df['Cap. de Mercado'].apply(lambda x: f"${x:,.0f}")
        
        table = dash_table.DataTable(
            data=table_df.to_dict('records'),
            columns=[{'name': col, 'id': col} for col in table_df.columns],
            style_table={'overflowX': 'auto'},
            style_cell={'textAlign': 'left'},
            style_header={'backgroundColor': '#f2f2f2', 'fontWeight': 'bold'}
        )
    except Exception as e:
        logger.error(f"Error al generar tabla: {e}")
        return {}, {}, None, "Error al generar la tabla"
    
    return price_fig, market_cap_fig, table, ""

# Callback para actualizar la lista de noticias
@app.callback(
    Output('news-list', 'children'),
    [Input('interval-component', 'n_intervals')]
)
def update_news(_):
    news_df = get_news_from_mysql()
    if news_df.empty:
        return [html.Li("No hay noticias disponibles")]
    items = []
    for _, row in news_df.iterrows():
        items.append(html.Li([
            html.A(row['title'], href=row['url'], target='_blank'),
            html.Span(f" - {row['timestamp']}", style={'fontSize': 'smaller'})
        ]))
    return items

# Ejecutar app
if __name__ == '__main__':
    app.run(debug=True)