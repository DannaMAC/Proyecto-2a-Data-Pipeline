"""
Visualización: Dashboard interactivo para los datos de criptomonedas
"""
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pymongo import MongoClient
from dash import Dash, html, dcc, Input, Output, dash_table
from sqlalchemy import create_engine
from datetime import datetime, timedelta

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
        
        # Obtener los últimos datos
        pipeline = [
            {"$sort": {"timestamp": -1}},
            {"$limit": 100},
            {"$project": {
                "_id": 0,
                "name": 1,
                "symbol": 1,
                "current_price": 1,
                "market_cap": 1,
                "price_change_24h": 1,
                "timestamp": 1
            }}
        ]
        
        data = list(collection.aggregate(pipeline))
        return pd.DataFrame(data) if data else pd.DataFrame()
    
    except Exception as e:
        print(f"Error al obtener datos de MongoDB: {e}")
        return pd.DataFrame()

# Función para obtener datos desde MySQL usando SQLAlchemy
def get_data_from_mysql():
    try:
        engine = create_engine(
            f"mysql+mysqlconnector://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/{MYSQL_DATABASE}"
        )
        
        # Consulta para obtener los precios más recientes
        query = """
        SELECT name, symbol, current_price, market_cap, price_change_24h, timestamp
        FROM crypto_prices
        ORDER BY timestamp DESC
        LIMIT 100
        """
        
        df = pd.read_sql(query, engine)
        return df
    
    except Exception as e:
        print(f"Error al obtener datos de MySQL: {e}")
        return pd.DataFrame()

# Función para obtener noticias desde MySQL
def get_news_from_mysql():
    try:
        engine = create_engine(
            f"mysql+mysqlconnector://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/{MYSQL_DATABASE}"
        )
        
        # Consulta para obtener las noticias más recientes
        query = """
        SELECT title, link, timestamp
        FROM crypto_news
        ORDER BY timestamp DESC
        LIMIT 10
        """
        
        df = pd.read_sql(query, engine)
        return df
    
    except Exception as e:
        print(f"Error al obtener noticias de MySQL: {e}")
        return pd.DataFrame()

# Inicializar la aplicación Dash
app = Dash(__name__, title="Crypto Data Dashboard")

# Definir el layout del dashboard
app.layout = html.Div([
    # Encabezado
    html.H1("Dashboard de Criptomonedas", style={'textAlign': 'center'}),
    
    # Selector de base de datos
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
        dash_table.DataTable(
            id='crypto-table',
            style_table={'overflowX': 'auto'},
            style_cell={'textAlign': 'left'},
            style_header={
                'backgroundColor': 'rgb(230, 230, 230)',
                'fontWeight': 'bold'
            }
        )
    ], style={'margin': '20px'}),
    
    # Noticias recientes
    html.Div([
        html.H2("Noticias Recientes de Criptomonedas"),
        html.Ul(id='news-list')
    ], style={'margin': '20px'}),
    
    # Intervalo para actualización automática
    dcc.Interval(
        id='interval-component',
        interval=60*1000,  # Actualizar cada 60 segundos
        n_intervals=0
    )
])

# Callback para actualizar la lista de criptomonedas en el dropdown
@app.callback(
    Output('crypto-selector', 'options'),
    [Input('database-selector', 'value'),
     Input('interval-component', 'n_intervals')]
)
def update_crypto_options(db_source, n_intervals):
    df = get_data_from_mongodb() if db_source == 'mongodb' else get_data_from_mysql()
    if df.empty:
        return []
    
    # Obtener lista única de criptomonedas
    options = [{'label': name, 'value': name} for name in sorted(df['name'].unique())]
    
    # Si no hay selecciones previas, preseleccionar las 5 primeras
    if not options:
        return []
    
    return options

# Callback para actualizar los gráficos y la tabla
@app.callback(
    [Output('price-chart', 'figure'),
     Output('market-cap-chart', 'figure'),
     Output('crypto-table', 'data'),
     Output('crypto-table', 'columns')],
    [Input('database-selector', 'value'),
     Input('crypto-selector', 'value'),
     Input('interval-component', 'n_intervals')]
)
def update_charts_and_table(db_source, selected_cryptos, n_intervals):
    # Obtener datos
    df = get_data_from_mongodb() if db_source == 'mongodb' else get_data_from_mysql()
    
    if df.empty:
        empty_fig = {
            'data': [],
            'layout': {
                'title': 'No hay datos disponibles',
                'height': 400
            }
        }
        return empty_fig, empty_fig, [], []
    
    # Convertir timestamp a datetime si es necesario
    if 'timestamp' in df.columns and not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
        df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    # Filtrar por criptomonedas seleccionadas
    if selected_cryptos:
        filtered_df = df[df['name'].isin(selected_cryptos)]
    else:
        # Si no hay selecciones, mostrar las 5 con mayor cap. de mercado
        top_cryptos = df.sort_values('market_cap', ascending=False)['name'].unique()[:5]
        filtered_df = df[df['name'].isin(top_cryptos)]
    
    if filtered_df.empty:
        empty_fig = {
            'data': [],
            'layout': {
                'title': 'No hay datos disponibles para las criptomonedas seleccionadas',
                'height': 400
            }
        }
        return empty_fig, empty_fig, [], []
    
    # Crear gráfico de precios
    price_fig = px.line(
        filtered_df, 
        x='timestamp', 
        y='current_price', 
        color='name',
        title='Evolución de Precios',
        labels={'current_price': 'Precio (USD)', 'timestamp': 'Fecha', 'name': 'Criptomoneda'}
    )
    
    # Crear gráfico de capitalización de mercado
    market_cap_fig = px.bar(
        filtered_df.sort_values('timestamp').drop_duplicates('name', keep='last'),
        x='name', 
        y='market_cap',
        color='name',
        title='Capitalización de Mercado',
        labels={'market_cap': 'Cap. de Mercado (USD)', 'name': 'Criptomoneda'}
    )
    
    # Preparar datos para la tabla
    table_df = filtered_df.sort_values('timestamp').drop_duplicates('name', keep='last')
    table_df = table_df[['name', 'symbol', 'current_price', 'market_cap', 'price_change_24h']]
    
    # Formatear columnas para la tabla
    formatted_df = table_df.copy()
    formatted_df['current_price'] = formatted_df['current_price'].apply(lambda x: f"${x:.2f}")
    formatted_df['market_cap'] = formatted_df['market_cap'].apply(lambda x: f"${x:,.0f}")
    formatted_df['price_change_24h'] = formatted_df['price_change_24h'].apply(
        lambda x: f"{x:.2f}" + "%" if pd.notna(x) else "N/A"
    )
    
    # Definir columnas para la tabla
    columns = [
        {"name": "Nombre", "id": "name"},
        {"name": "Símbolo", "id": "symbol"},
        {"name": "Precio (USD)", "id": "current_price"},
        {"name": "Cap. de Mercado", "id": "market_cap"},
        {"name": "Cambio 24h", "id": "price_change_24h"}
    ]
    
    return price_fig, market_cap_fig, formatted_df.to_dict('records'), columns

# Callback para actualizar la lista de noticias
@app.callback(
    Output('news-list', 'children'),
    [Input('interval-component', 'n_intervals')]
)
def update_news(n_intervals):
    news_df = get_news_from_mysql()
    
    if news_df.empty:
        return [html.Li("No hay noticias disponibles")]
    
    # Crear lista de noticias con enlaces
    news_items = []
    for _, row in news_df.iterrows():
        news_items.append(html.Li([
            html.A(row['title'], href=row['link'], target='_blank'),
            html.Span(f" - {row['timestamp']}", style={'fontSize': 'smaller'})
        ]))
    
    return news_items

# Ejecutar la aplicación sin reloader
if __name__ == '__main__':
    app.run(debug=True, use_reloader=False)

