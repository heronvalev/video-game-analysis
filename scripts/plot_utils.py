import sqlite3
import plotly.graph_objects as go

def top_genres_chart(db_path):
    """
    Generate a bar chart showing the top 10 most common genres in the dataset.

    Args:
        db_path (str): Path to the SQLite database file.

    Returns:
        str: HTML representation of the Plotly chart.
    """
    # Connect to the SQLite database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Query
    query = """
        SELECT genres.genre_name, COUNT(*) AS count
        FROM game_genres
        JOIN genres ON game_genres.genre_id = genres.genre_id
        GROUP BY genres.genre_name
        ORDER BY count DESC
        LIMIT 10
    """
    cursor.execute(query)
    results = cursor.fetchall()

    # Close connection
    conn.close()

    # Prepare Plotly data
    genres = [row[0] for row in results]
    counts = [row[1] for row in results]

    # Generate Plotly bar chart
    fig = go.Figure(
        go.Bar(
            x=counts,
            y=genres,
            orientation='h',
            marker_color='#fd7e14'
        )
    )

    fig.update_layout(
        title="Top 10 Genres",
        xaxis_title="Number of Games",
        yaxis_title="Genre",
        yaxis=dict(autorange='reversed'),
        template="plotly_dark",
        margin=dict(l=120, r=20, t=50, b=50)
    )

    # Return the HTML representation of the chart
    return fig.to_html(full_html=False)

def top_categories_chart(db_path):
    """
    Generate a bar chart showing the top 10 most common categories in the dataset.

    Args:
        db_path (str): Path to the SQLite database file.

    Returns:
        str: HTML representation of the Plotly chart.
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    query = """
        SELECT c.category_name, COUNT(*) AS count
        FROM categories c
        JOIN game_categories gc ON c.category_id = gc.category_id
        GROUP BY c.category_name
        ORDER BY count DESC
        LIMIT 10
    """

    cursor.execute(query)
    results = cursor.fetchall()
    conn.close()

    categories = [row[0] for row in results]
    counts = [row[1] for row in results]

    fig = go.Figure(
        go.Bar(
            x=counts,
            y=categories,
            orientation='h',
            marker_color='#28a745'
        )
    )

    fig.update_layout(
        title="Top 10 Categories",
        xaxis_title="Number of Games",
        yaxis_title="Category",
        yaxis=dict(autorange='reversed'),
        template="plotly_dark",
        margin=dict(l=120, r=20, t=50, b=50)
    )

    return fig.to_html(full_html=False)

def top_steamspy_tags_chart(db_path):
    """
    Generate a bar chart showing the top 10 most common SteamSpy tags in the dataset.

    Args:
        db_path (str): Path to the SQLite database file.

    Returns:
        str: HTML representation of the Plotly chart.
    """

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    query = """
        SELECT t.tag_name, COUNT(*) AS count
        FROM game_steamspy_tags gst
        JOIN steamspy_tags t ON gst.steamspy_tag_id = t.tag_id
        GROUP BY t.tag_name
        ORDER BY count DESC
        LIMIT 10
    """

    cursor.execute(query)
    results = cursor.fetchall()
    conn.close()

    tags = [row[0] for row in results]
    counts = [row[1] for row in results]

    fig = go.Figure(
        go.Bar(
            x=counts,
            y=tags,
            orientation='h',
            marker_color='#ffc107'
        )
    )

    fig.update_layout(
        title="Top 10 Tags",
        xaxis_title="Number of Games",
        yaxis_title="Tag",
        yaxis=dict(autorange='reversed'),
        template="plotly_dark",
        margin=dict(l=120, r=20, t=50, b=50)
    )

    return fig.to_html(full_html=False)

def top_developers_chart(db_path):
    """
    Generate a bar chart showing the top 10 developers
    with the most games in the dataset.

    Args:
        db_path (str): Path to the SQLite database file.

    Returns:
        str: HTML representation of the Plotly chart.
    """

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    query = """
        SELECT developer, COUNT(*) AS count
        FROM games
        GROUP BY developer
        ORDER BY count DESC
        LIMIT 10
    """

    cursor.execute(query)
    results = cursor.fetchall()
    conn.close()

    developers = [row[0] for row in results]
    counts = [row[1] for row in results]

    fig = go.Figure(
        go.Bar(
            x=counts,
            y=developers,
            orientation='h',
            marker_color='dodgerblue'
        )
    )

    fig.update_layout(
        title="Top 10 Developers",
        xaxis_title="Number of Games",
        yaxis_title="Developer",
        yaxis=dict(autorange='reversed'),
        template="plotly_dark",
        margin=dict(l=150, r=20, t=50, b=50)
    )

    return fig.to_html(full_html=False)

def top_publishers_chart(db_path):
    """
    Generate a bar chart showing the top 10 publishers
    with the most games in the dataset.

    Args:
        db_path (str): Path to the SQLite database file.

    Returns:
        str: HTML representation of the Plotly chart.
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    query = """
        SELECT publisher, COUNT(*) AS count
        FROM games
        GROUP BY publisher
        ORDER BY count DESC
        LIMIT 10
    """

    cursor.execute(query)
    results = cursor.fetchall()
    conn.close()

    publishers = [row[0] for row in results]
    counts = [row[1] for row in results]

    fig = go.Figure(
        go.Bar(
            x=counts,
            y=publishers,
            orientation='h',
            marker_color='#6f42c1'
        )
    )

    fig.update_layout(
        title="Top 10 Publishers",
        xaxis_title="Number of Games",
        yaxis_title="Publisher",
        yaxis=dict(autorange='reversed'),
        template="plotly_dark",
        margin=dict(l=150, r=20, t=50, b=50)
    )

    return fig.to_html(full_html=False)

def pricing_vs_playtime_tag_chart(db_path):
    """
    Show top 10 most expensive tags by average price,
    alongside average playtime.

    Args:
        db_path (str): Path to the SQLite database file.

    Returns:
        str: HTML representation of the Plotly chart.
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Query: join games -> game_genres -> genres, and ratings to get playtime
    query = """
        SELECT 
            t.tag_name,
            AVG(g.price) AS avg_price,
            AVG(r.average_playtime) / 60.0 AS avg_playtime_hours
        FROM games g
        JOIN ratings r ON g.appid = r.appid
        JOIN game_steamspy_tags gst ON g.appid = gst.appid
        JOIN steamspy_tags t ON gst.steamspy_tag_id = t.tag_id
        WHERE g.price > 0 AND r.average_playtime > 0
        GROUP BY t.tag_name
        ORDER BY avg_price DESC
        LIMIT 10;
    """
    cursor.execute(query)
    results = cursor.fetchall()
    conn.close()

    tags = [row[0] for row in results]
    avg_prices = [round(float(row[1]), 2) if row[1] is not None else 0.0 for row in results]
    avg_playtimes = [round(float(row[2]), 2) if row[2] is not None else 0.0 for row in results]

    fig = go.Figure()

    # Bar trace for average price
    fig.add_trace(go.Bar(
        x=tags,
        y=avg_prices,
        name="Avg Price (£)",
        marker_color="teal",
        yaxis="y1"
    ))

    # Line trace for average playtime
    fig.add_trace(go.Scatter(
        x=tags,
        y=avg_playtimes,
        name="Avg Playtime (hrs)",
        mode="lines+markers",
        marker=dict(color="lightblue"),
        line=dict(width=2),
        yaxis="y2"
    ))

    fig.update_layout(
        title="Top 10 Most Expensive Tags: Price vs Average Playtime",
        xaxis_title="Genre",
        yaxis=dict(
            title="Avg Price (£)",
            side="left",
            showgrid=True
        ),
        yaxis2=dict(
            title="Avg Playtime (hrs)",
            overlaying="y",
            side="right",
            showgrid=False
        ),
        template="plotly_dark",
        margin=dict(l=60, r=60, t=60, b=120),
        legend=dict(x=0.5, y=-0.3, orientation="h", xanchor="center"),
        bargap=0.25
    )

    fig.update_xaxes(tickangle=-20, automargin=True)

    return fig.to_html(full_html=False)

def pricing_vs_rating_tag_chart(db_path):
    """
    Show top 10 most expensive tags by average price,
    alongside average percentage of positive user ratings.

    Args:
        db_path (str): Path to the SQLite database file.

    Returns:
        str: HTML representation of the Plotly chart.
    """

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    query = """
        SELECT 
            t.tag_name,
            AVG(g.price) AS avg_price,
            AVG(CAST(r.positive_ratings AS FLOAT) / 
                (r.positive_ratings + r.negative_ratings) * 100) AS avg_positive_rating_pct
        FROM games g
        JOIN ratings r ON g.appid = r.appid
        JOIN game_steamspy_tags gst ON g.appid = gst.appid
        JOIN steamspy_tags t ON gst.steamspy_tag_id = t.tag_id
        WHERE g.price > 0 
          AND r.positive_ratings IS NOT NULL 
          AND r.negative_ratings IS NOT NULL 
          AND (r.positive_ratings + r.negative_ratings) > 0
        GROUP BY t.tag_name
        ORDER BY avg_price DESC
        LIMIT 10;
    """

    cursor.execute(query)
    results = cursor.fetchall()
    conn.close()

    tags = [row[0] for row in results]
    avg_prices = [round(float(row[1]), 2) if row[1] is not None else 0.0 for row in results]
    avg_ratings = [round(float(row[2])) if row[2] is not None else 0.0 for row in results]

    fig = go.Figure()

    # Bar trace for average price
    fig.add_trace(go.Bar(
        x=tags,
        y=avg_prices,
        name="Avg Price (£)",
        marker_color="crimson",
        yaxis="y1"
    ))

    # Line trace for average positive rating percentage
    fig.add_trace(go.Scatter(
        x=tags,
        y=avg_ratings,
        name="Avg Positive Rating (%)",
        mode="lines+markers",
        marker=dict(color="lightblue"),
        yaxis="y2"
    ))

    fig.update_layout(
        title="Top 10 Most Expensive Tags: Price vs Positive Rating",
        xaxis_title="Tag",
        yaxis=dict(
            title="Avg Price (£)",
            side="left"
        ),
        yaxis2=dict(
            title="Avg Positive Rating (%)",
            overlaying="y",
            side="right"
        ),
        template="plotly_dark",
        margin=dict(l=60, r=60, t=50, b=80),
        legend=dict(x=0.5, y=-0.3, orientation="h", xanchor="center")
    )

    return fig.to_html(full_html=False)


def free_vs_paid_chart(db_path):
    """
    Generate a pie chart showing the distribution of free vs paid games.

    Args:
        db_path (str): Path to the SQLite database file.

    Returns:
        str: HTML representation of the Plotly chart.
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    query = """
        SELECT
            CASE WHEN price = 0 THEN 'Free' ELSE 'Paid' END AS price_type,
            COUNT(*) AS count
        FROM games
        GROUP BY price_type
    """

    cursor.execute(query)
    results = cursor.fetchall()
    conn.close()

    labels = [row[0] for row in results]
    counts = [row[1] for row in results]

    fig = go.Figure(
        go.Pie(
            labels=labels,
            values=counts,
            textinfo='percent+label',
            marker=dict(colors=["#9b59b6", "#f1c40f"], line=dict(color='white', width=2))
        )
    )

    fig.update_layout(
        title="Distribution of Free vs Paid Games",
        template="plotly_dark",
        margin=dict(l=60, r=60, t=60, b=60)
    )
    return fig.to_html(full_html=False)

def achievements_vs_rating_chart(db_path):
    """
    Generate a bar chart showing average rating against availability of achievements.

    Args:
        db_path (str): Path to the SQLite database file.

    Returns:
        str: HTML representation of the Plotly chart.
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    query = """
        SELECT 
            CASE WHEN achievements > 0 THEN 'Has Achievements' ELSE 'No Achievements' END AS achievement_group,
            AVG(CAST(positive_ratings AS FLOAT) / (positive_ratings + negative_ratings) * 100) AS avg_positive_rating_pct
        FROM ratings
        WHERE positive_ratings IS NOT NULL AND negative_ratings IS NOT NULL
          AND (positive_ratings + negative_ratings) > 0
        GROUP BY achievement_group
    """
    cursor.execute(query)
    results = cursor.fetchall()
    conn.close()

    rating_map = {'Has Achievements': 0, 'No Achievements': 0}
    for group, avg_rating in results:
        rating_map[group] = avg_rating or 0

    x = list(rating_map.keys())
    y = [rating_map[x[0]], round(rating_map[x[1]])]

    fig = go.Figure(data=[
        go.Bar(
            x=x,
            y=y,
            text=[f"{round(v)}%" for v in y],
            textposition='outside',
            marker_color=['#1f77b4', '#ff7f0e']
        )
    ])

    fig.update_layout(
        title='Average Positive Rating vs Achievements Availability',
        yaxis_title='Average Positive Rating (%)',
        yaxis_range=[0, 100],
        template="plotly_dark",
        showlegend=False
    )

    return fig.to_html(full_html=False)

def games_released_over_time_chart(db_path):
    """
    Generate a line chart showing how many games were released each year.

    Args:
        db_path (str): Path to the SQLite database file.

    Returns:
        str: HTML representation of the Plotly chart.
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    query = """
        SELECT 
            STRFTIME('%Y', release_date) AS year,
            COUNT(*) AS total_games
        FROM games
        WHERE release_date IS NOT NULL
        GROUP BY year
        ORDER BY year
    """
    cursor.execute(query)
    results = cursor.fetchall()
    conn.close()

    years = [row[0] for row in results]
    counts = [row[1] for row in results]

    fig = go.Figure(data=[
        go.Scatter(
            x=years,
            y=counts,
            mode='lines+markers',
            line=dict(shape='linear'),
            marker=dict(size=6)
        )
    ])

    fig.update_layout(
        title='Number of Games Released Per Year',
        xaxis_title='Year',
        yaxis_title='Number of Games',
        xaxis_tickangle=-45,
        showlegend=False,
        template="plotly_dark"
    )

    return fig.to_html(full_html=False)