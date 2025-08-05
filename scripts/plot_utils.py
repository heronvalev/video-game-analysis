import sqlite3
import plotly.graph_objects as go

def top_genres_chart(db_path):
    """
    Generate a bar chart showing the top 10 most common genres in the dataset.

    Args:
        db_path (str): Path to the SQLite database file.

    Returns:
        str: HTML representation of the Plotly bar chart to embed in templates.
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
            marker_color='mediumseagreen'
        )
    )

    fig.update_layout(
        title="Top 10 Most Common Genres",
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
        str: HTML representation of the Plotly bar chart to embed in templates.
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
            marker_color='mediumseagreen'
        )
    )

    fig.update_layout(
        title="Top 10 Most Common Categories",
        xaxis_title="Number of Games",
        yaxis_title="Category",
        yaxis=dict(autorange='reversed'),
        template="plotly_dark",
        margin=dict(l=120, r=20, t=50, b=50)
    )

    return fig.to_html(full_html=False)

def top_steamspy_tag_chart(db_path):
    """
    Generate a bar chart showing the top 10 most common SteamSpy tags in the dataset.

    Args:
        db_path (str): Path to the SQLite database file.

    Returns:
        str: HTML representation of the Plotly bar chart to embed in templates.
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
            marker_color='indianred'
        )
    )

    fig.update_layout(
        title="Top 10 Most Frequent Tags",
        xaxis_title="Number of Games",
        yaxis_title="Tag",
        yaxis=dict(autorange='reversed'),
        template="plotly_dark",
        margin=dict(l=120, r=20, t=50, b=50)
    )

    return fig.to_html(full_html=False)