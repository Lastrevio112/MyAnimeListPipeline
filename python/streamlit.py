import pandas as pd
import duckdb
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

DUCKDB_PATH = "pipeline.duckdb"  # Comitted the entire database into a seperate folder seperate from the VS Code dev container so I can deploy this on streamlit

# 1. Cache the data so it doesn't reload on every click
@st.cache_data
def load_all_tables(db_path):
    tables = {}
    # List of all your table names from the datamart schema
    table_names = [
        "f_anime", 
        "links_demographics", "links_genres", "links_producers", "links_studios", "links_themes", "links_licensors",
        "d_demographics", "d_genres", "d_licensors", "d_producers", "d_studios", "d_themes",
        "d_sources", "d_statuses", "d_types"
    ]
    
    # Use 'with' or connect/close inside the cached function
    con = duckdb.connect(db_path, read_only=True)
    for table in table_names:
        query = f"SELECT * FROM datamart.{table}"
        tables[table] = con.execute(query).fetchdf()
    con.close()
    
    return tables

information_schema = duckdb.connect(DUCKDB_PATH, read_only=True).execute("SELECT * FROM information_schema.columns").fetchdf()

# Load everything into a single dictionary
data = load_all_tables(DUCKDB_PATH)


# --- DATA PREPARATION (Joining Dimensions) ---

# 1. Base Fact DataFrame
df_anime = data['f_anime'].copy()

# 2. Join simple 1:1 Dimensions (Source, Type, Status)
# These are simple merges because one anime has one source/type
df_enriched = df_anime.merge(data['d_sources'], on='source_id', how='left')
df_enriched = df_enriched.merge(data['d_types'], on='type_id', how='left')
df_enriched = df_enriched.merge(data['d_statuses'], on='status_id', how='left')
df_enriched = df_enriched.merge(data['links_licensors'], on='anime_id', how='left')
df_enriched = df_enriched.merge(data['d_licensors'], on='licensor_id', how='left')
df_enriched = df_enriched.merge(data['links_genres'], on='anime_id', how='left')
df_enriched = df_enriched.merge(data['d_genres'], on='genre_id', how='left')

# --- STREAMLIT UI ---

st.title("Anime Popularity Datamart Explorer")
st.markdown("Analyzing what drives success in the anime industry.")

# Sidebar Filters
st.sidebar.header("Global Filters")
year_range = st.sidebar.slider(
    "Select Year Range", 
    int(df_enriched['year'].min()), 
    int(df_enriched['year'].max()), 
    (2010, 2024)
)

filtered_df = df_enriched[
    (df_enriched['year'] >= year_range[0]) & 
    (df_enriched['year'] <= year_range[1])
]


# --- MODULE 1: The "Popularity vs. Quality" Correlation ---
st.header("1. Does Quality (Score) Drive Popularity?")

col1, col2 = st.columns([3, 1])

with col1:
    # Note: In your schema, 'popularity' is a rank (1 = most popular), 
    # so we often plot 'members' to see volume.
    fig_corr = px.scatter(
        filtered_df, 
        x="score", 
        y="members", 
        size="favorites", 
        hover_name="title_english",
        color="genre_name",
        title="Score vs. Member Count (Bubble size = Favorites)",
        labels={"score": "User Score", "members": "Total Members"},
        template="plotly_dark"
    )
    st.plotly_chart(fig_corr, use_container_width=True)

with col2:
    st.write("### Quick Stats")
    avg_score = filtered_df['score'].mean()
    total_members = filtered_df['members'].sum()
    st.metric("Avg Score", f"{avg_score:.2f}")
    st.metric("Total Members", f"{total_members:,}")


# --- MODULE 2: Genre Popularity (Handling the Link Table) ---
st.header("2. Genre Impact on Popularity")

# We need to join: f_anime -> links_genres -> d_genres
df_genres = data['links_genres'].merge(data['d_genres'], on='genre_id')
df_genre_metrics = df_genres.merge(df_enriched[['anime_id', 'members', 'score']], on='anime_id')

# Grouping by genre
genre_summary = df_genre_metrics.groupby('genre_name').agg({
    'anime_id': 'count',
    'members': 'sum',
    'score': 'mean'
}).rename(columns={'anime_id': 'anime_count'}).reset_index()

# Bar chart for genre popularity
fig_genre = px.bar(
    genre_summary.sort_values('members', ascending=False).head(15),
    x='genre_name',
    y='members',
    color='score',
    title="Top 15 Genres by Total Members (Color = Avg Score)",
    labels={'members': 'Total Members', 'genre_name': 'Genre'},
    template="plotly_white"
)
st.plotly_chart(fig_genre, use_container_width=True)


# --- MODULE 3: Source Material Analysis ---
st.header("3. Does the Source Matter?")

# Compare Manga vs Light Novel vs Original
source_analysis = filtered_df.groupby('source').agg({
    'score': 'mean',
    'popularity': 'mean', 
    'anime_id': 'count'
}).reset_index()

fig_source = px.pie(
    source_analysis, 
    values='anime_id', 
    names='source', 
    hole=0.4,
    title="Distribution of Anime by Source Material"
)
st.plotly_chart(fig_source, use_container_width=True)


# Create a summary that shows both Volume (Count) and Success (Members)
source_summary = filtered_df.groupby('source').agg({
    'anime_id': 'count',
    'members': 'mean',  # Average popularity per show
    'score': 'mean'     # Average quality
}).reset_index().sort_values('members', ascending=False)

# Using a Bar Chart to show Average Popularity per Source
fig_source = px.bar(
    source_summary,
    x='source',
    y='members',
    color='score', # Color shows quality
    text='anime_id', # Text on bar shows total count of shows
    title="Average Popularity (Members) by Source Material",
    labels={
        'members': 'Avg Members per Anime', 
        'anime_id': 'Total Shows',
        'score': 'Avg Score'
    },
    template="plotly_dark"
)

fig_source.update_traces(texttemplate='%{text} shows', textposition='outside')
st.plotly_chart(fig_source, use_container_width=True)


# --- MODULE 4: Yearly Trends ---
st.header("4. Temporal Trends: The Evolution of Anime")

# Grouping by year
temporal_df = filtered_df.groupby('year').agg({
    'anime_id': 'count',
    'members': 'sum',
    'score': 'mean'
}).reset_index()

# Create a dual-axis chart: Volume (Bars) and Quality (Line)
fig_temp = go.Figure()

# Bars for Number of Anime released
fig_temp.add_trace(go.Bar(
    x=temporal_df['year'],
    y=temporal_df['anime_id'],
    name="Total Releases",
    marker_color='rgba(100, 149, 237, 0.6)'
))

# Line for Average Score
fig_temp.add_trace(go.Scatter(
    x=temporal_df['year'],
    y=temporal_df['score'],
    name="Avg Score",
    yaxis="y2",
    line=dict(color='firebrick', width=3)
))

# Layout for dual axis
fig_temp.update_layout(
    title="Release Volume vs. Average Score Over Time",
    xaxis=dict(title="Year"),
    yaxis=dict(title="Number of Anime Releases"),
    yaxis2=dict(title="Average User Score", overlaying="y", side="right", range=[0, 10]),
    legend=dict(x=0.01, y=0.99),
    template="plotly_dark"
)

st.plotly_chart(fig_temp, use_container_width=True)


st.header("5. Which Genres Owned Each Era?")

# Join Genres to the filtered fact table
df_genre_temp = data['links_genres'].merge(data['d_genres'], on='genre_id')
df_genre_temp = df_genre_temp.merge(filtered_df[['anime_id', 'year', 'members']], on='anime_id')

# Group by Year and Genre, then get total members
genre_pivot = df_genre_temp.groupby(['year', 'genre_name'])['members'].sum().reset_index()

# Get the top 10 most popular genres overall to keep the chart clean
top_10_genres = genre_pivot.groupby('genre_name')['members'].sum().nlargest(10).index
genre_pivot_filtered = genre_pivot[genre_pivot['genre_name'].isin(top_10_genres)]

fig_genre_trend = px.area(
    genre_pivot_filtered, 
    x="year", 
    y="members", 
    color="genre_name",
    title="Total Members (Popularity) by Genre Over Time",
    labels={"members": "Cumulative Members", "year": "Year"},
    template="plotly_dark",
    line_group="genre_name"
)

st.plotly_chart(fig_genre_trend, use_container_width=True)

# 1. We use the same 'genre_pivot_filtered' logic from before
# 2. We use the 'groupnorm' parameter in Plotly to normalize to 100%

fig_genre_pct = px.area(
    genre_pivot_filtered, 
    x="year", 
    y="members", 
    color="genre_name",
    title="Relative Market Share (%) of Top Genres Over Time",
    labels={"members": "Share of Total Popularity (%)", "year": "Year"},
    template="plotly_dark",
    line_group="genre_name"
)

# This is the 'magic' line that turns it into a 100% stacked chart
fig_genre_pct.update_layout(yaxis=dict(ticksuffix="%", range=[0, 100]))
fig_genre_pct.update_traces(stackgroup='one', groupnorm='percent') 

st.plotly_chart(fig_genre_pct, use_container_width=True)


st.header("6. Which Studios are the Most Efficient at Turning Quality into Popularity?")

# 1. Join Fact -> Links -> Dimensions
df_studios = data['links_studios'].merge(data['d_studios'], on='studio_id')
df_studios_metrics = df_studios.merge(filtered_df[['anime_id', 'score', 'members']], on='anime_id')

# 2. Aggregate metrics by studio
studio_stats = df_studios_metrics.groupby('studio_name').agg({
    'anime_id': 'count',
    'score': 'mean',
    'members': 'mean'
}).reset_index()

# 3. Filter for studios with at least 5 productions to avoid "one-hit wonders"
studio_stats = studio_stats[studio_stats['anime_id'] >= 5]

fig_studio = px.scatter(
    studio_stats,
    x="score",
    y="members",
    size="anime_id",
    hover_name="studio_name",
    title="Studio Efficiency (Avg Score vs. Avg Popularity)",
    labels={
        "score": "Average User Score",
        "members": "Average Members per Show",
        "anime_id": "Total Shows Produced"
    },
    template="plotly_dark",
    color="score"
)

# Add a reference line for the average score across all studios
avg_industry_score = studio_stats['score'].mean()
fig_studio.add_vline(x=avg_industry_score, line_dash="dot", line_color="gray", annotation_text="Industry Avg")

st.plotly_chart(fig_studio, use_container_width=True)


st.header("6. The Industry Power Players")
st.markdown("Comparing the 'Hit Rate' of the companies that create, fund, and distribute anime.")

# Helper function to join and aggregate Many-to-Many dimensions
def get_dim_metrics(dim_table_name, link_table_name, join_col):
    # Join: Fact -> Link -> Dimension
    df_link = data[link_table_name].merge(data[dim_table_name], on=join_col)
    df_combined = df_link.merge(filtered_df[['anime_id', 'score', 'members']], on='anime_id')
    
    # Aggregate
    stats = df_combined.groupby(f"{dim_table_name[2:-1]}_name").agg({
        'anime_id': 'count',
        'score': 'mean',
        'members': 'mean'
    }).reset_index()
    
    # Filter for significant players (min 5 titles)
    return stats[stats['anime_id'] >= 5]

# 1. Get the data for all three
studio_stats = get_dim_metrics('d_studios', 'links_studios', 'studio_id')
producer_stats = get_dim_metrics('d_producers', 'links_producers', 'producer_id')

# Note: d_licensors is a 1:1 or 1:Many in your schema (no links table), 
# so we join it directly to the enriched dataframe
licensor_stats = filtered_df.groupby('licensor_name').agg({
    'anime_id': 'count',
    'score': 'mean',
    'members': 'mean'
}).reset_index()
licensor_stats = licensor_stats[licensor_stats['anime_id'] >= 5]

# --- RENDER 3 COLUMNS ---
col1, col2, col3 = st.columns(3)

with col1:
    st.subheader("Studios")
    fig_st = px.scatter(studio_stats, x="score", y="members", size="anime_id", 
                        hover_name="studio_name", template="plotly_dark", color_discrete_sequence=['#636EFA'])
    st.plotly_chart(fig_st, use_container_width=True)

with col2:
    st.subheader("Producers (Funders)")
    fig_pr = px.scatter(producer_stats, x="score", y="members", size="anime_id", 
                        hover_name="producer_name", template="plotly_dark", color_discrete_sequence=['#EF553B'])
    st.plotly_chart(fig_pr, use_container_width=True)

with col3:
    st.subheader("Licensors (Distributors)")
    fig_li = px.scatter(licensor_stats, x="score", y="members", size="anime_id", 
                        hover_name="licensor_name", template="plotly_dark", color_discrete_sequence=['#00CC96'])
    st.plotly_chart(fig_li, use_container_width=True)


# --- DATA TABLE VIEW ---
with st.expander("View Raw Enriched Data"):
    st.dataframe(filtered_df)