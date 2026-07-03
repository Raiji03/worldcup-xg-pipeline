"""
World Cup Data Project — STEP 3: Explore (UI)
-----------------------------------------------
A Streamlit dashboard over the `shots` table built by the worldcup_etl Airflow DAG
(dags/worldcup_dag.py). Run the DAG first so output/worldcup.duckdb exists.

Setup:
    pip install streamlit plotly duckdb pandas

Run (from the airflow-worldcup project root):
    streamlit run app.py
"""

import duckdb
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

DB_FILE = "output/worldcup.duckdb"

# Fixed color palette so charts stay visually consistent across reruns/filters.
# CATEGORICAL is used for things with no natural order (play patterns).
# BLUE_SEQ is a sequential ramp used to shade bars by magnitude (e.g. top scorers).
# The rest are semantic single colors: BLUE/RED for over/under performance,
# GOOD for goals, MUTED for non-events, and GRID/AXIS/INK/SURFACE for chart chrome.
CATEGORICAL = ["#2a78d6", "#1baf7a", "#eda100", "#008300",
               "#4a3aa7", "#e34948", "#e87ba4", "#eb6834"]
BLUE_SEQ = ["#cde2fb", "#9ec5f4", "#5598e7", "#2a78d6", "#184f95"]
BLUE, RED, GOOD, MUTED = "#2a78d6", "#e34948", "#0ca30c", "#898781"
GRID, AXIS, INK, SURFACE = "#e1e0d9", "#c3c2b7", "#0b0b0b", "#fcfcfb"

st.set_page_config(page_title="World Cup 2022 — Shot Explorer", layout="wide")


def make_axis_labels_readable(fig):
    """
    Force axis tick labels and titles to use the INK color.

    Plotly's default axis styling can end up low-contrast against SURFACE
    depending on theme, so this is applied to every chart right before
    rendering to keep labels legible.

    Args:
        fig: a plotly.graph_objects.Figure to mutate in place.
    Returns:
        The same figure, for chaining.
    """
    fig.update_xaxes(color=INK, tickfont=dict(color=INK), title_font=dict(color=INK))
    fig.update_yaxes(color=INK, tickfont=dict(color=INK), title_font=dict(color=INK))
    return fig


@st.cache_resource
def get_con():
    # cache_resource (not cache_data) because a DuckDB connection is a live
    # resource/object, not serializable data — Streamlit should reuse the
    # same connection object across reruns instead of trying to pickle it.
    return duckdb.connect(DB_FILE, read_only=True)


@st.cache_data
def load_shots():
    # cache_data because this returns a DataFrame (plain data). Streamlit
    # hashes the return value and skips re-querying DuckDB on reruns that
    # don't change the underlying inputs (there are none here, so this
    # effectively runs once per session/file change).
    return get_con().execute("SELECT * FROM shots").df()


shots = load_shots()
teams_all = sorted(shots["team"].dropna().unique())
patterns_all = sorted(shots["play_pattern"].dropna().unique())
# Assign each play pattern a stable color by cycling through CATEGORICAL,
# so the same pattern always renders the same color across tabs/reruns.
pattern_color = {p: CATEGORICAL[i % len(CATEGORICAL)] for i, p in enumerate(patterns_all)}

st.title("World Cup 2022 — Shot & xG Explorer")

# --- Filters -----------------------------------------------------------
# All three filters are optional and combine with AND logic below.
f1, f2, f3 = st.columns(3)
sel_teams = f1.multiselect("Team", teams_all)
sel_patterns = f2.multiselect("Play pattern", patterns_all)
player_search = f3.text_input("Player contains")

df = shots
if sel_teams:
    df = df[df["team"].isin(sel_teams)]
if sel_patterns:
    df = df[df["play_pattern"].isin(sel_patterns)]
if player_search:
    # Case-insensitive substring match; na=False keeps rows with missing
    # player names from raising/matching incorrectly.
    df = df[df["player"].str.contains(player_search, case=False, na=False)]

# --- KPI summary row -----------------------------------------------------
# xG (expected goals) is a per-shot probability of scoring based on shot
# quality (location, angle, pattern, etc). Summing it gives a team/player's
# "expected" goal output, which we compare against actual goals below to
# measure over/underperformance (finishing above or below what the shot
# quality alone would predict).
total_shots = len(df)
total_goals = int(df["is_goal"].sum())
total_xg = df["xg"].sum()
conv = (total_goals / total_shots * 100) if total_shots else 0

k1, k2, k3, k4 = st.columns(4)
k1.metric("Shots", f"{total_shots:,}")
k2.metric("Goals", f"{total_goals:,}")
k3.metric("Total xG", f"{total_xg:.1f}")
k4.metric("Conversion", f"{conv:.1f}%")

st.divider()

tab_team, tab_player, tab_pattern, tab_map, tab_table = st.tabs(
    ["Team xG", "Top Scorers", "Play Patterns", "Shot Map", "Data Table"]
)

# --- Tab 1: Team xG overperformance --------------------------------------
# Shows which teams scored more (or fewer) goals than their shot quality
# predicted. Positive = clinical finishing / overperformance (blue),
# negative = wasteful finishing / underperformance (red).
with tab_team:
    team_stats = (
        df.groupby("team", as_index=False)
        .agg(xg=("xg", "sum"), goals=("is_goal", "sum"))
    )
    # Goals minus xG: the core "overperformance" metric for this tab.
    team_stats["over_under"] = (team_stats["goals"] - team_stats["xg"]).round(1)
    # Only show teams that actually scored, so the chart isn't cluttered
    # with teams sitting at/near zero on both goals and xG.
    team_stats = team_stats[team_stats["goals"] >= 1].sort_values("over_under", ascending=False)

    if team_stats.empty:
        st.info("No goals in the current filter selection.")
    else:
        colors = [RED if v < 0 else BLUE for v in team_stats["over_under"]]
        fig = go.Figure(go.Bar(
            x=team_stats["over_under"], y=team_stats["team"], orientation="h",
            marker_color=colors, text=team_stats["over_under"], textposition="outside",
            hovertemplate="%{y}<br>Goals minus xG: %{x:+.1f}<extra></extra>",
        ))
        fig.update_layout(
            title="Goals vs expected goals (overperformance)",
            plot_bgcolor=SURFACE, paper_bgcolor=SURFACE, font_color=INK,
            xaxis=dict(gridcolor=GRID, zerolinecolor=AXIS, title=dict(text="Goals minus xG", font=dict(color="black"))),
            yaxis=dict(autorange="reversed"),  # keep top performer at the top of the bar chart
            margin=dict(l=10, r=10, t=40, b=10),
        )
        make_axis_labels_readable(fig)
        st.plotly_chart(fig, width="stretch")

# --- Tab 2: Top scorers ---------------------------------------------------
# Ranks players by raw goal count (not xG), with xG shown on hover as
# context for whether their goal tally over/underrepresents shot quality.
with tab_player:
    player_stats = (
        df.groupby("player", as_index=False)
        .agg(goals=("is_goal", "sum"), xg=("xg", "sum"))
    )
    player_stats = player_stats[player_stats["goals"] >= 1].sort_values("goals", ascending=False).head(15)

    if player_stats.empty:
        st.info("No goals in the current filter selection.")
    else:
        max_goals = player_stats["goals"].max()

        def shade(v):
            # Map a player's goal count onto the 5-step BLUE_SEQ ramp so
            # higher scorers render darker/more saturated bars.
            idx = int((v / max_goals) * (len(BLUE_SEQ) - 1)) if max_goals else 0
            return BLUE_SEQ[idx]

        colors = [shade(v) for v in player_stats["goals"]]
        fig = go.Figure(go.Bar(
            x=player_stats["goals"], y=player_stats["player"], orientation="h",
            marker_color=colors, text=player_stats["goals"], textposition="outside",
            customdata=player_stats["xg"],
            hovertemplate="%{y}<br>Goals: %{x}<br>xG: %{customdata:.2f}<extra></extra>",
        ))
        fig.update_layout(
            title="Top scorers",
            plot_bgcolor=SURFACE, paper_bgcolor=SURFACE, font_color=INK,
            xaxis=dict(gridcolor=GRID, title=dict(text="Goals", font=dict(color="black"))),
            yaxis=dict(autorange="reversed"),  # highest scorer at the top
            margin=dict(l=10, r=10, t=40, b=10),
        )
        make_axis_labels_readable(fig)
        st.plotly_chart(fig, width="stretch")

# --- Tab 3: Play patterns --------------------------------------------------
# "Play pattern" is StatsBomb's classification of how a possession/attack
# started (e.g. regular play, corner, free kick, counter). This shows which
# patterns produced the most goals (and shot volume, via hover) in the
# current filter.
with tab_pattern:
    pattern_stats = (
        df.groupby("play_pattern", as_index=False)
        .agg(goals=("is_goal", "sum"), shots=("is_goal", "count"))
        .sort_values("goals", ascending=False)
    )

    if pattern_stats.empty:
        st.info("No shots in the current filter selection.")
    else:
        colors = [pattern_color.get(p, MUTED) for p in pattern_stats["play_pattern"]]
        fig = go.Figure(go.Bar(
            x=pattern_stats["play_pattern"], y=pattern_stats["goals"],
            marker_color=colors, text=pattern_stats["goals"], textposition="outside",
            customdata=pattern_stats["shots"],
            hovertemplate="%{x}<br>Goals: %{y}<br>Shots: %{customdata}<extra></extra>",
        ))
        fig.update_layout(
            title="Goals by play pattern",
            plot_bgcolor=SURFACE, paper_bgcolor=SURFACE, font_color=INK,
            yaxis=dict(gridcolor=GRID, title=dict(text="Goals", font=dict(color="black"))),
            margin=dict(l=10, r=10, t=40, b=10),
        )
        make_axis_labels_readable(fig)
        st.plotly_chart(fig, width="stretch")

# --- Tab 4: Shot map --------------------------------------------------------
# Plots every shot at its pitch location. Coordinates follow StatsBomb's
# standard 120 x 80 pitch grid (units are "StatsBomb yards", not meters),
# with x=0 at the defending team's own goal line and x=120 at the attacking
# goal line being shot at. The rectangle at x=102-120 approximates the
# 18-yard box; the vertical line at x=60 marks the halfway line.
with tab_map:
    st.caption("Shot locations in StatsBomb pitch coordinates (120 x 80)")
    goal_df = df[df["is_goal"] == 1]
    miss_df = df[df["is_goal"] == 0]

    fig = go.Figure()
    fig.add_shape(type="rect", x0=0, y0=0, x1=120, y1=80, line=dict(color=AXIS))  # pitch outline
    fig.add_shape(type="line", x0=60, y0=0, x1=60, y1=80, line=dict(color=GRID))  # halfway line
    fig.add_shape(type="rect", x0=102, y0=18, x1=120, y1=62, line=dict(color=GRID))  # penalty box
    # Draw misses first so goal markers render on top and aren't obscured.
    fig.add_trace(go.Scatter(
        x=miss_df["x"], y=miss_df["y"], mode="markers", name="No goal",
        marker=dict(size=8, color=MUTED, opacity=0.6),
        text=miss_df["player"] + " — " + miss_df["outcome"],
        hovertemplate="%{text}<extra></extra>",
    ))
    fig.add_trace(go.Scatter(
        x=goal_df["x"], y=goal_df["y"], mode="markers", name="Goal",
        marker=dict(size=10, color=GOOD, line=dict(color="white", width=1)),
        text=goal_df["player"] + " — Goal",
        hovertemplate="%{text}<extra></extra>",
    ))
    fig.update_layout(
        plot_bgcolor=SURFACE, paper_bgcolor=SURFACE, font_color=INK,
        xaxis=dict(range=[0, 120], visible=False),
        # scaleanchor/scaleratio locks y to x's scale so the pitch isn't
        # visually stretched/squished regardless of container width.
        yaxis=dict(range=[0, 80], visible=False, scaleanchor="x", scaleratio=1),
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
        margin=dict(l=10, r=10, t=40, b=10),
        height=500,
    )
    st.plotly_chart(fig, width="stretch")

# --- Tab 5: Raw data table --------------------------------------------------
# Escape hatch for inspecting/verifying individual shot records behind the
# charts above, sorted chronologically within each match.
with tab_table:
    st.dataframe(
        df.sort_values(["match_id", "minute"]).reset_index(drop=True),
        width="stretch",
        height=500,
    )
    st.caption(f"{len(df):,} rows shown")
