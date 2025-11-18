import pandas as pd
import plotly.express as px

# Load data
df = pd.read_csv("wikipedia_api_scraper/merged_output.csv")

# Calculate Age
df["Birth Date"] = pd.to_datetime(df["Birth Date"], errors="coerce")
df["Death Date"] = pd.to_datetime(df["Death Date"], errors="coerce")
df["Age at Death"] = (df["Death Date"] - df["Birth Date"]).dt.days / 365.25

# Number of Memorial Highways by County
df_count = df.groupby("COUNTY", as_index=False).size().sort_values("size", ascending=False)
fig = px.bar(
    df_count,
    x="COUNTY",
    y="size",
    title="Number of Memorial Highways by County",
    labels={"size": "Count"}
)
fig.show()

# Histogram of Occupations
fig = px.histogram(
    df,
    x="Primary Occupation",
    title="Primary Occupations of Memorial Honorees",
    color="Primary Occupation",
)
fig.update_xaxes(categoryorder="total descending")
fig.show()

'''# Sam's validation vs confidence
fig = px.box(
    df,
    x="Does_Sam_Think_is_real",
    y="match_confidence",
    title="Match Confidence Distribution by Validation Status",
    color="Does_Sam_Think_is_real",
)
fig.show()'''

'''# Noah's validation vs confidence
fig = px.box(
    df,
    x="Noah_Thinks_is_Real",
    y="match_confidence",
    title="Match Confidence Distribution by Validation Status",
    color="Noah_Thinks_is_Real",
)
fig.show()'''

# Age at Death
fig = px.histogram(
    df,
    x="Age at Death",
    nbins=20,
    title="Distribution of Age at Death"
)
fig.show()

# Gender
fig = px.pie(
    df,
    names="Sex",
    title="Gender Distribution of Memorial Honorees"
)
fig.show()

'''# Military vs Non-Military
fig = px.pie(
    df,
    names="involved_in_military",
    title="Military vs Non-Military Honorees"
)
fig.show()
'''

# Birthdate Histogram
df_clean = df.dropna(subset=["Birth Date"])
fig = px.histogram(
    df_clean,
    x="Birth Date",
    title="Distribution of Birth Dates",
    labels={"Birth Date": "Birth Year"}
)
fig.show()