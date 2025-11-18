import pandas as pd
import plotly.express as px

# Load data
df = pd.read_csv("matched_data/sam_noah_odmp.csv")

# Calculate Age
df["Age at Death"] = (df["Death Date"] - df["Birth Date"]).dt.days / 365.25

# Number of Memorial Highways by County
fig = px.bar(
    df.groupby("COUNTY", as_index=False).size(),
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

# Sam's validation vs confidence
fig = px.box(
    df,
    x="Does_Sam_Think_is_real",
    y="match_confidence",
    title="Match Confidence Distribution by Validation Status",
    color="Does_Sam_Think_is_real",
)
fig.show()

# Noah's validation vs confidence
fig = px.box(
    df,
    x="Noah_Thinks_is_Real",
    y="match_confidence",
    title="Match Confidence Distribution by Validation Status",
    color="Noah_Thinks_is_Real",
)
fig.show()

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

# Military vs Non-Military
fig = px.pie(
    df,
    names="involved_in_military",
    title="Military vs Non-Military Honorees"
)
fig.show()
