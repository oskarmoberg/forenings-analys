import folium
import pandas as pd
import streamlit as st
from streamlit_folium import st_folium

DATA_FILE = "jonkoping_foreningar_cleaned.csv"


@st.cache_data
def load_data() -> pd.DataFrame:
    df = pd.read_csv(DATA_FILE)

    # Städning av vanliga CSV/Excel-problem
    if "postnummer" in df.columns:
        df["postnummer"] = (
            df["postnummer"]
            .astype(str)
            .str.replace(r"\.0$", "", regex=True)
            .str.replace(r"\s+", "", regex=True)
            .str.strip()
        )

    numeric_cols = ["registrerad_ar", "lat", "lon", "fordon_antal", "sni", "sni_antal"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        else:
            df[col] = pd.NA

    # Ålder på förening om kolumnen saknas eller är trasig
    current_year = pd.Timestamp.today().year
    if "föreningsålder_år" not in df.columns:
        df["föreningsålder_år"] = df["registrerad_ar"].apply(
            lambda y: current_year - int(y) if pd.notna(y) else pd.NA
        )
    else:
        df["föreningsålder_år"] = pd.to_numeric(df["föreningsålder_år"], errors="coerce")

    # Strängkolumner
    string_cols = [
        "name",
        "orgnr",
        "gatuadress",
        "postadress",
        "arbetsgivare",
        "f_skatt",
        "moms",
        "har_fordon",
        "sni_text_clean",
        "bolagsform",
        "status",
    ]
    for col in string_cols:
        if col not in df.columns:
            df[col] = ""
        df[col] = df[col].fillna("")

    return df


def make_marker_color(row: pd.Series) -> str:
    has_vehicle = str(row.get("har_fordon", "")).strip().lower() == "ja"
    is_employer = str(row.get("arbetsgivare", "")).strip().lower() == "ja"

    if has_vehicle and is_employer:
        return "orange"
    if has_vehicle:
        return "red"
    if is_employer:
        return "green"
    return "blue"


st.set_page_config(page_title="Föreningskarta", layout="wide")
st.title("Föreningskarta – föreningsdashboard")
st.caption("Visualisering baserad på färdig CSV med latitude och longitude.")


df = load_data()

if df.empty:
    st.error(f"Ingen data hittades i {DATA_FILE}.")
    st.stop()

with_coords = df.dropna(subset=["lat", "lon"]).copy()
without_coords = len(df) - len(with_coords)

st.info(
    f"Totalt antal föreningar: {len(df)} | Med koordinater: {len(with_coords)} | Utan koordinater: {without_coords}"
)
st.markdown(
    "**Färgförklaring:** 🟠 Har både fordon och arbetsgivare &nbsp;&nbsp; 🔴 Har fordon &nbsp;&nbsp; 🟢 Arbetsgivare = Ja &nbsp;&nbsp; 🔵 Övriga"
)

if with_coords.empty:
    st.warning("CSV-filen innehåller inga användbara koordinater ännu.")
    st.stop()

valid_years = with_coords["registrerad_ar"].dropna()
if valid_years.empty:
    st.error("Kolumnen 'registrerad_ar' saknar användbara värden.")
    st.stop()

min_year = int(valid_years.min())
max_year = int(valid_years.max())

valid_sni = sorted([int(x) for x in with_coords["sni"].dropna().unique()])

st.sidebar.header("Filter")

year_range = st.sidebar.slider(
    "Registreringsår",
    min_year,
    max_year,
    (max(max_year - 5, min_year), max_year),
)

selected_sni = st.sidebar.multiselect(
    "SNI-nummer",
    options=valid_sni,
    default=[],
)

arbetsgivare_filter = st.sidebar.selectbox(
    "Arbetsgivare",
    options=["Alla", "Ja", "Nej"],
    index=0,
)

fordon_filter = st.sidebar.selectbox(
    "Har fordon",
    options=["Alla", "Ja", "Nej"],
    index=0,
)

filtered = with_coords.copy()
filtered = filtered[
    (filtered["registrerad_ar"] >= year_range[0])
    & (filtered["registrerad_ar"] <= year_range[1])
]

if selected_sni:
    filtered = filtered[filtered["sni"].isin(selected_sni)]

if arbetsgivare_filter != "Alla":
    filtered = filtered[
        filtered["arbetsgivare"].str.strip().str.lower() == arbetsgivare_filter.lower()
    ]

if fordon_filter != "Alla":
    filtered = filtered[
        filtered["har_fordon"].str.strip().str.lower() == fordon_filter.lower()
    ]

col1, col2, col3, col4, col5 = st.columns(5)
vehicle_mask = filtered["har_fordon"].str.strip().str.lower() == "ja"
employer_mask = filtered["arbetsgivare"].str.strip().str.lower() == "ja"
both_mask = vehicle_mask & employer_mask

col1.metric("Visade föreningar", len(filtered))
col2.metric("Med fordon", int(vehicle_mask.sum()))
col3.metric("Arbetsgivare = Ja", int(employer_mask.sum()))
col4.metric("Fordon + arbetsgivare", int(both_mask.sum()))
col5.metric("Unika SNI", int(filtered["sni"].dropna().nunique()))

if filtered.empty:
    st.warning("Inga föreningar matchar nuvarande filter.")
    st.stop()

center_lat = filtered["lat"].mean()
center_lon = filtered["lon"].mean()

m = folium.Map(location=[center_lat, center_lon], zoom_start=11)

for _, row in filtered.iterrows():
    popup = folium.Popup(
        (
            f"<b>{row.get('name', '-') or '-'}</b><br>"
            f"Org.nr: {row.get('orgnr', '-') or '-'}<br>"
            f"Registrerad år: {int(row['registrerad_ar']) if pd.notna(row['registrerad_ar']) else '-'}<br>"
            f"Ålder: {int(row['föreningsålder_år']) if pd.notna(row['föreningsålder_år']) else '-'} år<br>"
            f"SNI: {int(row['sni']) if pd.notna(row['sni']) else '-'} {row.get('sni_text_clean', '')}<br>"
            f"Arbetsgivare: {row.get('arbetsgivare', '-') or '-'}<br>"
            f"Har fordon: {row.get('har_fordon', '-') or '-'}<br>"
            f"Fordon antal: {int(row['fordon_antal']) if pd.notna(row['fordon_antal']) else 0}<br>"
            f"Adress: {row.get('gatuadress', '-') or '-'}, {row.get('postadress', '-') or '-'}"
        ),
        max_width=360,
    )

    folium.CircleMarker(
        location=[row["lat"], row["lon"]],
        radius=7,
        color=make_marker_color(row),
        fill=True,
        fill_opacity=0.8,
        popup=popup,
    ).add_to(m)

st.success("Kartan är laddad nedan.")
st_folium(m, width=1400, height=700)

st.subheader("Tabell över filtrerade föreningar")
show_cols = [
    "name",
    "orgnr",
    "registrerad_ar",
    "föreningsålder_år",
    "sni",
    "sni_text_clean",
    "arbetsgivare",
    "har_fordon",
    "fordon_antal",
    "gatuadress",
    "postadress",
    "lat",
    "lon",
]
show_cols = [col for col in show_cols if col in filtered.columns]
st.dataframe(filtered[show_cols].sort_values(["registrerad_ar", "name"], ascending=[False, True]), use_container_width=True)
