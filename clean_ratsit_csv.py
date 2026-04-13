import json
import re
import time
from pathlib import Path

import pandas as pd
import requests

INPUT = "jonkoping_foreningar_enriched.csv"
OUTPUT = "jonkoping_foreningar_cleaned.csv"

GEOCODE_CACHE = "geocode_cache.json"
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
HEADERS = {
    "User-Agent": "oskar-foreningar-geocoding/1.0 (local analysis)"
}
RETRY_FAILED_CACHE = True

def clean_text(text: str) -> str:
    text = str(text or "").replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text

def extract_between(text: str, start_label: str, end_labels: list[str]) -> str:
    text = str(text or "")
    escaped_start = re.escape(start_label)
    escaped_ends = "|".join(re.escape(x) for x in end_labels)
    pattern = rf"{escaped_start}\s*(.*?)\s*(?=(?:{escaped_ends})|$)"
    match = re.search(pattern, text, flags=re.DOTALL | re.IGNORECASE)
    if not match:
        return ""
    return clean_text(match.group(1))

def extract_date(text: str, label: str, end_labels: list[str]) -> str:
    value = extract_between(text, label, end_labels)
    m = re.search(r"\b\d{4}-\d{2}-\d{2}\b", value)
    return m.group(0) if m else ""

def extract_sni_parts(text: str):
    text = str(text or "")
    m = re.search(
        r"Svensk näringsgrensindelning SNI\s+(.*?)\s*(?=Verksamhetsbeskrivning|Firmateckning|Ansvariga personer|$)",
        text,
        flags=re.DOTALL | re.IGNORECASE,
    )
    if not m:
        return "", "", 0, [], []

    sni_block = clean_text(m.group(1))
    matches = re.findall(r"(\d{5})\s*-\s*([^\d]+?)(?=(?:\s+\d{5}\s*-)|$)", sni_block)
    if not matches:
        return "", clean_text(sni_block), 0, [], []

    codes = [clean_text(code) for code, _ in matches]
    texts = [clean_text(desc) for _, desc in matches]

    primary_code = codes[0] if codes else ""
    primary_text = texts[0] if texts else ""
    sni_count = len(codes)

    return primary_code, primary_text, sni_count, codes, texts

def normalize_employer(value: str) -> str:
    v = clean_text(value).lower()
    if "ej registrerad som arbetsgivare" in v:
        return "Nej"
    if "registrerad som arbetsgivare" in v:
        return "Ja"
    return clean_text(value)

def normalize_f_skatt(value: str) -> str:
    v = clean_text(value).lower()
    if "ej registrerad för f-skatt" in v:
        return "Nej"
    if "registrerad för f-skatt" in v:
        return "Ja"
    return clean_text(value)

def normalize_moms(value: str) -> str:
    v = clean_text(value).lower()
    if "ej aktiv i momsregistret" in v:
        return "Nej"
    if "aktiv i momsregistret" in v:
        return "Ja"
    return clean_text(value)

def extract_status_clean(text: str) -> str:
    text = str(text or "")
    # Ex: "Aktiv Adress ..." eller "Avregistrerad 2026-02-27 Adress ..."
    m = re.search(r"^(.*?)\s+Adress\b", text, flags=re.DOTALL)
    if m:
        return clean_text(m.group(1))
    return clean_text(text[:80])

def extract_city_clean(text: str) -> str:
    text = str(text or "")
    # Tar första delen före Kontaktpersoner
    text = re.split(r"\bKontaktpersoner\b", text, maxsplit=1, flags=re.IGNORECASE)[0]
    return clean_text(text)

def extract_sate(text: str) -> str:
    return extract_between(
        text,
        "Säte:",
        ["Registrering", "Bolaget bildat:", "Bolaget registrerat:"]
    )


def extract_gatuadress(text: str) -> str:
    return extract_between(
        text,
        "Gatuadress:",
        ["Postadress:", "Säte:", "Registrering", "Bolaget bildat:", "Bolaget registrerat:"]
    )


def extract_postadress(text: str) -> str:
    return extract_between(
        text,
        "Postadress:",
        ["Säte:", "Registrering", "Bolaget bildat:", "Bolaget registrerat:"]
    )


# --- Postnummer och postort helpers ---
def extract_postnummer(postadress: str) -> str:
    text = str(postadress or "")
    m = re.search(r"(\d{3}\s?\d{2})", text)
    return m.group(1).replace(" ", "") if m else ""


def extract_postort(postadress: str) -> str:
    text = str(postadress or "")
    m = re.search(r"\d{3}\s?\d{2}\s+(.*)", text)
    return clean_text(m.group(1)) if m else ""

def extract_registrerad(text: str) -> str:
    return extract_date(
        text,
        "Bolaget registrerat:",
        ["Firmanamn registrerat:", "Bolagsordning:", "Skatteuppgifter"]
    )

def extract_arbetsgivare(text: str) -> str:
    return normalize_employer(
        extract_between(
            text,
            "Arbetsgivare:",
            ["F-skatt:", "Moms:", "Momsregistreringsnummer"]
        )
    )

def extract_f_skatt(text: str) -> str:
    return normalize_f_skatt(
        extract_between(
            text,
            "F-skatt:",
            ["Moms:", "Momsregistreringsnummer", "Svensk näringsgrensindelning SNI"]
        )
    )

def extract_moms(text: str) -> str:
    return normalize_moms(
        extract_between(
            text,
            "Moms:",
            ["Momsregistreringsnummer", "Svensk näringsgrensindelning SNI"]
        )
    )


# --- Fordon helpers ---
def extract_fordon_antal(text: str) -> int:
    text = str(text or "")

    patterns = [
        r"(\d+)\s+fordon\s+registrerat",
        r"(\d+)\s+fordon\s+registrerade",
        r"(\d+)\s+registrerade?\s+fordon",
    ]

    for pattern in patterns:
        m = re.search(pattern, text, flags=re.IGNORECASE)
        if m:
            try:
                return int(m.group(1))
            except Exception:
                return 0

    return 0


def extract_har_fordon(text: str) -> str:
    antal = extract_fordon_antal(text)
    return "Ja" if antal > 0 else "Nej"


def build_full_address(row) -> str:
    parts = [
        clean_text(row.get("gatuadress", "")),
        clean_text(row.get("postnummer", "")),
        clean_text(row.get("postort", "")),
        "Sweden",
    ]
    parts = [part for part in parts if part]
    return ", ".join(parts)


def load_geocode_cache() -> dict:
    path = Path(GEOCODE_CACHE)
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_geocode_cache(cache: dict) -> None:
    Path(GEOCODE_CACHE).write_text(
        json.dumps(cache, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def geocode_address(address: str, postnummer: str, postort: str, cache: dict) -> tuple[float | None, float | None]:
    address = clean_text(address)
    postnummer = clean_text(postnummer)
    postort = clean_text(postort)

    # Boxadresser är ofta dåliga för exakt geokodning. Låt fallback på postnummer/postort ta över snabbare.
    lower_address = address.lower()
    if lower_address.startswith("box ") or " box " in lower_address:
        address = ""

    # --- 1. Försök med full adress ---
    if address:
        if address in cache:
            cached = cache[address]
            if cached.get("lat") is not None and cached.get("lon") is not None:
                return cached.get("lat"), cached.get("lon")
            if not RETRY_FAILED_CACHE:
                return cached.get("lat"), cached.get("lon")

        try:
            response = requests.get(
                NOMINATIM_URL,
                params={
                    "q": address,
                    "format": "jsonv2",
                    "limit": 1,
                    "countrycodes": "se",
                },
                headers=HEADERS,
                timeout=20,
            )
            response.raise_for_status()
            data = response.json()
            if data:
                lat = float(data[0]["lat"])
                lon = float(data[0]["lon"])
                cache[address] = {"lat": lat, "lon": lon}
                time.sleep(1.1)
                return lat, lon
        except Exception:
            pass

    # --- 2. Fallback: postnummer + ort ---
    fallback_key = f"{postnummer} {postort}".strip()
    if fallback_key:
        if fallback_key in cache:
            cached = cache[fallback_key]
            if cached.get("lat") is not None and cached.get("lon") is not None:
                return cached.get("lat"), cached.get("lon")
            if not RETRY_FAILED_CACHE:
                return cached.get("lat"), cached.get("lon")

        try:
            response = requests.get(
                NOMINATIM_URL,
                params={
                    "q": fallback_key + ", Sweden",
                    "format": "jsonv2",
                    "limit": 1,
                    "countrycodes": "se",
                },
                headers=HEADERS,
                timeout=20,
            )
            response.raise_for_status()
            data = response.json()
            if data:
                lat = float(data[0]["lat"])
                lon = float(data[0]["lon"])
                cache[fallback_key] = {"lat": lat, "lon": lon}
                time.sleep(1.1)
                return lat, lon
        except Exception:
            pass

    # --- 3. Sista fallback: bara postnummer ---
    if postnummer:
        if postnummer in cache:
            cached = cache[postnummer]
            if cached.get("lat") is not None and cached.get("lon") is not None:
                return cached.get("lat"), cached.get("lon")
            if not RETRY_FAILED_CACHE:
                return cached.get("lat"), cached.get("lon")

        try:
            response = requests.get(
                NOMINATIM_URL,
                params={
                    "q": postnummer + ", Sweden",
                    "format": "jsonv2",
                    "limit": 1,
                    "countrycodes": "se",
                },
                headers=HEADERS,
                timeout=20,
            )
            response.raise_for_status()
            data = response.json()
            if data:
                lat = float(data[0]["lat"])
                lon = float(data[0]["lon"])
                cache[postnummer] = {"lat": lat, "lon": lon}
                time.sleep(1.1)
                return lat, lon
        except Exception:
            pass

    # --- Misslyckades ---
    cache[address or fallback_key or postnummer] = {"lat": None, "lon": None}
    time.sleep(1.1)
    return None, None


df = pd.read_csv(INPUT, encoding="utf-8-sig")

# Använd status-kolumnen som rå Ratsit-text
raw = df["status"].fillna("")

df["city_clean"] = df["city"].fillna("").apply(extract_city_clean)
df["gatuadress_clean"] = raw.apply(extract_gatuadress)
df["postadress_clean"] = raw.apply(extract_postadress)

df["postnummer"] = df["postadress_clean"].apply(extract_postnummer)
df["postort"] = df["postadress_clean"].apply(extract_postort)
df["sate_clean"] = raw.apply(extract_sate)
df["status_clean"] = raw.apply(extract_status_clean)
df["registrerad_datum_clean"] = raw.apply(extract_registrerad)
df["arbetsgivare_clean"] = raw.apply(extract_arbetsgivare)
df["f_skatt_clean"] = raw.apply(extract_f_skatt)
df["moms_clean"] = raw.apply(extract_moms)

sni_parts = raw.apply(extract_sni_parts)
df["sni_kod_clean"] = sni_parts.apply(lambda x: x[0])
df["sni_text_clean"] = sni_parts.apply(lambda x: x[1])
df["sni_antal"] = sni_parts.apply(lambda x: x[2])

max_sni_count = int(df["sni_antal"].max()) if not df["sni_antal"].isna().all() else 0
for i in range(max_sni_count):
    df[f"SNI{i+1}"] = sni_parts.apply(lambda x: x[3][i] if len(x[3]) > i else "")
    df[f"SNI{i+1}_text"] = sni_parts.apply(lambda x: x[4][i] if len(x[4]) > i else "")

df["fordon_antal"] = raw.apply(extract_fordon_antal)
print(f"Fordon hittade på bolag: {(df['fordon_antal'] > 0).sum()} st")
df["har_fordon"] = raw.apply(extract_har_fordon)

df["riskindikator_fordon_utan_skattflaggor"] = df.apply(
    lambda row: "Ja"
    if row.get("har_fordon") == "Ja"
    and row.get("arbetsgivare") == "Nej"
    and row.get("f_skatt") == "Nej"
    else "Nej",
    axis=1,
)

# Valfritt: skriv över gamla kolumner med de rena värdena
df["city"] = df["city_clean"]
df["gatuadress"] = df["gatuadress_clean"]
df["postadress"] = df["postadress_clean"]
df["registrerad_datum"] = df["registrerad_datum_clean"]

# Gör datumet Excel-vänligt (riktigt datumfält)
df["registrerad_datum_sort"] = pd.to_datetime(df["registrerad_datum"], errors="coerce")

# Excel-vänliga hjälpkolumner för enklare filtrering och sortering
current_year = pd.Timestamp.today().year
reg_year = df["registrerad_datum_sort"].dt.year

df["registrerad_ar"] = reg_year
df["föreningsålder_år"] = reg_year.apply(lambda y: current_year - int(y) if pd.notna(y) else pd.NA)
df["ny_forening_3_ar"] = reg_year.apply(lambda y: "Ja" if pd.notna(y) and int(y) >= current_year - 3 else "Nej")
df["ny_forening_5_ar"] = reg_year.apply(lambda y: "Ja" if pd.notna(y) and int(y) >= current_year - 5 else "Nej")

df["arbetsgivare"] = df["arbetsgivare_clean"]
df["f_skatt"] = df["f_skatt_clean"]
df["moms"] = df["moms_clean"]
df["sni"] = df["sni_kod_clean"]
df["status"] = df["status_clean"]

# Bygg full adress och geokoda till koordinater
# Postnummer kan ibland ha blivit float i CSV/Excel, t.ex. 59731.0
if "postnummer" in df.columns:
    df["postnummer"] = (
        df["postnummer"]
        .astype(str)
        .str.replace(r"\.0$", "", regex=True)
        .str.replace(r"\s+", "", regex=True)
        .str.strip()
    )

if "postort" in df.columns:
    df["postort"] = df["postort"].astype(str).str.strip()

cache = load_geocode_cache()
df["full_address"] = df.apply(build_full_address, axis=1)

coords = []
for i, row in df.iterrows():
    address = row["full_address"]
    postnummer = row.get("postnummer", "")
    postort = row.get("postort", "")

    print(f"Geokodar {i+1}/{len(df)}: {address} | fallback: {postnummer} {postort}")

    coords.append(
        geocode_address(address, postnummer, postort, cache)
    )

save_geocode_cache(cache)
df["lat"] = [c[0] for c in coords]
df["lon"] = [c[1] for c in coords]
print(f"Geokodade träffar: {df['lat'].notna().sum()} av {len(df)}")

# Ta bort onödiga kolumner (hjälp- och clean-kolumner som inte ska med till Excel)
df = df.drop(columns=[
    "föreningsålder_år",
    "ny_forening_3_ar",
    "ny_forening_5_ar",
    "riskindikator_fordon_utan_skattflaggor",
    "datakalla",
    "city_clean",
    "gatuadress_clean",
    "postadress_clean",
    "arbetsgivare_clean",
    "f_skatt_clean",
    "moms_clean",
], errors="ignore")
# Välj kolumnordning
preferred_cols = [
    "name",
    "orgnr",
    "email",
    "website",
    "city",
    "gatuadress",
    "postadress",
    "postnummer",
    "postort",
    "full_address",
    "lat",
    "lon",
    "sate_clean",
    "registrerad_datum",
    "registrerad_datum_sort",
    "registrerad_ar",
    "arbetsgivare",
    "f_skatt",
    "moms",
    "fordon_antal",
    "har_fordon",
    "sni",
    "sni_text_clean",
    "sni_antal",
    "bolagsform",
    "status",
]

remaining = [c for c in df.columns if c not in preferred_cols]

sni_dynamic_cols = []
for i in range(1, max_sni_count + 1):
    sni_dynamic_cols.append(f"SNI{i}")
    sni_dynamic_cols.append(f"SNI{i}_text")

remaining = [c for c in remaining if c not in sni_dynamic_cols]

df = df[preferred_cols + sni_dynamic_cols + remaining]

df.to_csv(OUTPUT, index=False, encoding="utf-8-sig")
print(f"Klar: {OUTPUT}")
print(f"Geocode-cache sparad i: {GEOCODE_CACHE}")
print(f"RETRY_FAILED_CACHE = {RETRY_FAILED_CACHE}")
print("Kartfunktion pausad. Endast städad CSV exporteras, nu även med koordinater.")