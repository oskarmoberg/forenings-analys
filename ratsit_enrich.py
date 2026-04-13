from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
import csv
import os
import re
import time

INPUT_CSV = "jonkoping_foreningar.csv"
OUTPUT_CSV = "jonkoping_foreningar_enriched.csv"
MAX_ROWS = None  # sätt till None för fler, men börja litet
# Tips: håll liten testmängd tills företagsprofil-länkarna ser rätt ut
DEBUG_DIR = "ratsit_debug"

def normalize_orgnr(orgnr: str) -> str:
    return re.sub(r"\D", "", orgnr or "")

def normalize_name_for_match(name: str) -> str:
    name = (name or "").lower().strip()
    name = name.replace("&", " och ")
    # Gör 'a6' och liknande till 'a 6' så att A6 Golfklubb matchar A 6 GOLFKLUBB
    name = re.sub(r"([a-zA-ZåäöÅÄÖ])(\d)", r"\1 \2", name)
    name = re.sub(r"(\d)([a-zA-ZåäöÅÄÖ])", r"\1 \2", name)
    name = re.sub(r"[^a-z0-9åäö\s]", " ", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name


def find_result_link(page, orgnr: str, name: str):
    formatted_orgnr = f"{orgnr[:6]}-{orgnr[6:]}"
    wanted = normalize_name_for_match(name)

    links = page.locator("a")
    count = links.count()

    best_candidate = None

    for i in range(count):
        try:
            candidate = links.nth(i)
            href = candidate.get_attribute("href") or ""
            text = candidate.inner_text(timeout=500).strip()

            # Skippa interna ankarlänkar och tomma länkar
            if not href or href.startswith("#"):
                continue

            # Vi vill ha riktiga företagslänkar, inte navigation eller sökfilter.
            # Men hoppa inte över själva företagsprofil-länkar om de råkar innehålla orgnr i sluggen.
            if "/sok/" in href and orgnr not in href.replace("-", ""):
                continue

            normalized_text = normalize_name_for_match(text)

            # 1. Bäst: direkt orgnr i href eller text
            if orgnr in href.replace("-", "") or formatted_orgnr in text:
                return candidate

            # 2. Bra: företagslänk där namnet matchar hyggligt
            if wanted and wanted in normalized_text:
                best_candidate = candidate
        except Exception:
            pass

    return best_candidate

def safe_text(locator) -> str:
    try:
        return locator.inner_text(timeout=3000).strip()
    except Exception:
        return ""


def save_debug(page, name: str):
    os.makedirs(DEBUG_DIR, exist_ok=True)
    safe_name = re.sub(r"[^a-zA-Z0-9_-]", "_", name)
    try:
        page.screenshot(path=f"{DEBUG_DIR}/{safe_name}.png", full_page=True)
    except Exception:
        pass


def find_visible_search_input(page):
    selectors = [
        "input[placeholder*='Sök']",
        "input[placeholder*='person eller företag']",
        "input[type='search']",
        "input[type='text']",
    ]

    for selector in selectors:
        locator = page.locator(selector)
        count = locator.count()
        for i in range(count):
            try:
                candidate = locator.nth(i)
                if candidate.is_visible(timeout=1000) and candidate.is_enabled(timeout=1000):
                    return candidate
            except Exception:
                pass

    return None

def extract_between(text: str, start_label: str, end_labels: list[str]) -> str:
    escaped_start = re.escape(start_label)
    escaped_ends = "|".join(re.escape(x) for x in end_labels)
    pattern = rf"{escaped_start}:\s*(.*?)\s*(?=(?:{escaped_ends}):|$)"
    match = re.search(pattern, text, flags=re.DOTALL)
    if not match:
        return ""
    value = match.group(1)
    value = value.replace("\n", " ")
    value = re.sub(r"\s+", " ", value).strip()
    return value


def clean_value(value: str) -> str:
    value = (value or "").replace("\xa0", " ")
    value = re.sub(r"\s+", " ", value).strip()
    return value


def first_sentence_or_line(value: str) -> str:
    value = clean_value(value)
    if not value:
        return ""

    # Klipp bort vanliga efterföljande etiketter eller brus som ibland följer med
    split_markers = [
        "Visa alla arbetsställen",
        "Läs mer",
        "Mer information",
        "Alla arbetsställen",
        "Omsättning",
        "Vinst",
        "Anställda",
    ]
    for marker in split_markers:
        if marker in value:
            value = value.split(marker, 1)[0].strip()

    return clean_value(value)


def normalize_registration(value: str) -> str:
    value = first_sentence_or_line(value)
    match = re.search(r"\b\d{4}-\d{2}-\d{2}\b", value)
    return match.group(0) if match else value


def normalize_yes_no_text(value: str) -> str:
    value = first_sentence_or_line(value)
    mappings = {
        "Registrerad som arbetsgivare": "Ja",
        "Ej registrerad som arbetsgivare": "Nej",
        "Registrerad för F-skatt": "Ja",
        "Ej registrerad för F-skatt": "Nej",
        "Aktiv i momsregistret": "Ja",
        "Ej aktiv i momsregistret": "Nej",
    }
    for src, target in mappings.items():
        if src.lower() in value.lower():
            return target
    return value


def normalize_sni(value: str) -> str:
    value = first_sentence_or_line(value)
    match = re.search(r"\b\d{5}\b", value)
    return match.group(0) if match else value

def parse_company_page(page) -> dict:
    # Läs hela sidan som text och plocka ut fälten mellan etiketter
    body_text = page.locator("body").inner_text()
    print(f"    Parserar sida: {page.url}")
    print(f"    Innehåller 'Bolaget registrerat': {'Bolaget registrerat' in body_text}")
    print(f"    Innehåller 'Arbetsgivare': {'Arbetsgivare' in body_text}")

    registrerad_datum = extract_between(
        body_text,
        "Bolaget registrerat",
        ["Firmanamn registrerat", "Bolagsordning", "Skatteuppgifter"]
    )
    arbetsgivare = extract_between(
        body_text,
        "Arbetsgivare",
        ["F-skatt", "Moms", "Momsregistreringsnummer"]
    )
    f_skatt = extract_between(
        body_text,
        "F-skatt",
        ["Moms", "Momsregistreringsnummer", "Svensk näringsgrensindelning SNI"]
    )
    moms = extract_between(
        body_text,
        "Moms",
        ["Momsregistreringsnummer", "Svensk näringsgrensindelning SNI"]
    )
    sni = extract_between(
        body_text,
        "Svensk näringsgrensindelning SNI",
        ["Verksamhetsbeskrivning", "Firmateckning"]
    )
    bolagsform = extract_between(
        body_text,
        "Bolagsform",
        ["Säte", "Status", "Adress"]
    )
    status = extract_between(
        body_text,
        "Status",
        ["Adress", "Registrering"]
    )

    return {
        "registrerad_datum": normalize_registration(registrerad_datum),
        "arbetsgivare": normalize_yes_no_text(arbetsgivare),
        "f_skatt": normalize_yes_no_text(f_skatt),
        "moms": normalize_yes_no_text(moms),
        "sni": normalize_sni(sni),
        "bolagsform": first_sentence_or_line(bolagsform),
        "status": first_sentence_or_line(status),
    }

def accept_cookies(page):
    # Ratsit använder ofta Cookiebot i en iframe. Försök både i huvudsidan och i iframes.
    labels = [
        "Tillåt alla cookies",
        "Endast nödvändiga cookies",
        "Godkänn alla cookies",
        "Godkänn",
        "Acceptera",
    ]

    frames = [page] + list(page.frames)

    for frame in frames:
        for label in labels:
            try:
                btn = frame.locator(f"text={label}").first
                if btn.count() > 0 and btn.is_visible(timeout=1500):
                    btn.click(timeout=3000)
                    page.wait_for_timeout(1500)
                    print(f"Cookies hanterade via: {label}")
                    return True
            except Exception:
                pass

    print("Ingen cookie-popup hittades")
    return False


def new_page(browser):
    page = browser.new_page()
    page.set_default_timeout(10000)
    return page


def search_orgnr(page, orgnr: str):
    # Låt sidan stabilisera sig och hantera eventuella cookies igen
    try:
        page.wait_for_load_state("networkidle", timeout=5000)
    except Exception:
        pass

    accept_cookies(page)
    page.wait_for_timeout(1500)

    search_input = find_visible_search_input(page)
    if search_input is None:
        save_debug(page, f"no_search_input_{orgnr}")
        raise RuntimeError(f"Kunde inte hitta synligt sökfält på {page.url}")

    try:
        search_input.click(timeout=5000)
    except Exception:
        # fallback om ett overlay stör första klicket
        accept_cookies(page)
        page.wait_for_timeout(1000)
        search_input = find_visible_search_input(page)
        if search_input is None:
            save_debug(page, f"search_input_missing_after_retry_{orgnr}")
            raise RuntimeError(f"Sökfält försvann efter cookie-retry på {page.url}")
        search_input.click(timeout=5000, force=True)

    search_input.fill(orgnr)
    page.wait_for_timeout(800)

    print(f"    Söker på orgnr: {orgnr}")

    # Försök först med Enter
    search_input.press("Enter")
    page.wait_for_timeout(3000)

    # Om cookie-popup kom tillbaka, hantera den igen
    accept_cookies(page)
    page.wait_for_timeout(1000)

    # Om vi fortfarande är kvar på startsidan, klicka på sökikonen också
    if page.url.rstrip("/") == "https://www.ratsit.se":
        try:
            search_button = page.locator("button[type='submit'], button[aria-label*='Sök'], a[aria-label*='Sök']").first
            if search_button.count() > 0 and search_button.is_visible(timeout=2000):
                search_button.click(timeout=3000)
                page.wait_for_timeout(3000)
            else:
                page.locator("button, a").filter(has=page.locator("svg")).first.click(timeout=3000)
                page.wait_for_timeout(3000)
        except Exception:
            pass

    # En sista gång om cookie-popupen dyker upp igen efter sök
    accept_cookies(page)
    page.wait_for_timeout(1000)

def enrich():
    rows = []
    with open(INPUT_CSV, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)

    if MAX_ROWS is not None:
        rows = rows[:MAX_ROWS]

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        page = new_page(browser)

        for i, row in enumerate(rows, start=1):
            orgnr = normalize_orgnr(row.get("orgnr", ""))
            if not orgnr:
                print(f"[{i}] Skippar {row.get('name','')} - saknar orgnr")
                continue

            try:
                print(f"[{i}] Söker {row.get('name','')} ({orgnr})")

                # Om sidan råkat stängas, skapa en ny
                if page.is_closed():
                    page = new_page(browser)

                page.goto("https://www.ratsit.se", wait_until="domcontentloaded")
                page.wait_for_timeout(2500)
                accept_cookies(page)
                page.wait_for_timeout(1500)
                search_orgnr(page, orgnr)

                page.wait_for_timeout(1500)

                result_link = find_result_link(page, orgnr, row.get("name", ""))
                if result_link is None:
                    save_debug(page, f"no_result_link_{i}_{orgnr}")
                    raise RuntimeError(
                        f"Ingen träfflänk hittades efter sök för {orgnr} / {row.get('name', '')} på {page.url}"
                    )

                result_link.wait_for(state="visible", timeout=10000)

                print(f"    Aktuell URL efter sök: {page.url} | matchar namn: {row.get('name', '')}")
                detail_page = page
                result_href = result_link.get_attribute("href") or ""
                print(f"    Result href: {result_href}")

                if result_href and not result_href.startswith("#"):
                    target_url = result_href if result_href.startswith("http") else f"https://www.ratsit.se{result_href}"

                    # Om länken fortfarande pekar på söksidan är det inte en riktig företagslänk
                    if "/sok/foretag" in target_url:
                        save_debug(page, f"bad_result_href_{i}_{orgnr}")
                        raise RuntimeError(
                            f"Träfflänken för {orgnr} / {row.get('name', '')} pekade fortfarande på söksidan istället för företagsprofil. href={result_href!r}"
                        )

                    page.goto(target_url, wait_until="domcontentloaded")
                    detail_page = page
                else:
                    save_debug(page, f"bad_result_href_{i}_{orgnr}")
                    raise RuntimeError(
                        f"Träfflänken för {orgnr} / {row.get('name', '')} var inte en riktig företagslänk. href={result_href!r}"
                    )

                detail_page.wait_for_timeout(2000)
                print(f"    Detaljsida URL: {detail_page.url}")
                data = parse_company_page(detail_page)

                row["registrerad_datum"] = data["registrerad_datum"]
                row["arbetsgivare"] = data["arbetsgivare"]
                row["f_skatt"] = data["f_skatt"]
                row["moms"] = data["moms"]
                row["sni"] = data["sni"]
                row["bolagsform"] = data["bolagsform"]
                row["status"] = data["status"]
                row["datakalla"] = "Ratsit"

                if detail_page is not page and not detail_page.is_closed():
                    detail_page.close()
                    page.wait_for_timeout(1000)

                print(
                    f"    registrerad={row['registrerad_datum']} | "
                    f"arbetsgivare={row['arbetsgivare']} | "
                    f"f_skatt={row['f_skatt']} | moms={row['moms']} | "
                    f"sni={row['sni']} | bolagsform={row['bolagsform']}"
                )

                time.sleep(2.5)

            except PlaywrightTimeoutError:
                print(f"[{i}] Timeout för {row.get('name','')} på URL: {page.url if not page.is_closed() else 'stängd sida'}")
                if not page.is_closed():
                    save_debug(page, f"timeout_{i}_{orgnr}")
                if page.is_closed():
                    page = new_page(browser)
            except Exception as e:
                print(f"[{i}] Fel för {row.get('name','')}: {e} | URL: {page.url if not page.is_closed() else 'stängd sida'}")
                if not page.is_closed():
                    save_debug(page, f"error_{i}_{orgnr}")
                if page.is_closed():
                    page = new_page(browser)

        browser.close()

    fieldnames = list(rows[0].keys()) if rows else []
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    print(f"\nKlart: {OUTPUT_CSV}")

if __name__ == "__main__":
    enrich()