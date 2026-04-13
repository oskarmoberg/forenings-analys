from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
import csv
import re
import time

URL = "https://jonkoping.actorsmartbook.se/Associations.aspx"
OUTPUT_CSV = "jonkoping_foreningar.csv"
MAX_PAGES = None  # sätt till None för att köra alla sidor


def safe_text(locator):
    try:
        text = locator.inner_text(timeout=3000).strip()
        return text
    except Exception:
        return ""


def get_info_buttons(page):
    # Begränsa till faktiska knappar i tabellen, inte dolda labels eller annat skräp i DOM:en
    return page.locator("button:has-text('Info')")


def extract_modal_data(page):
    modal = page.locator("div.modal-content").filter(has=page.locator("text=Föreningsinformation")).last
    modal.wait_for(state="visible", timeout=10000)

    # Titel (föreningsnamn)
    title = safe_text(modal.locator("h3").first) or safe_text(modal.locator("h4").first)

    modal_text = modal.inner_text().replace("\r\n", "\n")

    def extract_between(start_label, end_labels=None):
        end_labels = end_labels or []
        escaped_start = re.escape(start_label)
        if end_labels:
            escaped_ends = "|".join(re.escape(label) for label in end_labels)
            pattern = rf"{escaped_start}:\s*(.*?)\s*(?=(?:{escaped_ends}):|$)"
        else:
            pattern = rf"{escaped_start}:\s*(.*?)\s*$"

        match = re.search(pattern, modal_text, flags=re.DOTALL)
        if not match:
            return ""

        value = match.group(1)
        value = value.replace("\n", " ")
        value = re.sub(r"\s+", " ", value).strip()
        return value

    orgnr = extract_between("Org.nr", ["Epost", "Hemsida", "Ort", "Kontaktpersoner"])
    email = extract_between("Epost", ["Hemsida", "Ort", "Kontaktpersoner"])
    website = extract_between("Hemsida", ["Ort", "Kontaktpersoner"])
    city = extract_between("Ort", ["Kontaktpersoner"])

    return {
        "name": title,
        "orgnr": orgnr,
        "email": email,
        "website": website,
        "city": city,
    }


def get_total_pages(page):
    # Försöker läsa "Sidan 1/69"
    body_text = page.locator("body").inner_text()
    for line in body_text.splitlines():
        line = line.strip()
        if line.startswith("Sidan ") and "/" in line:
            try:
                after = line.replace("Sidan ", "")
                current, total = after.split("/")
                return int(total.strip())
            except Exception:
                pass
    return None


def scrape():
    all_rows = []
    seen = set()  # för att undvika dubletter

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)  # sätt True när det funkar
        page = browser.new_page()
        page.goto(URL, wait_until="domcontentloaded")

        # Hantera cookie-banner (om den dyker upp)
        try:
            cookie_button = page.locator("text=Godkänn alla cookies")
            if cookie_button.is_visible(timeout=3000):
                cookie_button.click()
                page.wait_for_timeout(1000)
        except Exception:
            pass

        # Vänta på den synliga rubriken i innehållet, inte en dold menylänk
        page.locator("h1", has_text="Föreningskatalog").wait_for(state="visible", timeout=15000)

        # Vänta sedan tills minst en faktisk synlig Info-knapp finns i listan
        get_info_buttons(page).first.wait_for(state="visible", timeout=15000)

        total_pages = get_total_pages(page)
        if not total_pages:
            print("Kunde inte läsa antal sidor. Kör tills 'Nästa' inte går.")
            total_pages = 9999

        if MAX_PAGES is not None:
            total_pages = min(total_pages, MAX_PAGES)
            print(f"Testkörning: kommer bara köra {total_pages} sidor.")

        for page_num in range(1, total_pages + 1):
            print(f"Skrapar sida {page_num}...")

            # Vänta in faktiska synliga Info-knappar på aktuell sida
            get_info_buttons(page).first.wait_for(state="visible", timeout=10000)

            info_buttons = get_info_buttons(page)
            count = info_buttons.count()

            # Vi loopar över knapparna på aktuell sida
            for i in range(count):
                try:
                    btn = get_info_buttons(page).nth(i)
                    btn.scroll_into_view_if_needed()
                    btn.click(timeout=5000)

                    data = extract_modal_data(page)

                    # skapa unik nyckel (orgnr om finns, annars namn)
                    key = data["orgnr"] if data["orgnr"] else data["name"]

                    if key not in seen:
                        seen.add(key)
                        all_rows.append(data)
                        print(f"  Hämtade: {data['name']}")
                    else:
                        print(f"  Skippar dubblett: {data['name']}")

                    # Stäng modal
                    close_btn = page.locator("div.modal-content >> text=×").last
                    if close_btn.is_visible():
                        close_btn.click()
                    else:
                        # fallback: tryck Escape
                        page.keyboard.press("Escape")

                    page.wait_for_timeout(500)

                except PlaywrightTimeoutError:
                    print(f"  Timeout på rad {i+1} på sida {page_num}, hoppar vidare.")
                    try:
                        page.keyboard.press("Escape")
                    except Exception:
                        pass
                except Exception as e:
                    print(f"  Fel på rad {i+1} på sida {page_num}: {e}")
                    try:
                        page.keyboard.press("Escape")
                    except Exception:
                        pass

            # Försök gå till nästa sida
            next_button = page.locator("text=Nästa").first
            if next_button.count() > 0 and next_button.is_visible():
                classes = next_button.get_attribute("class") or ""
                aria_disabled = next_button.get_attribute("aria-disabled") or ""

                # Om knappen är disabled eller vi är på sista sidan, bryt
                if "disabled" in classes.lower() or aria_disabled == "true" or page_num >= total_pages:
                    break

                next_button.click()
                page.wait_for_timeout(1500)
                get_info_buttons(page).first.wait_for(state="visible", timeout=10000)
            else:
                break

        browser.close()

    # Skriv CSV
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["name", "orgnr", "email", "website", "city"],
            extrasaction="ignore"
        )
        writer.writeheader()
        writer.writerows(all_rows)

    print(f"\nKlart. Sparade {len(all_rows)} föreningar till {OUTPUT_CSV} (MAX_PAGES={MAX_PAGES})")


if __name__ == "__main__":
    scrape()