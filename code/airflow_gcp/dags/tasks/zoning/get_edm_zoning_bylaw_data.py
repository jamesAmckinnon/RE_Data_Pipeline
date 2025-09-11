def get_edm_zoning_bylaw_data():
    from bs4 import BeautifulSoup
    import requests
    import re
    import json
    import pandas as pd
    from sqlalchemy import create_engine, Column, Integer, Float, String
    from sqlalchemy.dialects.postgresql import JSONB
    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy.dialects.postgresql import insert
    import requests
    from airflow.hooks.base import BaseHook
    import traceback


    BASE_URL = "https://zoningbylaw.edmonton.ca"

    def get_db_engine():
        conn = BaseHook.get_connection("supabase_db_TP_IPv4") 
        engine = create_engine(
            f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}'
        )
        return engine
    
    def safe_cast(val, to_type, default):
        try:
            if val is None or pd.isna(val) or val in ["NaN", "nan"]:
                return default
            if to_type is str:
                return str(val)
            return to_type(val)
        except (ValueError, TypeError):
            return default

    Base = declarative_base()

    class ZoningBylaws(Base):
        __tablename__ = 'zoning_bylaws'
        
        zoning_bylaw_id = Column(Integer, primary_key=True)
        zone_name = Column(String)
        zone_code = Column(String)
        section = Column(String)
        purpose = Column(String)
        bylaw_url = Column(String)
        permitted_uses = Column(JSONB)
        regulations = Column(JSONB)
        
        def __repr__(self):
            return f"<ZoningBylaws(zone_name={self.zone_name}, zone_code=${self.zone_code}, section={self.section})>"

    def get_section_key(span_text):
        # Remove number prefixes like "2." from "2. Permitted Uses"
        return re.sub(r"^\d+\.\s*", "", span_text).strip()

    def parse_zone_purpose(accordion):
        content_div = accordion.find("div", class_="node__content")
        if content_div:
            text = content_div.get_text(strip=True, separator=" ")
            t = text.strip()
            
            # Case 1: "up to X or Y storeys" (e.g., "up to 3 or 4 Storeys")
            match = re.search(r'up\s*to\s*(\d+)\s*or\s*(\d+)\s*storeys?', t, re.IGNORECASE)
            if match:
                return text, f"up to {match.group(1)} or {match.group(2)}"
                
            # Case 2: "up to X storeys" (e.g., "up to 3 Storeys")
            match = re.search(r'up\s*to\s*(\d+)\s*storeys?', t, re.IGNORECASE)
            if match:
                return text, f"up to {match.group(1)}"
                
            # Case 3: range with "to" (e.g., "4 to 8 Storeys") - must have digits before and after "to"
            match = re.search(r'(\d+)\s*to\s*(\d+)\s*storeys?', t, re.IGNORECASE)
            if match:
                return text, f"{match.group(1)} to {match.group(2)}"
                
            # Case 4: alternative with "or" (e.g., "3 or 4 Storeys")
            match = re.search(r'(\d+)\s*or\s*(\d+)\s*storeys?', t, re.IGNORECASE)
            if match:
                return text, f"{match.group(1)} or {match.group(2)}"
                
            # Case 4: single number (e.g., "3 Storeys")
            match = re.search(r'(\d+)\s*storeys?', t, re.IGNORECASE)
            if match:
                return text, match.group(1)
                
            # No mention of storeys
            return text, None
        else:
            return None, None


    def parse_permitted_uses(accordion):
        permitted = {}
        current_category = None
        current_main_use = None

        for row in accordion.select("table > tbody > tr"):
            # Detect category headers (h5 inside td)
            h5 = row.find("h5")
            if h5:
                current_category = h5.get_text(strip=True).replace("#", "").strip()
                permitted[current_category] = {}
                current_main_use = None
                continue

            # Skip rows without a current category
            if not current_category:
                continue

            # Look for main definition link
            link = row.find("a", class_="definition")
            cell_text = row.get_text(" ", strip=True)

            # If there is a nested table, it’s “limited to” the last main use
            nested_table = row.find("table")
            if nested_table and current_main_use:
                limited_list = []
                for nested_row in nested_table.select("tr"):
                    nested_link = nested_row.find("a", class_="definition")
                    if nested_link:
                        limited_list.append({
                            "name": nested_link.get_text(strip=True),
                            "url": BASE_URL + nested_link["href"]
                        })
                permitted[current_category][current_main_use]["limited to"] = limited_list
                continue

            # Otherwise treat as a main permitted use
            if link:
                main_use_name = link.get_text(strip=True)
                permitted[current_category][main_use_name] = {
                    "url": BASE_URL + link["href"],
                    "limited to": None
                }
                current_main_use = main_use_name
            else:
                # If row has text but no link, still store as a main use with no URL
                text_only = cell_text.strip()
                if text_only:
                    permitted[current_category][text_only] = {
                        "url": None,
                        "limited to": None
                    }
                    current_main_use = text_only

        return permitted


    def parse_site_and_building_regulations(accordion, zone_code):
        zones = {}

        tables = accordion.select("table")
        if not tables:
            return zones


        variables_list = [ "Maximum Height", "Floor Area Ratio", "Density", "Site Coverage" ]
        modified_zone_vars = []
        regulation_dict = {}

        for table in tables:
            thead = table.find("thead")
            tbody = table.find("tbody")

            if thead:
                header_text = thead.get_text(" ", strip=True)
                variables_in_header = any([variable in header_text for variable in variables_list])
                first_row = tbody.find_all("tr")[0]
                modifier_col_in_first_row = first_row.find_all("td")[1].get_text(" ", strip=True)

                if variables_in_header and modifier_col_in_first_row == "Modifier on Zoning Map":
                    first_row = tbody.find_all("tr")[0]
                    table_variables = [
                        v.get_text(" ", strip=True) for v in first_row.find_all("td") 
                        if v.get_text(" ", strip=True) != "Subsection" 
                    ]
                    
                    for table_row in tbody.find_all("tr")[1:]:
                        row_cols = table_row.find_all("td")
                        temp_dict = {}

                        for i, row_col in enumerate(row_cols[1:]):
                            relevant_variable = [variable for variable in variables_list if variable in table_variables[i]]

                            if len(relevant_variable) == 1:
                                temp_dict[table_variables[i]] = row_col.get_text(" ", strip=True)

                        temp_dict["Modifier on Zoning Map"] = row_cols[1].get_text(" ", strip=True)
                        modified_zone_vars.append(temp_dict)

                else:
                    first_row = tbody.find_all("tr")[0]
                    table_cols = [ 
                        v.get_text(" ", strip=True) for v in first_row.find_all("td")
                        if v.get_text(" ", strip=True) != "Subsection" 
                    ]
                    
                    if "Value" in table_cols and "Regulation" in table_cols:
                        regulation_col_idx = table_cols.index("Regulation")
                        value_col_idx      = table_cols.index("Value")

                        for table_row in tbody.find_all("tr")[1:]:
                            tds = table_row.find_all("td")[1:]
                            
                            if len(tds) > 1:
                                regulation_text = tds[regulation_col_idx].get_text(" ", strip=True)
                                variable_in_row = [variable for variable in variables_list if variable in regulation_text]

                                if len(variable_in_row):
                                    regulation_dict[regulation_text] = tds[value_col_idx].get_text(" ", strip=True)


        if modified_zone_vars:
            for mod in modified_zone_vars:
                modifier_code = mod["Modifier on Zoning Map"]
                mod_copy = mod.copy()
                del mod_copy["Modifier on Zoning Map"]
                
                merged = {**mod_copy, **regulation_dict}
                zones[f"{zone_code} {modifier_code}"] = merged
        else:
            zones[zone_code] = regulation_dict

        return zones


    def parse_zone_title(full_title):
        # Match: section (digits + dot + digits), zone code (letters/numbers), then zone name
        m = re.match(r"^\s*(\d+(?:\.\d+)?)\s+([A-Z0-9.]+)\s*-\s*(.+)$", full_title)
        if m:
            section = m.group(1)       # "3.16"
            zone_code = m.group(2)     # "A6"
            zone_name = m.group(3)     # "River Crossing Zone"
            return section, zone_code, zone_name
        else:
            # fallback: return whole thing as zone_name if parsing fails
            return None, full_title, full_title
        

    def scrape_zone_page(url):
        response = requests.get(url)
        soup = BeautifulSoup(response.text, "html.parser")

        content = soup.select_one(".page-container__content")
        if not content:
            return {}

        # Zone name is in the <h1 class="page-title">
        title_tag = content.select_one("h1.page-title span")
        full_title = title_tag.get_text(strip=True) if title_tag else url

        section, zone_code, zone_name = parse_zone_title(full_title)

        temp_data = {
            "Section": section,
            "Zone Name": zone_name,
            "url": url
        }
        output_data = {}

        # Parse sections
        for accordion in content.select("div.accordion"):
            button = accordion.find("button", class_="accordion__btn")
            if not button:
                continue

            span = button.find("span", class_="acc-text")
            if not span:
                continue

            section_key = get_section_key(span.get_text(strip=True))

            if section_key == "Purpose":
                text, num_storeys = parse_zone_purpose(accordion)
                if text:
                    temp_data["purpose"] = text
                if num_storeys:
                    temp_data["_num_storeys"] = num_storeys  

            elif section_key == "Permitted Uses":
                temp_data["permitted_uses"] = parse_permitted_uses(accordion)

            elif section_key == "Site and Building Regulations":
                zones = parse_site_and_building_regulations(accordion, zone_code)

                for zone_code, zone_vars in zones.items():
                    # Inject num_storeys into regulations if it exists
                    if temp_data.get("_num_storeys"):
                        zone_vars["Number of Storeys"] = temp_data["_num_storeys"]
                        del temp_data["_num_storeys"]

                    output_data[zone_code] = {
                        **temp_data,               # purpose & permitted_uses
                        "regulations": zone_vars   # regulations + num_storeys
                    }
            
        return output_data

    def scrape_all_zones(menu_url):
        response = requests.get(menu_url)
        soup = BeautifulSoup(response.text, "html.parser")

        menu_container = soup.select_one("div.menu-container")
        zones_data = {}
        url_count = 0

        stop = False
        for li in menu_container.select("ul.menu__primary_main li"):
            lis = li.find_all("a", href=True)
            for a_tag in lis:
                text = a_tag.get_text(strip=True)
                href = a_tag["href"]
                full_url = BASE_URL + href

                if "Part 4" in text: # Only consider parts 2 and 3
                    stop = True
                    break

                if not href or href.startswith("#") or "part-1" in href:
                    continue

                if (url_count % 10) == 0:
                    print(f"Getting zoning data from link {url_count}...")

                url_count += 1
                zone_data = scrape_zone_page(full_url)

                if zone_data:
                    zones_data.update(zone_data)

            if stop:
                break

        return zones_data



    menu_url = BASE_URL + "/part-2-standard-zones-and-overlays"
    all_zones = scrape_all_zones(menu_url)

    try:
        engine = get_db_engine()
        # Create tables if they don't exist
        Base.metadata.create_all(engine)
        
        # Create a session to interact with the database
        Session = sessionmaker(bind=engine)
        session = Session()

        num_zones = len(all_zones.items())

        db_zone_bylaws = []
        for i, (zone_code, zone) in enumerate(all_zones.items()):
            if i % 10 == 0:
                print(f"Adding permit {i} of {num_zones}")

            db_zone_bylaws.append({
                "zone_name": safe_cast(zone.get('Zone Name'), str, 'Unknown'),
                "zone_code": safe_cast(zone_code, str, 'Unknown'),
                "section": safe_cast(zone.get('Section'), str, 'Unknown'),
                "purpose": safe_cast(zone.get('purpose'), str, 'Unknown'),
                "bylaw_url": safe_cast(zone.get('url'), str, 'Unknown'),
                "permitted_uses": zone.get('permitted_uses'),
                "regulations": zone.get('regulations')
            })

        # Bulk insert all zoning bylaw records at once
        stmt = insert(ZoningBylaws)
        result = session.execute(stmt, db_zone_bylaws)
        session.commit()
        print(f"Successfully saved {result.rowcount} zoning bylaw records to the database")

    except Exception as e:
        if 'session' in locals():
            session.rollback()
        print(f"Error saving to database: {traceback.print_exc()}")
    
    finally:
        if 'session' in locals():
            session.close()
    
    return