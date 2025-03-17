import os
import time
import re
import gzip
import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
from selenium import webdriver
from selenium.webdriver import Firefox
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    NoSuchElementException,
    StaleElementReferenceException,
    TimeoutException,
    ElementClickInterceptedException
)

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")

class FormHandler:
    def __init__(self, url: str):
        self.options = Options()
        self.options.add_argument('-headless')
        self.driver = Firefox(options=self.options)
        self.driver.get(url)
        self.wait = WebDriverWait(self.driver, 20)
        
        # Map form elements to their IDs.
        self.form_elements = {
            'state': 'ctl00_ContentPlaceHolder1_ddlstate',
            'district': 'ctl00_ContentPlaceHolder1_ddldistrict',
            'block': 'ctl00_ContentPlaceHolder1_ddlBlock',
            'panchayat': 'ctl00_ContentPlaceHolder1_ddlPanchayat',
            'year': 'ctl00_ContentPlaceHolder1_ddlAuditYear',
            'date': 'ctl00_ContentPlaceHolder1_ddlGSDate',
            'option': 'ctl00_ContentPlaceHolder1_ddlselect'
        }
        
        self.html_dir = Path('../data/html')

        self.html_dir.mkdir(exist_ok=True)
        
        self.results = []
        self.total_processed = 0
        self.save_interval = 10  # (Optional) If you still want to save intermediate results to CSV
        # Remove CSV dependency: do not load from CSV; instead, build processed_set from HTML files.
        self.processed_set = self._load_processed_set()
    
    def clean_value(self, val: str) -> str:
        """Return the cleaned version of val matching _save_webpage() formatting."""
        return "".join(c if c.isalnum() or c in ['_', '-', '.'] else "_" for c in val.strip())
    
    def _load_processed_set(self) -> set:
        """
        Reconstruct the processed combination keys from saved HTML files in `self.html_dir` and `self.html_dir_old`.
        This ensures that if a file with these values exists in either directory, we do not re-download.
        """
        processed = set()

        # List of directories to check
        directories = [self.html_dir]

        for directory in directories:
            if not directory.exists():
                logging.warning(f"HTML directory {directory} does not exist. Skipping.")
                continue

            # List all saved HTML files (.html and .html.gz)
            html_files = list(directory.glob("*.html")) + list(directory.glob("*.html.gz"))
            logging.info(f"[DEBUG] Found {len(html_files)} HTML files in {directory}.")

            for file in html_files:
                filename = file.stem  # Remove extension (e.g., '.html.gz')
                logging.debug(f"[DEBUG] Processing filename: {filename}")
                try:
                    # Separate out the option field by splitting on the last underscore.
                    base, _ = filename.rsplit("_", 1)
                except ValueError:
                    logging.warning(f"[WARNING] Skipping malformed filename (cannot rsplit): {filename}")
                    continue

                # Split the remaining base into exactly six parts.
                parts = base.split("_", 5)
                if len(parts) != 6:
                    logging.warning(f"[WARNING] Skipping malformed filename (expected 6 parts, got {len(parts)}): {filename}")
                    continue

                state_val, district_val, block_val, panchayat_val, year_val, date_val = parts

                # Construct key from the first 6 cleaned fields.
                key = (
                    self.clean_value(state_val),
                    self.clean_value(district_val),
                    self.clean_value(block_val),
                    self.clean_value(panchayat_val),
                    self.clean_value(year_val),
                    self.clean_value(date_val)
                )
                processed.add(key)

        logging.info(f"✅ Loaded {len(processed)} processed combinations from both directories.")
        return processed

    def _wait_for_enabled(self, element_id: str) -> Select:
        """Wait for a dropdown element to be clickable and return a Select object."""
        element = self.wait.until(EC.element_to_be_clickable((By.ID, element_id)))
        return Select(element)

    def _get_options_dict(self, element, exclude_default=True, max_wait=10):
        """
        Wait for the dropdown element to be present and then return its options as a dictionary.
        If 'element' is a string, treat it as the element's ID and locate it.
        Excludes options with value "0" if exclude_default is True.
        """
        options_dict = {}
        try:
            wait = WebDriverWait(self.driver, max_wait)
            if isinstance(element, str):
                element_id = element
                wait.until(EC.presence_of_element_located((By.ID, element_id)))
                element = self.driver.find_element(By.ID, element_id)
            else:
                element_id = element.get_attribute("id")
                wait.until(EC.presence_of_element_located((By.ID, element_id)))
            
            options = element.find_elements(By.TAG_NAME, "option")
            for option in options:
                text = option.text.strip()
                value = option.get_attribute("value").strip()
                if exclude_default and (not value or value.lower() in ["", "select", "choose", "0"]):
                    continue
                options_dict[value] = text

            if not options_dict:
                raise Exception(f"No options found for element {element_id}")
        except Exception as e:
            logging.error("Error getting options for element %s: %s", element_id, e)
            raise
        return options_dict
    
    def _select_option(self, element_id: str, value: str, max_retries=1) -> bool:
        """
        Select an option in the dropdown by its value.
        Returns True on success; False if all retries fail.
        """
        for attempt in range(max_retries):
            try:
                self.wait.until(EC.invisibility_of_element_located((By.CSS_SELECTOR, "div.wrapper")))
                select = self._wait_for_enabled(element_id)
                select.select_by_value(value)
                time.sleep(2)
                return True
            except (TimeoutException, ElementClickInterceptedException) as e:
                logging.warning("Error selecting option %s for element %s: %s. Attempt %d of %d.",
                                value, element_id, e, attempt+1, max_retries)
                try:
                    element = self.driver.find_element(By.ID, element_id)
                    self.driver.execute_script("arguments[0].scrollIntoView(true);", element)
                    self.driver.execute_script("arguments[0].click();", element)
                except Exception as inner_e:
                    logging.warning("Could not scroll or click element %s via JS: %s", element_id, inner_e)
                time.sleep(2)
            except Exception as e:
                if attempt == max_retries - 1:
                    logging.error("Failed selecting option %s for element %s after %d attempts: %s",
                                  value, element_id, max_retries, e)
                    return False
                logging.warning("Error selecting option %s for element %s: %s. Retrying...", value, element_id, e)
                time.sleep(2)
        return False

    def _save_webpage(self, row: dict) -> str:
        """Save the current page HTML to disk as gzip and return the filename."""
        # Build filename with all 7 fields so that it's unique.
        filename = (
            f"{row['state_val']}_{row['district_val']}_{row['block_val']}_"
            f"{row['panchayat_val']}_{row['year_val']}_{row['date_val']}_{row['option_val']}.html.gz"
        )
        filename = "".join(c if c.isalnum() or c in ['_', '-', '.'] else "_" for c in filename)
        filepath = self.html_dir / filename
        try:
            html_content = self.driver.page_source.encode('utf-8')
            with gzip.open(filepath, 'wb') as f:
                f.write(html_content)
            return str(filepath)
        except Exception as e:
            logging.error("Error saving compressed HTML: %s", e)
            return ""
        
    def iterate_form(self) -> pd.DataFrame:
        """
        Iterate over all nested dropdown combinations and collect a long-form DataFrame.
        Resumes intelligently based on user-supplied resume parameters.
        """
        # (Assume self.resume_manual may have been set via set_resume_parameters)
        try:
            state_dict = self._get_options_dict(self.form_elements['state'])
        except Exception as e:
            logging.error("Unable to get state options: %s", e)
            return pd.DataFrame(self.results)
        
        for state_val, state_label in state_dict.items():
            if "meghalaya" not in state_label.lower():
                continue
            if not self._select_option(self.form_elements['state'], state_val):
                logging.warning("Skipping state %s due to selection failure.", state_val)
                continue
            
            try:
                district_dict = self._get_options_dict(self.form_elements['district'])
            except Exception as e:
                logging.error("Unable to get district options for state %s: %s", state_val, e)
                continue
            
            for district_val, district_label in district_dict.items():
                        
                if not self._select_option(self.form_elements['district'], district_val):
                    logging.warning("Skipping district %s.", district_val)
                    continue
                
                try:
                    block_dict = self._get_options_dict(self.form_elements['block'])
                except Exception as e:
                    logging.error("Unable to get block options for district %s: %s", district_val, e)
                    continue
                
                for block_val, block_label in block_dict.items():
                    
                    if not self._select_option(self.form_elements['block'], block_val):
                        logging.warning("Skipping block %s.", block_val)
                        continue
                    
                    try:
                        panchayat_dict = self._get_options_dict(self.form_elements['panchayat'])
                    except Exception as e:
                        logging.error("Unable to get panchayat options for block %s: %s", block_val, e)
                        continue
                    
                    for panchayat_val, panchayat_label in panchayat_dict.items():
                        
                        if not self._select_option(self.form_elements['panchayat'], panchayat_val):
                            logging.warning("Skipping panchayat %s.", panchayat_val)
                            continue
                        
                        try:
                            year_dict = self._get_options_dict(self.form_elements['year'])
                        except Exception as e:
                            logging.error("Unable to get year options for panchayat %s: %s", panchayat_val, e)
                            continue
                        
                        for year_val, year_label in year_dict.items():
                            if not self._select_option(self.form_elements['year'], year_val):
                                logging.warning("Skipping year %s.", year_val)
                                continue
                            
                            try:
                                date_dict = self._get_options_dict(self.form_elements['date'])
                            except Exception as e:
                                logging.error("Unable to get date options for year %s: %s", year_val, e)
                                continue
                            
                            for date_val, date_label in date_dict.items():
                                if not self._select_option(self.form_elements['date'], date_val):
                                    logging.warning("Skipping date %s.", date_val)
                                    continue
                                
                                try:
                                    option_dict = self._get_options_dict(self.form_elements['option'], exclude_default=False)
                                except Exception as e:
                                    logging.error("Unable to get option values: %s", e)
                                    continue
                                
                                option_val, option_label = None, None
                                for k, v in option_dict.items():
                                    if v.lower() == "all":
                                        option_val, option_label = k, v
                                        break
                                if option_val is None:
                                    if option_dict:
                                        option_val, option_label = list(option_dict.items())[0]
                                    else:
                                        logging.warning("No option found for the 'option' dropdown; skipping combination.")
                                        continue
                                
                                # Construct key from the first 6 fields (excluding the option)
                                key = (
                                    self.clean_value(state_val),
                                    self.clean_value(district_val),
                                    self.clean_value(block_val),
                                    self.clean_value(panchayat_val),
                                    self.clean_value(year_val),
                                    self.clean_value(date_val)
                                )
                                if key in self.processed_set:
                                    logging.info("Combination already processed, skipping: %s", key)
                                    continue
                                
                                max_attempts = 1
                                attempts = 0
                                success = False
                                while attempts < max_attempts:
                                    if not self._select_option(self.form_elements['option'], option_val):
                                        logging.warning("Failed to select final option %s on attempt %d.", option_val, attempts+1)
                                        attempts += 1
                                        self.driver.refresh()
                                        time.sleep(3)
                                        try:
                                            self._select_option(self.form_elements['state'], state_val)
                                            self._select_option(self.form_elements['district'], district_val)
                                            self._select_option(self.form_elements['block'], block_val)
                                            self._select_option(self.form_elements['panchayat'], panchayat_val)
                                            self._select_option(self.form_elements['year'], year_val)
                                            self._select_option(self.form_elements['date'], date_val)
                                        except Exception as e2:
                                            logging.error("Error re-selecting after refresh: %s", e2)
                                        continue
                                    else:
                                        success = True
                                        break
                                
                                if not success:
                                    logging.error("Maximum attempts reached for combination %s. Skipping.", key)
                                    continue
                                
                                html_file = self._save_webpage({
                                    'state_val': state_val,
                                    'district_val': district_val,
                                    'block_val': block_val,
                                    'panchayat_val': panchayat_val,
                                    'year_val': year_val,
                                    'date_val': date_val,
                                    'option_val': option_val
                                })
                                row = {
                                    'state_val': state_val,
                                    'state_label': state_label,
                                    'district_val': district_val,
                                    'district_label': district_label,
                                    'block_val': block_val,
                                    'block_label': block_label,
                                    'panchayat_val': panchayat_val,
                                    'panchayat_label': panchayat_label,
                                    'year_val': year_val,
                                    'year_label': year_label,
                                    'date_val': date_val,
                                    'date_label': date_label,
                                    'option_val': option_val,
                                    'option_label': option_label,
                                    'html_file': html_file,
                                    'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                }
                                self.results.append(row)
                                self.total_processed += 1
                                # Add the key (first 6 fields) to processed_set.
                                self.processed_set.add(key)
                                
                                logging.info(
                                    "[%d] Processed: State: %s (%s), District: %s (%s), Block: %s (%s), "
                                    "Panchayat: %s (%s), Year: %s (%s), Date: %s (%s), Option: %s (%s). "
                                    "HTML saved to %s",
                                    self.total_processed, state_label, state_val,
                                    district_label, district_val, block_label, block_val,
                                    panchayat_label, panchayat_val, year_label, year_val,
                                    date_label, date_val, option_label, option_val, html_file
                                )
                                break
        return

    def cleanup(self):
        self.driver.quit()

url = "https://mnregaweb4.nic.in/netnrega/SocialAuditFindings/SA-GPReport.aspx?page=S&lflag=eng"
handler = FormHandler(url)
try:
    df = handler.iterate_form()
finally:
    handler.cleanup()