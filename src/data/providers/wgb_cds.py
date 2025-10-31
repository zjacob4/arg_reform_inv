import time
import random
import re
from datetime import datetime
from typing import List, Tuple, Optional
from io import StringIO

import pandas as pd
import requests
from bs4 import BeautifulSoup
from .base import SeriesProvider, ProviderError

WGB_HIST_URL = "https://www.worldgovernmentbonds.com/cds-historical-data/argentina/5-years/"
WGB_SOVEREIGN_HUB = "https://www.worldgovernmentbonds.com/sovereign-cds/"
UA = "Mozilla/5.0 (compatible; arg-reform-inv/1.0; +https://example.org)"

class WGBCDSProviderError(RuntimeError):
    ...

def _polite_get(url: str, tries: int = 3, base_sleep: float = 0.6) -> str:
    last = None
    headers = {
        "User-Agent": UA,
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
    }
    for i in range(tries):
        try:
            r = requests.get(url, timeout=30, headers=headers)
            if r.status_code != 200:
                raise WGBCDSProviderError(f"HTTP {r.status_code}")
            text = r.text
            if "<title>Just a moment" in text or "Please enable JavaScript" in text:
                raise WGBCDSProviderError("Intermittent JS/CF challenge")
            return text
        except Exception as e:
            last = e
            if i == tries - 1:
                raise WGBCDSProviderError(f"GET failed: {e}")
            time.sleep(base_sleep * (2**i) + random.random() * 0.25)
    raise WGBCDSProviderError(str(last))

def _to_float_bps(x) -> Optional[float]:
    if x is None:
        return None
    s = str(x).strip()
    if s in ("", "-", "—"):
        return None
    s = s.replace(",", "")
    try:
        return float(s)
    except Exception:
        # try to extract numbers like "1,785.10 bps"
        m = re.search(r"([-+]?\d[\d,]*\.?\d*)", s)
        if m:
            try:
                return float(m.group(1).replace(",", ""))
            except Exception:
                return None
        return None

def fetch_history_argentina_cds_5y_wgb(start: Optional[str] = None, end: Optional[str] = None) -> List[Tuple[datetime, float]]:
    """
    Preferred when the page serves a static table (sometimes it doesn't).
    Returns list of (ts, bps). Falls back to the sovereign hub page if needed.
    """
    out: List[Tuple[datetime, float]] = []

    # Try the historical page first
    try:
        html = _polite_get(WGB_HIST_URL)
        try:
            tables = pd.read_html(StringIO(html))
        except ValueError:
            tables = []
        if tables:
            # Pick a table with a Date column
            df = None
            for t in tables:
                if any("date" in str(c).lower() for c in t.columns) and t.shape[1] >= 2:
                    df = t.copy()
                    break
            if df is not None:
                # Normalize and parse
                date_col = next(c for c in df.columns if "date" in str(c).lower())
                value_col = next((c for c in df.columns if c != date_col), None)
                if value_col is not None:
                    for _, row in df.iterrows():
                        ds = str(row[date_col]).strip()
                        try:
                            dt = pd.to_datetime(ds).to_pydatetime()
                        except Exception:
                            continue
                        v = _to_float_bps(row[value_col])
                        if v is None:
                            continue
                        out.append((dt, v))
    except Exception:
        # ignore and try hub
        pass

    # If still empty, try the sovereign hub page (sometimes has a big country table)
    if not out:
        try:
            hub = _polite_get(WGB_SOVEREIGN_HUB)
            try:
                tables = pd.read_html(StringIO(hub))
            except ValueError:
                tables = []
            if tables:
                # Find a table that lists countries and 5Y CDS
                chosen = None
                for t in tables:
                    cols = [str(c).lower() for c in t.columns]
                    if any("country" in c for c in cols) and any("5" in c and "cds" in c for c in cols):
                        chosen = t
                        break
                if chosen is None:
                    # fallback: widest table
                    chosen = max(tables, key=lambda df: df.shape[0])
                # filter Argentina rows
                cols = [str(c).lower() for c in chosen.columns]
                chosen.columns = cols
                # guess value column
                cand_value_cols = [c for c in cols if ("5" in c and "cds" in c) or ("last" in c or "value" in c)]
                value_col = cand_value_cols[0] if cand_value_cols else cols[-1]
                country_col = next((c for c in cols if "country" in c), cols[0])
                arg_rows = chosen[chosen[country_col].astype(str).str.contains("argentina", case=False, na=False)]
                # No dates on hub; we’ll treat this as latest only with 'now' ts
                if not arg_rows.empty:
                    v = _to_float_bps(arg_rows.iloc[0][value_col])
                    if v is not None:
                        out.append((datetime.utcnow(), v))
        except Exception:
            pass

    # Sort & filter
    out.sort(key=lambda x: x[0])
    if start:
        out = [r for r in out if r[0].date().isoformat() >= start]
    if end:
        out = [r for r in out if r[0].date().isoformat() <= end]
    return out

def _try_playwright() -> Optional[float]:
    """Try using Playwright to render JavaScript and extract CDS value."""
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            page.goto(WGB_HIST_URL, wait_until="networkidle", timeout=30000)
            # Wait for the value to load (check for data-async-loaded="true" or non-placeholder value)
            try:
                page.wait_for_selector('span.summary-value-number[data-async-loaded="true"]', timeout=10000)
            except Exception:
                # Fallback: wait a bit for JS to execute
                page.wait_for_timeout(3000)
            
            # Extract value using Playwright
            elem = page.query_selector('span.summary-value-number')
            if elem:
                val_text = elem.inner_text().strip()
                if val_text and val_text not in ('----', '---', '--', '-', ''):
                    try:
                        val = float(val_text.replace(",", ""))
                        if 500 <= val <= 10000:
                            browser.close()
                            return val
                    except ValueError:
                        pass
            browser.close()
    except ImportError:
        # Playwright not installed
        pass
    except Exception:
        # Playwright failed, continue with other methods
        pass
    return None

def fetch_latest_argentina_cds_5y_wgb() -> List[Tuple[datetime, float]]:
    """
    Fast path to get today's (latest) CDS even when there's no table.
    Navigates the exact HTML structure: summary-banner -> summary-value -> summary-amount -> summary-value-number
    The HTML contains: <span class="summary-value-number" data-async-variable="jsGlobalResult|result.ultimoValore">1030.95</span>
    
    Note: The page loads values via JavaScript, so if static HTML parsing fails, this will try Playwright.
    """
    html = _polite_get(WGB_HIST_URL)
    soup = BeautifulSoup(html, "html.parser")

    # Quick diagnostic: check if the expected elements exist in the HTML
    has_summary_banner = 'summary-banner' in html
    has_summary_value_number = 'summary-value-number' in html
    has_ultimo_valore = 'ultimoValore' in html or 'ultimovalore' in html
    
    # Strategy 1: Navigate the exact HTML structure path
    # summary-banner -> summary-value -> summary-amount -> summary-value-number
    # Handle classes that might have multiple values (e.g., "summary-value borsaInverso")
    summary_banner = soup.find('div', class_=lambda x: x and 'summary-banner' in x)
    if summary_banner:
        # summary-value might have additional classes like "borsaInverso"
        summary_value = summary_banner.find('div', class_=lambda x: x and 'summary-value' in x)
        if summary_value:
            summary_amount = summary_value.find('span', class_='summary-amount')
            if summary_amount:
                summary_value_number = summary_amount.find('span', class_='summary-value-number')
                if summary_value_number:
                    val_text = summary_value_number.get_text(strip=True)
                    # Handle JavaScript placeholder (---- or similar)
                    if val_text in ('----', '---', '--', '-', ''):
                        # Value hasn't been loaded by JavaScript yet - will try other strategies
                        pass
                    else:
                        try:
                            val = float(val_text.replace(",", ""))
                            if 500 <= val <= 10000:
                                return [(datetime.utcnow(), val)]
                        except ValueError:
                            pass
    
    # Strategy 2: CSS selector for the exact path (multiple variations)
    css_selectors = [
        'div.summary-banner div.summary-value span.summary-amount span.summary-value-number',
        '.summary-banner .summary-value .summary-amount .summary-value-number',
        'span.summary-value-number',  # Simplest - just find it anywhere
    ]
    for selector in css_selectors:
        try:
            elems = soup.select(selector)
            for elem in elems:
                val_text = elem.get_text(strip=True)
                # Skip JavaScript placeholders
                if val_text in ('----', '---', '--', '-', ''):
                    continue
                try:
                    val = float(val_text.replace(",", ""))
                    if 500 <= val <= 10000:
                        return [(datetime.utcnow(), val)]
                except ValueError:
                    continue
        except Exception:
            continue

    # Strategy 3: Look for span with class 'summary-value-number' (simpler fallback)
    # Try both exact match and checking if class contains the string
    summary_elem = soup.find('span', class_='summary-value-number')
    if not summary_elem:
        # Also try with lambda to handle multiple classes
        summary_elem = soup.find('span', class_=lambda x: x and ('summary-value-number' in str(x) if isinstance(x, list) else 'summary-value-number' in x))
    
    if summary_elem:
        val_text = summary_elem.get_text(strip=True)
        try:
            val = float(val_text.replace(",", ""))
            if 500 <= val <= 10000:
                return [(datetime.utcnow(), val)]
        except ValueError:
            pass

    # Strategy 4: Look for span with data-async-variable containing 'ultimoValore'
    all_spans = soup.find_all('span', attrs={'data-async-variable': True})
    for span in all_spans:
        data_attr = span.get('data-async-variable', '')
        if data_attr and 'ultimoValore' in data_attr:
            val_text = span.get_text(strip=True)
            try:
                val = float(val_text.replace(",", ""))
                if 500 <= val <= 10000:
                    return [(datetime.utcnow(), val)]
            except ValueError:
                continue

    # Strategy 3: Search raw HTML for the pattern directly (most reliable - works even if BeautifulSoup misses it)
    # The HTML structure: data-async-variable="jsGlobalResult|result.ultimoValore">1030.95
    # Try multiple patterns with different escaping and whitespace handling
    html_patterns = [
        # Exact match: data-async-variable="jsGlobalResult|result.ultimoValore">number
        r'data-async-variable=["\']jsGlobalResult\|result\.ultimoValore["\'][^>]*>(\d{3,}[\d,]*\.?\d*)',
        # With whitespace tolerance
        r'data-async-variable\s*=\s*["\']jsGlobalResult\|result\.ultimoValore["\'][^>]*>\s*(\d{3,}[\d,]*\.?\d*)',
        # Simpler: just look for ultimoValore followed by closing tag and number
        r'ultimoValore["\'][^>]*>\s*(\d{3,}[\d,]*\.?\d*)',
        # Even simpler: just the number pattern after "ultimoValore"
        r'ultimoValore[^>]*>([0-9]{3,}[0-9,]*\.[0-9]+)',
    ]
    for pattern in html_patterns:
        html_match = re.search(pattern, html, flags=re.IGNORECASE | re.DOTALL)
        if html_match:
            val_str = html_match.group(1).replace(",", "").strip()
            try:
                val = float(val_str)
                if 500 <= val <= 10000:
                    return [(datetime.utcnow(), val)]
            except ValueError:
                continue

    # Strategy 4: Fall back to text search
    text = soup.get_text(separator=" ", strip=True)
    # Pattern: "Argentina 5 Years" ... number (require 3+ digits)
    m = re.search(r"Argentina.*?5\s*Years.*?(\d{3,}[\d,]*\.?\d*)", text, flags=re.IGNORECASE)
    if not m:
        m = re.search(r"CDS.*?Argentina.*?(\d{3,}[\d,]*\.?\d*)", text, flags=re.IGNORECASE)
    
    if m:
        try:
            val = float(m.group(1).replace(",", ""))
            if 500 <= val <= 10000:
                return [(datetime.utcnow(), val)]
        except ValueError:
            pass

    # Strategy 5: Ultimate fallback - find any number in CDS range (1000-1100) near Argentina/CDS keywords
    # This is a very broad search but should catch the value if it exists anywhere in the HTML
    # Look for numbers in reasonable CDS range that appear near relevant keywords
    all_numbers = re.findall(r'\b(\d{4}\.\d+)\b', html)
    # Filter to numbers in reasonable Argentina CDS range (currently ~1000-1100 bps)
    candidates = []
    for num_str in all_numbers:
        try:
            val = float(num_str)
            if 1000 <= val <= 1100:  # Narrow range for current Argentina CDS
                # Check if it appears near Argentina or CDS in the HTML
                num_pos = html.find(num_str)
                if num_pos >= 0:
                    context_start = max(0, num_pos - 500)
                    context_end = min(len(html), num_pos + 500)
                    context = html[context_start:context_end].lower()
                    if 'argentina' in context or 'cds' in context or 'ultimovalore' in context:
                        candidates.append((val, num_pos))
        except ValueError:
            continue
    
    if candidates:
        # Prefer the one closest to "ultimoValore" or "Argentina"
        ultimo_pos = html.lower().find('ultimovalore')
        arg_pos = html.lower().find('argentina')
        if ultimo_pos >= 0 or arg_pos >= 0:
            min_ref_pos = min([p for p in [ultimo_pos, arg_pos] if p >= 0], default=-1)
            if min_ref_pos >= 0:
                candidates.sort(key=lambda x: abs(x[1] - min_ref_pos))
                return [(datetime.utcnow(), candidates[0][0])]
        
        # Otherwise just pick the first (should only be one anyway)
        return [(datetime.utcnow(), candidates[0][0])]

    # Final fallback: Try the hub table
    rows = fetch_history_argentina_cds_5y_wgb()
    if rows:
        return rows[-1:]
    
    # If we got here and the HTML contains the expected markers but no value was found,
    # the page might be JavaScript-rendered. Try one more time with a very simple search.
    if has_summary_banner and has_summary_value_number:
        # Last resort: find ANY span with summary-value-number class, regardless of structure
        all_summary_spans = soup.find_all('span', class_=lambda x: x and 'summary-value-number' in str(x))
        for span in all_summary_spans:
            val_text = span.get_text(strip=True)
            # Skip placeholders
            if val_text in ('----', '---', '--', '-', ''):
                continue
            try:
                val = float(val_text.replace(",", ""))
                if 500 <= val <= 10000:
                    return [(datetime.utcnow(), val)]
            except ValueError:
                continue
    
    # Final attempt: Use Playwright to render JavaScript if available
    playwright_val = _try_playwright()
    if playwright_val is not None:
        return [(datetime.utcnow(), playwright_val)]
    
    return []


class WGBCDSProvider(SeriesProvider):
    """WorldGovernmentBonds CDS provider for Argentina 5Y CDS (USD).
    
    Supports series code "CDS_ARG_5Y_USD" to fetch Argentina 5-year CDS spreads.
    """
    
    def fetch_timeseries(
        self,
        series_code: str,
        start: Optional[str] = None,
        end: Optional[str] = None
    ) -> List[Tuple[datetime, float]]:
        """Fetch CDS time series.
        
        Args:
            series_code: Must be "CDS_ARG_5Y_USD"
            start: Start date in YYYY-MM-DD format
            end: End date in YYYY-MM-DD format
            
        Returns:
            List of (datetime, spread_bps) tuples
        """
        if series_code != "CDS_ARG_5Y_USD":
            raise ProviderError(f"WGBCDSProvider: only supports 'CDS_ARG_5Y_USD' (got {series_code})")
        
        try:
            return fetch_history_argentina_cds_5y_wgb(start=start, end=end)
        except WGBCDSProviderError as e:
            raise ProviderError(f"WGBCDSProvider: {e}")
