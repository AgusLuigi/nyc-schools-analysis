#PandasDeepSeekAgent — Local LLM agent for DataFrame analysis in Jupyter Notebooks.

#Prerequisites:
#    1. Install Ollama: https://ollama.ai
#    2. Pull model:  ollama pull deepseek-coder-v2
#    3. Start Ollama server: ollama serve   (runs on http://localhost:11434)
#    4. Python packages:
#         pip install pandasai ollama nbformat pandas numpy plotly

#Connection:
#    - Ollama REST API: http://localhost:11434/v1  (OpenAI-compatible)
#    - Model ID: "deepseek-coder-v2"
#    - No API keys needed — everything local.

#Usage in Notebook:
#    from pandas_deepseek_agent import PandasDeepSeekAgent
#    agent = PandasDeepSeekAgent(globals(), logging_level="INFO")
#    agent.chat("Describe df_sales", "TEXT")
#"""

import logging
import re
import sys
import os
import io
import time
import gc
import multiprocessing
import traceback
import inspect
import threading
from typing import List, Tuple, Dict, Any, Optional

import pandas as pd                             # pyre-ignore[16]
import numpy as np                              # pyre-ignore[16]
from langdetect import detect, DetectorFactory
DetectorFactory.seed = 0

# Optional imports (graceful degradation)
try:
    import psutil   # pyre-ignore[16]
    HAS_PSUTIL = True
except ImportError:
    psutil = None
    HAS_PSUTIL = False

try:
    import plotly.express as px  # pyre-ignore[16]
    import plotly.io as pio      # pyre-ignore[16]
    HAS_PLOTLY = True
except ImportError:
    HAS_PLOTLY = False

try:
    import nbformat  # pyre-ignore[16]
    HAS_NBFORMAT = True
except ImportError:
    HAS_NBFORMAT = False

try:
    import ollama as ollama_client  # pyre-ignore[16]
    HAS_OLLAMA = True
except ImportError:
    HAS_OLLAMA = False

# Globale Steuerung für CPU-Reserve (lebt im RAM bis Kernel-Restart)
_LLM_RESERVED_CORES = 2          # Start: 2 Kerne fürs System
_LLM_CONFIG_LOCKED = False       # Wenn einmal auf 4 erhöht, bleibt so
# Constants
OLLAMA_BASE_URL = "http://localhost:11434"
OLLAMA_MODEL = "deepseek-coder-v2"
DF_PREFIXES = ("df","df_","cl", "cl_","v", "v_")
EXCLUDE_TYPES = ("SmartDataframe", "SmartDatalake", "Agent", "LLM", "LocalLLM")
MAX_REPAIR_ATTEMPTS = 3
OUTPUT_TYPES = ("TEXT", "NUMERIC", "TABLE", "PLOT", "CODE")

# Keyword detection
# ---------------------------------------------------------
# INTENT KEYWORD MAP (User-directed output type hints)
# ---------------------------------------------------------
KEYWORD_MAP = {
    "plot":      "PLOT",
    "chart":     "PLOT",
    "graphic":   "PLOT",
    "diagram":   "PLOT",

    "numeric":   "NUMERIC",
    "number":    "NUMERIC",
    "value":     "NUMERIC",
    "count":    "NUMERIC",
    "sum":     "NUMERIC",

    "table":     "TABLE",
    "tabelle":   "TABLE",

    "code":      "CODE",

    "text":      "TEXT",
    "describe":  "TEXT",
    "explain":   "TEXT",
    "issue":   "TEXT",
    "why":     "TEXT",
}
def _llm_adapt_reserved_cores_from_usage(cpu_usage: float, threshold: float = 85.0):
    """
    Wenn CPU-Dauerlast zu hoch ist:
    - erhöhe Reservierung von 2 auf 4 Kerne
    - fixiere das bis zum Kernel-Restart.
    """
    global _LLM_RESERVED_CORES, _LLM_CONFIG_LOCKED

    if _LLM_CONFIG_LOCKED:
        return

    if cpu_usage >= threshold and _LLM_RESERVED_CORES < 4:
        _LLM_RESERVED_CORES = 4
        _LLM_CONFIG_LOCKED = True

def _llm_global_config() -> Dict[str, Any]:
    """
    Liefert CPU-schonende Default-Config für den LLM:
    - reserviert 2 oder 4 Kerne für System/Browser
    - restliche Kerne stehen dem LLM zur Verfügung.
    """
    total_cores = multiprocessing.cpu_count() or 1
    reserved = _LLM_RESERVED_CORES
    threads = max(1, total_cores - reserved)

    return {
        "num_thread": threads,
        "num_ctx": 4096,
        "temperature": 0.0,
    }

def _llm_soft_throttle(max_cpu: float = 90.0, max_wait: float = 1.0):
    """
    Kurzes Warten, wenn CPU schon sehr hoch ist, bevor der LLM-Call startet.
    """
    if not HAS_PSUTIL:          # ← war: if psutil is None  (greift nie)
        return
    waited = 0.0
    while waited < max_wait:
        usage = psutil.cpu_percent(interval=0.1)
        if usage < max_cpu:
            return
        waited += 0.1
    time.sleep(0.2)

def start_spinner(stop_event, is_de: bool = True):
    """
    VISUELLES FEEDBACK [UX-STANDARD]
    Zeigt einen Spinner während der LLM-Verarbeitung und prüft RAM/CPU.
    Nutzt die Messung, um die CPU-Reserve (2 -> 4 Kerne) selbstständig hochzustellen.
    """
    def perform_system_check():
        """
        SYSTEM-CHECK: Validiert RAM & CPU-Last.
        Zeigt Warnungen und passt ggf. Reservierung an.
        """
        if not HAS_PSUTIL:          # ← NEU: psutil nicht installiert → kein Check
            return ""

        ram_avail = psutil.virtual_memory().available / (1024**3)
        cpu_usage = psutil.cpu_percent(interval=None)

        # CPU-Reservierung anpassen, wenn zu hoch
        _llm_adapt_reserved_cores_from_usage(cpu_usage)

        status_msg = ""
        if ram_avail < 1.5:
            gc.collect()
            status_msg = f"⚠️ RAM kritisch ({ram_avail:.2f}GB). GC ausgeführt."
        if cpu_usage > 90:
            cpu_alert = f" | 🔥 CPU Last hoch ({cpu_usage:.0f}%)."
            status_msg = status_msg + cpu_alert if status_msg else cpu_alert
        return status_msg

    chars = ['⠋','⠙','⠹','⠸','⠼','⠴','⠦','⠧','⠇','⠏']
    msg = "Analysiere (Lokale GPU/CPU)..." if is_de else "Analyzing (Local GPU/CPU)..."
    idx = 0
    while not stop_event.is_set():
        alert_msg = perform_system_check()
        sys.stdout.write(f'\r{msg} {chars[idx % len(chars)]} {alert_msg}')
        sys.stdout.flush()
        idx += 1
        time.sleep(1.0)
    sys.stdout.write('\r' + ' ' * 100 + '\r')
    sys.stdout.flush()

def detect_output_type(question: str) -> str:
    """
    Intent-Erkennung:
    - User-Vorgabe (extra) hat Vorrang und wird in query() gehandhabt.
    - Reihenfolge: Gruppen-Trigger → Einzel-Trigger → Keyword-Map → Default
    """
    q = question.lower()

    # 1. Gruppenfragen (per/by/je/pro/nach) → TABLE
    group_triggers = [" per ", " by ", " je ", " pro ", " nach "]
    if any(tok in q for tok in group_triggers):
        return "TABLE"

    # 2. Einfache Kennzahl-Fragen → NUMERIC
    single_triggers = ["how many", "how much", "anzahl", "wie viele", "total ", "sum "]
    if any(tok in q for tok in single_triggers):
        return "NUMERIC"

    for keyword, otype in KEYWORD_MAP.items():  # Großschreibung wie oben definiert
        if keyword in q:
            return otype

    # 4. Default: TABLE ist für DataFrames robuster
    return "TABLE"

# Main class
class PandasDeepSeekAgent:
    """
    Local LLM agent that uses DeepSeek-Coder-V2 via Ollama
    to analyze DataFrames in RAM.

    Connection:
        Ollama must be running on localhost:11434.
        >>> ollama serve                       # Terminal 1
        >>> ollama pull deepseek-coder-v2      # once

    Example:
        >>> agent = PandasDeepSeekAgent(globals())
        >>> agent.chat("Show the first rows of df_sales", "TABLE")
    """

    def __init__(
        self,
        namespace: Dict[str, Any],
        logging_level: str = "INFO",
        ollama_base_url: str = OLLAMA_BASE_URL,
        model: str = OLLAMA_MODEL,
    ):
        # Logging
        self.logger = logging.getLogger("PandasDeepSeekAgent")
        self.logger.setLevel(
            getattr(logging, 
            logging_level.upper(), 
            logging.INFO))
        self.logger.disabled = True # Disable debugging
        
        if not self.logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            handler.setFormatter(
                logging.Formatter("[%(levelname)s] %(message)s")
            )
            self.logger.addHandler(handler)

        # Namespace (globals() of the notebook)
        self.namespace = namespace

        # Ollama connection
        self.ollama_base_url = ollama_base_url
        self.model = model
        self._verify_connection()

    # Connection test
    def _verify_connection(self):
        """Check whether Ollama is reachable and the model is loaded."""
        if not HAS_OLLAMA:
            self.logger.warning(
                "ollama Python package not installed. "
                "Install with: pip install ollama"
            )
            return

        try:
            models = ollama_client.list()
            model_names = [m.model for m in models.models] if hasattr(models, 'models') else []
            if not any(self.model in n for n in model_names):
                self.logger.warning(
                    f"Model '{self.model}' not found. "
                    f"Available models: {model_names}. "
                    f"Pull with: ollama pull {self.model}"
                )
            else:
                self.logger.info(
                    f"✓ Ollama connected — model '{self.model}' ready."
                )
        except Exception as e:
            self.logger.error(
                f"Ollama not reachable at {self.ollama_base_url}: {e}\n"
                f"Start Ollama with: ollama serve"
            )

    def _detect_language(self, question: str) -> str:
        """
        Language detector using langdetect.
        Returns ISO 639-1 codes like 'de', 'en', 'fr'.
        Falls detection fehlschlägt, default 'en'.
        """
        try:
            code = detect(question)
            return code  # e.g. 'de', 'en', 'fr', 'es', ...
        except Exception:
            return "en"

    # RAM scan
    def identify_sources(self) -> List[Tuple[str, Dict[str, Any]]]:
        """
        Scan globals() for DataFrames with prefixes df_, cl_, v_.
        Exclude LLM/PandasAI objects.

        Semantik:
        - df_*  : Quell-DataFrames (Originaldaten)
        - cl_*  : Clean-Overlays (gleicher Name, korrigierte Zellen, sonst NaN)
        - v_*   : Visualisierungs-DataFrames (abgeleitete Views)
        """
        sources: List[Tuple[str, Dict[str, Any]]] = []

        for name, obj in self.namespace.items():
            if name.startswith("_"):
                continue
            if type(obj).__name__ in EXCLUDE_TYPES:
                continue
            if not any(name.startswith(p) for p in DF_PREFIXES):
                continue
            if not isinstance(obj, pd.DataFrame):
                continue

            # Rolle und base_name in einem Schritt ableiten
            if name.startswith(("df_", "df")):
                role = "SOURCE"
                base_name = name.replace("df_", "", 1).replace("df", "", 1)
            elif name.startswith(("cl_", "cl")):
                role = "CLEAN"
                base_name = name.replace("cl_", "", 1).replace("cl", "", 1)
            elif name.startswith(("v_", "v")):
                role = "VISUAL"
                base_name = name.replace("v_", "", 1).replace("v", "", 1)
            else:
                role = "GENERIC"
                base_name = name

            info = {
                "shape":     obj.shape,
                "columns":   list(obj.columns),
                "dtypes":    {c: str(d) for c, d in obj.dtypes.items()},
                "head":      obj.head(3).to_string(),
                "role":      role,
                "base_name": base_name,
            }

            sources.append((name, info))
            self.logger.debug(f"DataFrame found: {name} {obj.shape} role={role}")

        self.logger.info(f"RAM scan: {len(sources)} DataFrame(s) found.")
        return sources

    def _resolve_dataframe_name(
        self, question: str, sources: List[Tuple[str, Dict[str, Any]]]
    ) -> Optional[str]:
        """
        Versucht, den vom Benutzer gemeinten DataFrame-Namen zu finden:
        1. Exakt erwähnter Name.
        2. Fuzzy-Match (ähnlichster Name).
        """
        import difflib

        q_lower = question.lower()
        df_names = [name for name, _ in sources]

        # 1) Exakte Nennung im Text
        for name in df_names:
            if name.lower() in q_lower:
                return name

        # 2) Fuzzy: letztes „wortähnliches“ Token nehmen
        tokens = re.findall(r"[a-zA-Z_][a-zA-Z0-9_]*", q_lower)
        if not tokens:
            return None
        candidate = tokens[-1]  # z.B. df_zukini

        best = difflib.get_close_matches(candidate, df_names, n=1, cutoff=0.6)
        return best[0] if best else None

    def _df_to_cl_visual_views(
        self, sources: List[Tuple[str, Dict[str, Any]]]
    ) -> List[Tuple[str, Dict[str, Any]]]:
        """
        Build virtual visual views (v_*) by merging df_* (SOURCE) and cl_* (CLEAN)
        without modifying the original df_* objects.

        - Wenn zu einem base_name sowohl df_* als auch cl_* existieren:
          v_<base_name> = df_<base_name> mit Overlays aus cl_<base_name> (wo cl nicht NaN).
        - Wenn nur df_* existiert, kann optional v_<base_name> = df_<base_name> gespiegelt werden.
        """
        # Index nach base_name und role aufbauen
        by_base: Dict[str, Dict[str, Dict[str, Any]]] = {}
        for name, info in sources:
            base = info.get("base_name", name)
            role = info.get("role", "GENERIC")
            by_base.setdefault(base, {})[role] = {"name": name, "info": info}

        new_visuals: List[Tuple[str, Dict[str, Any]]] = []

        for base, roles in by_base.items():
            src = roles.get("SOURCE")
            cln = roles.get("CLEAN")

            # Nur wenn es eine Quelle gibt, kann eine Visualisierung sinnvoll sein
            if not src:
                continue

            df_name = src["name"]
            df_obj = self.namespace.get(df_name)
            if not isinstance(df_obj, pd.DataFrame):
                continue

            if cln:
                cl_name = cln["name"]
                cl_obj = self.namespace.get(cl_name)
                if isinstance(cl_obj, pd.DataFrame):
                    # Merge: cl über df legen, wo cl nicht NaN ist
                    try:
                        v_obj = df_obj.copy()
                        v_obj.update(cl_obj)
                    except Exception:
                        # Fallback: wenn update scheitert, nimm df unverändert
                        v_obj = df_obj.copy()
                else:
                    v_obj = df_obj.copy()
            else:
                # Kein CLEAN-Overlay: Visualisierung = Kopie der Quelle
                v_obj = df_obj.copy()

            v_name = f"v_{base}"
            # Im Namespace registrieren (ohne df_* zu verändern)
            self.namespace[v_name] = v_obj

            v_info = {
                "shape": v_obj.shape,
                "columns": list(v_obj.columns),
                "dtypes": {c: str(d) for c, d in v_obj.dtypes.items()},
                "head": v_obj.head(3).to_string(),
                "role": "VISUAL",
                "base_name": base,
            }
            new_visuals.append((v_name, v_info))
            self.logger.debug(f"Visual view created: {v_name} from base '{base}'")

        # Bestehende Quellen bleiben, Visual-Views werden ergänzt
        return sources + new_visuals

    def _numeric_block_to_table(self, raw_output: str) -> pd.DataFrame:
        """
        Mehrere numerische Werte als DataFrame mit 1 Spalte 'value',
        jede Zahl in einer eigenen Zeile.
        """
        lines = [l for l in raw_output.splitlines() if l.strip()]
        if len(lines) <= 1:
            # Für Single-Wert NICHT als Tabelle verwenden
            return pd.DataFrame({"value": [raw_output.strip()]})

        # Alle Zahlen aus allen Zeilen extrahieren
        values = []
        for line in lines:
            nums = re.findall(r"[-+]?\d*\.?\d+", line)
            for n in nums:
                values.append(float(n))
        if not values:
            return pd.DataFrame({"value": [raw_output.strip()]})
        return pd.DataFrame({"value": values})

    # Read notebook outputs
    def get_recent_notebook_outputs(self, max_cells: int = 5) -> List[str]:
        """
        Load the last N cell outputs from .ipynb files in the current
        directory (sorted by execution_count).
        """
        if not HAS_NBFORMAT:
            self.logger.debug("nbformat not installed — skipping.")
            return []

        outputs = []
        for fname in sorted(os.listdir(".")):
            if not fname.endswith(".ipynb"):
                continue
            try:
                nb = nbformat.read(fname, as_version=4)
                cells_with_exec = [
                    c for c in nb.cells
                    if c.cell_type == "code" and c.get("execution_count")
                ]
                cells_with_exec.sort(
                    key=lambda c: c["execution_count"], reverse=True
                )
                for cell in cells_with_exec[:max_cells:]:
                    for out in cell.get("outputs", []):
                        if out.output_type == "stream":
                            outputs.append(out.text[:500])
                        elif "text/plain" in out.get("data", {}):
                            outputs.append(out["data"]["text/plain"][:500])
            except Exception:
                pass

        self.logger.debug(f"Notebook outputs loaded: {len(outputs)} entries.")
        return outputs[-max_cells:]

    # LLM call
    def _query_llm(self, prompt: str) -> str:
        """Send prompt to DeepSeek via Ollama and return the response."""
        if not HAS_OLLAMA:
            raise RuntimeError(
                "ollama package missing. Install with: pip install ollama"
            )

        # ggf. leicht bremsen, wenn System schon am Limit
        _llm_soft_throttle()

        # aktuelle Config holen (nutzt 2 oder 4 reservierte Kerne)
        cfg = _llm_global_config()

        self.logger.debug(
            f"LLM prompt ({len(prompt)} characters) being sent... | "
            f"reserved_cores={_LLM_RESERVED_CORES}, num_thread={cfg['num_thread']}"
        )

        response = ollama_client.chat(
            model=self.model,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are a Python data analysis expert. "
                        "ALWAYS generate executable Python/Pandas code. "
                        "Reply in English if the question is in English. "
                        "Return code in ```python ... ``` blocks."
                    ),
                },
                {"role": "user", "content": prompt},
            ],
            options={
                "num_ctx": cfg["num_ctx"],
                "num_thread": cfg["num_thread"],
                "temperature": cfg["temperature"],
            },
        )

        answer = response["message"]["content"]
        self.logger.debug(f"LLM response: {answer[:200]}...")
        return answer

    # Extract & execute code
    @staticmethod
    def _extract_code(llm_response: str) -> str:
        """Extract Python code from Markdown code blocks."""
        blocks = re.findall(r"```python\s*\n(.*?)```", llm_response, re.DOTALL)
        if blocks:
            return "\n".join(blocks)
        # Fallback: treat entire response as code
        return llm_response.strip()

    def _execute_code(self, code: str) -> Tuple[bool, str]:
        """
        Execute code in the notebook namespace.
        Returns (success: bool, output: str).
        """
        old_stdout = sys.stdout
        sys.stdout = buffer = io.StringIO()
        success = True
        try:
            exec(code, self.namespace)
        except Exception:
            success = False
            buffer.write(traceback.format_exc())
        finally:
            sys.stdout = old_stdout

        output = buffer.getvalue()
        return success, output

    def _format_numeric_output(self, code: str, raw_output: str) -> str:
        """
        Format ONE numeric value as: logical_name = value
        - logical_name wird aus der letzten sinnvollen Zuweisung im Code extrahiert.
        - value wird auf 4 Nachkommastellen formatiert (bei Float).
        """
        # Zahlen aus dem Print-Output holen
        numbers = re.findall(r"[-+]?\d*\.?\d+", raw_output)
        if not numbers:
            return raw_output
        val = float(numbers[-1])
        # Logischen Namen aus dem generierten Code extrahieren
        logic_lines = [
            line.strip()
            for line in code.split("\n")
            if "=" in line
            and not any(x in line for x in ["print", "fig", "plt.", "px.", "result"])
        ]
        if logic_lines:
            last_logic = logic_lines[-1]
            logical_name = last_logic.split("=", 1)[0].strip()
            logical_name = (
                logical_name.replace("df['", "").replace("']", "")
                            .replace('df["', "").replace('"]', "")
            )
        else:
            logical_name = "result"
        if val.is_integer():
            return f"{logical_name} = {int(val)}"
        return f"{logical_name} = {val:.4f}"

    # Output conversion
    def _convert_output(self, code: str, raw_output: str, output_type: str) -> Any:
        """Convert the result to the desired output type."""
        from io import StringIO

        # 1. CODE bleibt wie er ist
        if output_type == "CODE":
            return code

        # 2. NUMERIC: Single-Metric oder Multi-Wert-Tabelle
        if output_type == "NUMERIC":
            numbers = re.findall(r"[-+]?\d*\.?\d+", raw_output)
            if not numbers:
                return raw_output
            if len(numbers) == 1:
                return self._format_numeric_output(code, raw_output)

            lines = [l for l in raw_output.splitlines() if l.strip()]
            if all(re.fullmatch(r"[-+]?\d*\.?\d+", l.strip()) for l in lines):
                return self._numeric_block_to_table(raw_output)
            return raw_output

        # 3. TABLE: jetzt erst FWF-Parse versuchen
        if output_type == "TABLE":
            try:
                df_try = pd.read_fwf(StringIO(raw_output))
                if df_try.shape[1] >= 2 or df_try.shape[0] >= 3:
                    unnamed = [c for c in df_try.columns if str(c).startswith("Unnamed")]
                    for col in unnamed:
                        df_try = df_try.drop(columns=[col])
                    # einfache Heuristik: eine numerische Spalte → absteigend sortieren
                    num_cols = df_try.select_dtypes(include=[np.number]).columns.tolist()
                    if len(num_cols) == 1:
                        df_try = df_try.sort_values(by=num_cols[0], ascending=False)
                    return df_try
            except Exception:
                pass
            return raw_output

        # 4. PLOT
        if output_type == "PLOT":
            if not HAS_PLOTLY:
                return "Plotly not installed. pip install plotly"
            return raw_output  # Plot war schon gezeigt

        # 5. TEXT oder alles andere
        return raw_output

    # Detect repair mismatch
    def _detect_mismatch(self, code: str, output_type: str) -> bool:
        """Check whether the generated code matches the desired output type."""
        code_lower = code.lower()

        if output_type == "TEXT" and ("plotly" in code_lower or ".show()" in code_lower):
            return True  # Plot statt Text

        if output_type == "PLOT" and "print(" in code_lower and ".show()" not in code_lower:
            return True  # Text statt Plot

        if output_type == "NUMERIC" and "head(" in code_lower:
            return True  # Tabelle statt Zahl

        # ← NEU: TABLE-Check
        if output_type == "TABLE":
            if ".show()" in code_lower:
                return True  # Plot statt Tabelle
            table_indicators = ["to_string", "groupby", "pivot", "head(", "dataframe"]
            if not any(ind in code_lower for ind in table_indicators):
                return True  # Keine Tabellenstruktur erkennbar → wahrscheinlich Einzelwert

        # Strikter CODE-Modus: keine direkten Ausgaben/Plots
        if output_type == "CODE":
            forbidden = ["print(", ".show(", "fig.show(", "plt.show("]
            if any(tok in code_lower for tok in forbidden):
                return True

        return False

    # Main method: chat()
    def chat(
        self,
        question: str,
        output_type: Optional[str] = None,
    ) -> Any:
        """
        Ask the agent a question.

        Args:
            question:    Question in natural language.
            output_type: TEXT | NUMERIC | TABLE | PLOT | CODE
                         (auto-detected if None).

        Returns:
            Answer in the desired format.

        Example:
            >>> agent.chat("How many rows does df_sales have?", "NUMERIC")
            1234.0
        """
        # 1. Determine output type
        if output_type is None:
            output_type = detect_output_type(question)
        output_type = output_type.upper()
        if output_type not in OUTPUT_TYPES:
            output_type = "TEXT"
        self.logger.info(f"Output type: {output_type}")

        # 2. RAM scan
        sources = self.identify_sources()
        sources = self._df_to_cl_visual_views(sources)
        if not sources:
            return "⚠ No DataFrames found in RAM (prefixes: df_, cl_, v_)."

        # 2b. Gewünschten DF-Namen aus der Frage auflösen
        preferred = self._resolve_dataframe_name(question, sources)
        if preferred:
            sources = [(name, info) for name, info in sources if name == preferred]

        # 3. Notebook outputs
        nb_outputs = self.get_recent_notebook_outputs()

        # 4. Build prompt
        source_desc = "\n".join(
            f"- {name}: shape={info['shape']}, columns={info['columns']}\n"
            f"  Head:\n{info['head']}"
            for name, info in sources
        )
        nb_context = (
            "\nRecent notebook outputs:\n" + "\n---\n".join(nb_outputs)
            if nb_outputs else ""
        )

        output_instructions = {
            "TEXT": (
                "Answer the question in plain language using print() statements. "
                "Always explain briefly what was computed and how it relates to the question. "
                "If you also compute numbers, mention them in the explanation. "
                "Format all numeric values with at most 4 decimal places; "
                "integers without decimals, floats with up to 4 decimals, "
                "unless the user explicitly requests a different precision."
            ),

            "NUMERIC": (
                "If the question logically requires a single overall metric "
                "(for example one total, one overall average, one global min or max), "
                "compute exactly ONE numeric value. "
                "Assign it to a clearly named variable and then print it once "
                "in the form 'variable_name = value'. "
                "Format the numeric value so that integers are printed without decimals "
                "and floating point values are printed with 4 decimal places, "
                "unless the user explicitly requests a different precision. "
                "Do NOT print intermediate tables or multiple numeric lines."
            ),

            "TABLE": (
                "If the question asks for values per group (for example per borough, per category, "
                "per school), compute the grouped statistics using groupby() and aggregation. "
                "Always aggregate into a DataFrame with at least one label column and one or more "
                "value columns, then call reset_index() so the label is a normal column "
                "(no Series, no index-based output). "
                "Before printing, round all floating point columns so that they have at most "
                "4 decimal places (for example df = df.round(4)), "
                "unless the user explicitly asks for a different precision. "
                "If the question explicitly asks for a total over the table (for example 'give total avg', "
                "'overall average', 'global max', 'global min', or 'overall count'), "
                "then add ONE extra row whose label makes clear what kind of total it is and whose value "
                "is computed with the SAME statistic as in the column: "
                "for averages use an overall average (e.g. 'AVG_Total'), for minima an overall minimum "
                "(e.g. 'Min_Total'), for maxima an overall maximum (e.g. 'Max_Total'), "
                "for counts a total count (e.g. 'Count_Total'). "
                "After computing the aggregated DataFrame, always sort it in descending order "
                "by the main numeric column (for example 'Average_Student_Count') using "
                "df.sort_values(by='<that_column>', ascending=False, inplace=True) before printing. "
                "Finally print ONLY this DataFrame with print(df.to_string(index=False))."
            ),

            "PLOT": (
                "Create exactly one clear Plotly chart that directly answers the question. "
                "Use an existing DataFrame from RAM, aggregate as needed, and then create a figure "
                "with plotly.express (px). "
                "Keep count/number columns as integers in the DataFrame where possible, but format "
                "their display in the chart so that they appear as whole numbers (no decimals). "
                "Do NOT pass 'hovertemplate' directly to px.bar or other px functions. "
                "Instead, first create the figure, then call "
                "fig.update_layout(yaxis_tickformat='.0f') to format the axis and "
                "fig.update_traces(hovertemplate='%{y:.0f}') to format the hover labels. "
                "For real-valued metrics use at most 4 decimal places. "
                "Finally call fig.show() once. Do NOT print tables or text."
            ),


            "CODE": (
                "Return only the minimal, clean Python/Pandas code needed to answer the question. "
                "Use existing DataFrames from RAM and do not recreate them or add explanations. "
                "When you format numeric output in this code (for example with round(), DataFrame.round(), "
                "or f-strings), ensure that floats have at most 4 decimal places and integers no decimals "
                "by default, unless the user explicitly requests a different precision."
            ),
        }

        # Sprache für TEXT steuern (DE/EN/… → Ziel-Sprache für die Antwort)
        lang = self._detect_language(question)
        language_hint = ""
        if output_type == "TEXT":
            if lang.startswith("de"):
                language_hint = " Answer in German."
            elif lang.startswith("fr"):
                language_hint = " Answer in French."
            else:
                language_hint = " Answer in English."

        text_instruction = output_instructions.get(
            output_type,
            output_instructions["TEXT"]
        ) + language_hint

        prompt = f"""
Available DataFrames in RAM:
{source_desc}
{nb_context}
Question: {question}
Instructions:
- Generate executable Python/Pandas code.
- The DataFrames already exist as variables — do NOT recreate them.
- {text_instruction}
- Return the code in a ```python``` block.
"""

        # 5. LLM query + repair loop
        for attempt in range(1, MAX_REPAIR_ATTEMPTS + 1):
            self.logger.info(f"Attempt {attempt}/{MAX_REPAIR_ATTEMPTS}...")

            llm_response = self._query_llm(prompt)
            code = self._extract_code(llm_response)
            self.logger.debug(f"Generated code:\n{code}")

            # Check for mismatch BEFORE executing
            if self._detect_mismatch(code, output_type):
                self.logger.warning(
                    f"⚠ Mismatch detected (attempt {attempt}). Repairing..."
                )
                prompt += (
                    f"\n\nERROR: Your code does not match the output type '{output_type}'. "
                    f"{'Use print() instead of plot.' if output_type == 'TEXT' else ''}"
                    f"{'Use plotly fig.show().' if output_type == 'PLOT' else ''}"
                    f" Please try again."
                )
                continue

            # Execute code
            success, raw_output = self._execute_code(code)

            # Spinner-/Status-Zeilen aus dem Output entfernen
            cleaned_lines = []
            for line in raw_output.splitlines():
                if "Analysiere (Lokale GPU/CPU)" in line or "Analyzing (Local GPU/CPU)" in line:
                    continue
                if "RAM kritisch" in line or "CPU Last hoch" in line:
                    continue
                cleaned_lines.append(line)
            raw_output = "\n".join(cleaned_lines)

            if not success:
                self.logger.warning(f"⚠ Execution error (attempt {attempt}):\n{raw_output[:300]}")
                prompt += (f"\n\nERROR during execution:\n{raw_output[:500]}\n"f"Fix the code and try again.")
                continue

            # Success
            result = self._convert_output(code, raw_output, output_type)
            self.logger.info("✓ Executed successfully.")

            if output_type == "CODE":
                print(f"```python\n{code}\n```")
            if output_type == "TEXT":
                if isinstance(result, str):
                    return result.strip()
                return result
            return result

        # Fallback after max attempts
        self.logger.error("✗ Max repair attempts reached. Falling back to TEXT.")
        return raw_output if 'raw_output' in dir() else "Error: No response generated."

# Silence all standard loggers (httpx, ollama, etc.)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("ollama").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

# If you also want your own agent logger completely silent:
logging.getLogger("PandasDeepSeekAgent").setLevel(logging.CRITICAL)

def query(question: str, extra: Optional[str] = None):
    """
    Universal shortcut: automatically detects DataFrames in the notebook.
    Usage: query("question") or query("question", "extra/type")
    """
    caller_globals = inspect.stack()[1].frame.f_globals
    agent = PandasDeepSeekAgent(namespace=caller_globals, logging_level="CRITICAL")
    # Spinner-Thread vorbereiten
    stop_event = threading.Event()
    spinner_thread = threading.Thread(target=start_spinner, args=(stop_event, True))
    spinner_thread.daemon = True
    try:
        spinner_thread.start()
        if extra and extra.upper() in ["TEXT", "NUMERIC", "TABLE", "PLOT", "CODE"]:
            result = agent.chat(question, output_type=extra.upper())
        elif extra:
            combined_question = f"{question} (Reference: {extra})"
            result = agent.chat(combined_question)
        else:
            result = agent.chat(question)
    finally:
        stop_event.set()
        spinner_thread.join(timeout=1.0)
    return result

# ausreisser erkennung zusetzlich zur query funktion
def highlight_outliers_iqr(df: pd.DataFrame, factor: float = 1.5):
    """
    Return a Styler that highlights outliers per numeric column
    using IQR-based fences (Q1 - factor*IQR, Q3 + factor*IQR).
    """
    num_cols = df.select_dtypes(include=[np.number]).columns
    def _highlight(s: pd.Series):
        if s.name not in num_cols:
            return [''] * len(s)
        q1 = s.quantile(0.25)
        q3 = s.quantile(0.75)
        iqr = q3 - q1
        lower = q1 - factor * iqr
        upper = q3 + factor * iqr
        return [
            'background-color: #ffdddd' if (x < lower or x > upper) else ''
            for x in s
        ]
    return df.style.apply(_highlight, axis=0)

# Anwendung
# from io import StringIO
# v_query = query("zeige mir die Tabelle df_sales", "TABLE")
# df_tmp = pd.read_fwf(StringIO(v_query))
# display(highlight_outliers_iqr(df_tmp))
#PandasDeepSeekAgent — Local LLM agent for DataFrame analysis in Jupyter Notebooks.