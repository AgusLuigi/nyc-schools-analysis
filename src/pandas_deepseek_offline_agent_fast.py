import io, re, sys, os, time, logging, inspect, traceback, threading
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd     # pyre-ignore[16]
import numpy as np      # pyre-ignore[16]

try:
    import ollama as ollama_client  # pyre-ignore[16]
    HAS_OLLAMA = True
except ImportError:
    HAS_OLLAMA = False

DF_PREFIXES = ("df", "df_", "cl", "cl_", "v", "v_")
OUTPUT_TYPES = ("TEXT", "NUMERIC", "TABLE", "PLOT", "CODE")
OLLAMA_MODEL = "deepseek-coder-v2"

def detect_output_type_fast(question: str) -> str:
    q = question.lower()
    if any(tok in q for tok in [" per ", " by ", " je ", " pro ", " nach "]):
        return "TABLE"
    if any(tok in q for tok in ["how many", "how much", "anzahl", "wie viele", "total ", "sum "]):
        return "NUMERIC"
    if any(k in q for k in ["plot", "chart", "diagram", "graphic"]):
        return "PLOT"
    if any(k in q for k in ["code"]):
        return "CODE"
    if any(k in q for k in ["table", "tabelle"]):
        return "TABLE"
    return "TABLE"

class PandasDeepSeekAgentFast:
    def __init__(
        self,
        namespace: Dict[str, Any],
        logging_level: str = "WARNING",
        model: str = OLLAMA_MODEL,
    ):
        self.namespace = namespace
        self.model = model

        self.logger = logging.getLogger("PandasDeepSeekAgentFast")
        self.logger.setLevel(getattr(logging, logging_level.upper(), logging.WARNING))
        if not self.logger.handlers:
            h = logging.StreamHandler(sys.stdout)
            h.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))
            self.logger.addHandler(h)

        if not HAS_OLLAMA:
            self.logger.warning("ollama not installed. pip install ollama")
        else:
            try:
                models = ollama_client.list()
                names = [m.model for m in getattr(models, "models", [])]
                if not any(self.model in n for n in names):
                    self.logger.warning(
                        f"Model '{self.model}' not found. Pull with: ollama pull {self.model}"
                    )
            except Exception as e:
                self.logger.warning(f"Ollama not reachable: {e}")

    def identify_sources(self) -> List[Tuple[str, Dict[str, Any]]]:
        out: List[Tuple[str, Dict[str, Any]]] = []
        for name, obj in self.namespace.items():
            if name.startswith("_"):
                continue
            if not any(name.startswith(p) for p in DF_PREFIXES):
                continue
            if not isinstance(obj, pd.DataFrame):
                continue
            info = {
                "shape": obj.shape,
                "columns": list(obj.columns),
                "dtypes": {c: str(d) for c, d in obj.dtypes.items()},
                "head": obj.head(3).to_string(),
            }
            out.append((name, info))
        return out

    def _resolve_dataframe_name(
        self, question: str, sources: List[Tuple[str, Dict[str, Any]]]
    ) -> Optional[str]:
        import difflib
        q = question.lower()
        names = [n for n, _ in sources]
        for n in names:
            if n.lower() in q:
                return n
        tokens = re.findall(r"[a-zA-Z_][a-zA-Z0-9_]*", q)
        if not tokens:
            return None
        cand = tokens[-1]
        best = difflib.get_close_matches(cand, names, n=1, cutoff=0.6)
        return best[0] if best else None

    def _query_llm(self, prompt: str) -> str:
        if not HAS_OLLAMA:
            raise RuntimeError("ollama package missing.")
        resp = ollama_client.chat(
            model=self.model,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are a Python data analysis expert. "
                        "ALWAYS generate executable Python/Pandas code. "
                        "Return code in ```python ... ``` blocks."
                    ),
                },
                {"role": "user", "content": prompt},
            ],
            options={"num_ctx": 4096, "temperature": 0.0},
        )
        return resp["message"]["content"]

    @staticmethod
    def _extract_code(llm_response: str) -> str:
        blocks = re.findall(r"```python\s*\n(.*?)```", llm_response, re.DOTALL)
        return "\n".join(blocks) if blocks else llm_response.strip()

    def _execute_code(self, code: str) -> Tuple[bool, str]:
        old = sys.stdout
        buf = io.StringIO()
        sys.stdout = buf
        ok = True
        try:
            exec(code, self.namespace)
        except Exception:
            ok = False
            buf.write(traceback.format_exc())
        finally:
            sys.stdout = old
        return ok, buf.getvalue()

    def _format_numeric_output(self, code: str, raw: str) -> str:
        nums = re.findall(r"[-+]?\d*\.?\d+", raw)
        if not nums:
            return raw
        val = float(nums[-1])
        logic_lines = [
            l.strip()
            for l in code.splitlines()
            if "=" in l and "print" not in l and "fig" not in l
        ]
        name = "result"
        if logic_lines:
            name = logic_lines[-1].split("=", 1)[0].strip()
        if val.is_integer():
            return f"{name} = {int(val)}"
        return f"{name} = {val:.4f}"

    def _convert_output(self, code: str, raw: str, otype: str) -> Any:
        from io import StringIO

        if otype == "CODE":
            return code
        if otype == "NUMERIC":
            nums = re.findall(r"[-+]?\d*\.?\d+", raw)
            if not nums:
                return raw
            if len(nums) == 1:
                return self._format_numeric_output(code, raw)
            return raw
        if otype == "TABLE":
            try:
                df = pd.read_fwf(StringIO(raw))
                if df.shape[0] or df.shape[1]:
                    # simple cleanup
                    drop_cols = [c for c in df.columns if str(c).startswith("Unnamed")]
                    if drop_cols:
                        df = df.drop(columns=drop_cols)
                    num_cols = df.select_dtypes(include=[np.number]).columns
                    if len(num_cols) == 1:
                        df = df.sort_values(by=num_cols[0], ascending=False)
                    return df
            except Exception:
                pass
            return raw
        return raw

    def chat(self, question: str, output_type: Optional[str] = None) -> Any:
        if output_type is None:
            output_type = detect_output_type_fast(question)
        output_type = output_type.upper()
        if output_type not in OUTPUT_TYPES:
            output_type = "TEXT"

        sources = self.identify_sources()
        if not sources:
            return "⚠ No DataFrames found in RAM (prefixes: df_, cl_, v_)."

        pref = self._resolve_dataframe_name(question, sources)
        if pref:
            sources = [(n, info) for n, info in sources if n == pref]

        src_desc = "\n".join(
            f"- {n}: shape={i['shape']}, columns={i['columns']}\nHead:\n{i['head']}"
            for n, i in sources
        )

        instructions = {
            "TEXT": (
                "Generate Python code that answers the question and prints a short explanation."
            ),
            "NUMERIC": (
                "Generate Python code that computes EXACTLY ONE numeric value and prints it as "
                "'name = value'. No other prints."
            ),
            "TABLE": (
                "Generate Python code that computes a grouped Pandas DataFrame, sorts it by its "
                "main numeric column descending, and prints only df.to_string(index=False)."
            ),
            "PLOT": (
                "Generate Python code that builds exactly one Plotly Express chart from the "
                "existing DataFrames and calls fig.show()."
            ),
            "CODE": (
                "Return only minimal Python/Pandas code. No explanations, no print, no fig.show()."
            ),
        }[output_type]

        prompt = f"""
Available DataFrames in RAM:
{src_desc}

Question: {question}

Instructions:
- Use the existing DataFrames; do NOT recreate data.
- {instructions}
- Return the code in a ```python``` block.
"""

        last_error = ""
        for _ in range(3):
            llm_resp = self._query_llm(prompt)
            code = self._extract_code(llm_resp)
            ok, raw = self._execute_code(code)
            if ok:
                return self._convert_output(code, raw, output_type)
            last_error = raw
            prompt += f"\n\nERROR during execution:\n{raw[500]}\nFix the code and try again."
        return last_error or "Error: No response generated."

def query_fast(question: str, extra: Optional[str] = None):
    caller_globals = inspect.stack()[1].frame.f_globals
    agent = PandasDeepSeekAgentFast(namespace=caller_globals, logging_level="WARNING")
    if extra and extra.upper() in OUTPUT_TYPES:
        return agent.chat(question, output_type=extra.upper())
    elif extra:
        return agent.chat(f"{question} (Reference: {extra})")
    return agent.chat(question)
