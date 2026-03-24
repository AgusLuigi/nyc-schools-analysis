# src/ini.py
import sys
import os
import glob
import importlib

# 1. UNIVERSAL PATH DETECTION (Surgically precise)
src_path = os.path.dirname(os.path.abspath(__file__))
root_path = os.path.abspath(os.path.join(src_path, '..'))

# Unlock path to src for Python
if src_path not in sys.path:
    sys.path.append(src_path)

# 2. AUTOMATIC MODULE IMPORT
imported_modules = []
for f in glob.glob(os.path.join(src_path, "*.py")):
    module_name = os.path.basename(f)[:-3]
    if module_name.startswith('__') or module_name == 'ini':
        continue
    
    try:
        mod = importlib.import_module(module_name)
        # Transfers all functions/classes into the global scope of the notebook
        globals().update({k: v for k, v in vars(mod).items() if not k.startswith('_')})
        imported_modules.append(module_name)
    except Exception as e:
        print(f"⚠️ Error loading {module_name}: {e}")

# 3. UNIVERSAL HELPER: LOAD DATA
def load_data(relative_path_from_root):
    """
    Loads CSV files based on the main folder.
    Example: load_data("daily_tasks/day_2/day_2_datasets/data.csv")
    """
    full_path = os.path.join(root_path, relative_path_from_root)
    if os.path.exists(full_path):
        return pd.read_csv(full_path)
    else:
        print(f"❌ File not found at: {full_path}")
        return None

print(f"✅ Universal workspace ready!")
print(f"📍 Main folder: {root_path}")
print(f"📦 Modules loaded: {', '.join(imported_modules)}")