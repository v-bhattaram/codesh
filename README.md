# 🐍 Understanding **Pylint Output for Python**

Pylint is a static analysis tool that checks Python code for:
- **Errors** (syntax or logic)
- **Code style violations** (PEP8)
- **Best practices** (logging, exception handling, etc.)

Each message looks like this:

```
file.py:LINE:COL: MESSAGE_CODE: DESCRIPTION (SYMBOL)
```

Example:
```
main.py:12:4: W0703: Catching too general exception Exception (broad-except)
```

---

## 🧩 Message Structure

| Part | Description | Example |
|------|--------------|----------|
| File | The file analyzed | `main.py` |
| Line:Col | Line and column of the issue | `12:4` |
| Message Code | Unique identifier | `W0703` |
| Description | Explanation of the problem | `Catching too general exception Exception` |
| Symbol | Short keyword for rule | `(broad-except)` |

---

## 🔠 Message Categories

| Prefix | Category | Meaning |
|:------:|:----------|:---------|
| `C` | Convention | Coding style / docstrings |
| `R` | Refactor | Code simplification suggestions |
| `W` | Warning | Potential logic/runtime issues |
| `E` | Error | Definite code issue |
| `F` | Fatal | Pylint couldn’t process the file |

---

## ⚡ Common Python Scenarios

### 🧩 1. Missing Docstrings or Naming Violations

```bash
main.py:1:0: C0114: Missing module docstring (missing-module-docstring)
main.py:5:0: C0103: Variable name "X" doesn't conform to snake_case naming style (invalid-name)
```

✅ **Fix:**
```python
"""Module for user authentication."""

def authenticate_user():
    ...
```

---

### 🧩 2. Unused Imports or Variables

```bash
main.py:2:0: W0611: Unused import sys (unused-import)
main.py:8:4: W0612: Unused variable 'temp' (unused-variable)
```

✅ **Fix:**
Remove unused imports/variables or use `_` for temporary values.

```python
import os  # only what you use
_, value = data.split(":")
```

---

### 🧩 3. Attribute or Member Errors

```bash
main.py:15:4: E1101: Module 'os' has no 'read' member (no-member)
```

✅ **Fix:**
Use correct module functions.

```python
# Correct way
with open('file.txt') as f:
    f.read()
```

---

### 🧩 4. Undefined Variables

```bash
main.py:22:8: E0602: Undefined variable 'result' (undefined-variable)
```

✅ **Fix:**
Ensure all variables are defined before use.

```python
result = compute()
print(result)
```

---

## ⚠️ Exception Handling Scenarios

### 🚫 5. Catching Too General Exceptions

```bash
main.py:30:4: W0703: Catching too general exception Exception (broad-except)
```

**Meaning:**  
Catching `Exception` (or worse, a bare `except:`) hides real errors.

✅ **Fix:**
Catch specific exceptions:
```python
try:
    result = risky_function()
except (ValueError, KeyError) as e:
    print(f"Handled error: {e}")
```

If you **must** use a broad exception (e.g., in a safe shutdown), add a **comment**:
```python
try:
    clean_up()
except Exception:  # pylint: disable=broad-except
    pass
```

---

### 🚫 6. Redundant or Unused Exception Variables

```bash
main.py:12:8: W0612: Unused variable 'err' (unused-variable)
```

✅ **Fix:**
Use the exception variable or omit it:
```python
try:
    do_work()
except ValueError:
    log_error()
```

or

```python
except ValueError as err:
    log.error("Failed to process: %s", err)
```

---

### 🚫 7. Bare Except Clauses

```bash
main.py:45:4: W0702: No exception type(s) specified (bare-except)
```

✅ **Fix:**
Specify at least one exception type:
```python
try:
    risky_call()
except Exception:
    handle_failure()
```

---

## 🪵 Logging Scenarios

### ⚙️ 8. Not Using Lazy Logging

```bash
main.py:18:4: W1201: Specify string format arguments as logging function parameters (logging-not-lazy)
```

**Meaning:**  
You used string interpolation before logging (inefficient).

❌ **Bad:**
```python
log.info("User %s logged in" % username)
```

✅ **Good:**
```python
log.info("User %s logged in", username)
```

---

### ⚙️ 9. Using `print()` Instead of Logging

```bash
main.py:20:4: W1510: Using print() instead of proper logging (print-statement)
```

✅ **Fix:**
Use `logging` module:
```python
import logging
log = logging.getLogger(__name__)

log.debug("Debug info")
log.info("Something happened")
log.error("An error occurred")
```

---

### ⚙️ 10. Logging Without Exception Info

```bash
main.py:35:4: W1203: Use logging.exception() when logging exceptions (logging-too-many-args)
```

✅ **Fix:**
Use `exc_info=True` to include traceback:
```python
try:
    risky()
except Exception:
    log.error("Failed during operation", exc_info=True)
```

---

## 🧩 Summary Table

| Category | Example Code | Meaning | Fix |
|-----------|---------------|---------|-----|
| `W0703` | `broad-except` | Catching all exceptions | Catch specific ones |
| `W1201` | `logging-not-lazy` | Improper string formatting in logs | Use lazy logging (`log.info("...", var)`) |
| `W0611` | `unused-import` | Import never used | Remove it |
| `E0602` | `undefined-variable` | Variable not declared | Declare before use |
| `C0114` | `missing-module-docstring` | No docstring | Add one |
| `W0702` | `bare-except` | Missing exception type | Specify exception type |

---

## 🧭 Recommended Setup

Create a `.pylintrc` in your project root:
```bash
pylint --generate-rcfile > .pylintrc
```

Adjust to your needs:
```ini
[MESSAGES CONTROL]
disable=C0114,C0115,C0116

[FORMAT]
max-line-length=100

[LOGGING]
logging-modules=logging
```

---

## 💬 Run and Review

```bash
pylint main.py
```

Example summary:
```
-----------------------------------
Your code has been rated at 8.42/10
-----------------------------------
```
